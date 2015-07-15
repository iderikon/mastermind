/*
 * Copyright (c) YANDEX LLC, 2015. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3.0 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library.
 */

#include "Couple.h"
#include "DiscoveryTimer.h"
#include "Group.h"
#include "Guard.h"
#include "Node.h"
#include "Storage.h"
#include "ThreadPool.h"

#include <elliptics/session.hpp>

using namespace ioremap;

namespace {

class UpdateJob : public ThreadPool::Job
{
public:
    UpdateJob(Storage & storage)
        : m_storage(storage)
    {}

    virtual void execute()
    {
        BH_LOG(*m_storage.get_logger(), DNET_LOG_NOTICE, "Updating groups and couples");

        m_storage.update_groups();
        m_storage.update_couples();

        m_storage.arm_timer();
    }

private:
    Storage & m_storage;
};

class UpdateJobToggle
{
public:
    UpdateJobToggle(ThreadPool & thread_pool, UpdateJob *job, int nr_groups)
        :
        m_thread_pool(thread_pool),
        m_update_job(job),
        m_nr_groups(nr_groups)
    {}

    void handle_completion()
    {
        if (! --m_nr_groups)
            m_thread_pool.execute_pending(m_update_job);
    }

private:
    ThreadPool & m_thread_pool;
    UpdateJob *m_update_job;
    std::atomic<int> m_nr_groups;
};

class GroupMetadataHandler
{
public:
    GroupMetadataHandler(elliptics::session & session,
            Group *group, UpdateJobToggle *toggle)
        :
        m_session(session.clone()),
        m_group(group),
        m_toggle(toggle)
    {}

    void result(const elliptics::read_result_entry & entry)
    {
        elliptics::data_pointer file = entry.file();
        m_group->save_metadata((const char *) file.data(), file.size());
    }

    void final(const elliptics::error_info & error)
    {
        if (error) {
            std::ostringstream ostr;
            ostr << "Metadata download failed: " << error.message();
            m_group->set_status_text(ostr.str());
            m_group->set_status(Group::BAD);
        }
        m_toggle->handle_completion();
    }

    elliptics::session *get_session()
    { return &m_session; }

private:
    elliptics::session m_session;
    Group *m_group;
    UpdateJobToggle *m_toggle;
};

} // unnamed namespace

Storage::Storage(ThreadPool & thread_pool, const Config & config,
        std::shared_ptr<elliptics::logger_base> logger)
    :
    m_thread_pool(thread_pool),
    m_config(config),
    m_logger(logger)
{}

Storage::~Storage()
{
    for (auto it = m_groups.begin(); it != m_groups.end(); ++it)
        delete it->second;
    for (auto it = m_couples.begin(); it != m_couples.end(); ++it)
        delete it->second;
}

void Storage::get_nodes(std::vector<Node *> & nodes)
{
    ReadGuard<RWMutex> guard(m_nodes_lock);

    auto it = m_nodes.begin();
    for (; it != m_nodes.end(); ++it)
        nodes.push_back(&it->second);
}

bool Storage::add_node(const char *host, int port, int family)
{
    char node_id[128];
    sprintf(node_id, "%s:%d:%d", host, port, family);

    std::string node_id_str(node_id);

    {
        ReadGuard<RWMutex> guard(m_nodes_lock);

        auto it = m_nodes.find(node_id_str);
        if (it != m_nodes.end()) {
            BH_LOG(*m_logger, DNET_LOG_DEBUG, "Node %s already exists", node_id_str.c_str());
            return false;
        }
    }

    {
        Node node(*this, host, port, family);

        BH_LOG(*m_logger, DNET_LOG_INFO, "New node %s", node_id_str.c_str());

        WriteGuard<RWMutex> guard(m_nodes_lock);

        return m_nodes.insert(std::make_pair(std::string(host), node)).second;
    }
}

void Storage::handle_backend(BackendStat & backend)
{
    Group *group;

    {
        ReadGuard<RWMutex> guard(m_groups_lock);
        auto it = m_groups.find(backend.group);
        if (it != m_groups.end()) {
            group = it->second;
            guard.release();

            group->update_backend(backend);
            return;
        }
    }

    group = new Group(backend, *this);

    {
        WriteGuard<RWMutex> guard(m_groups_lock);
        auto it = m_groups.lower_bound(backend.group);
        if (it != m_groups.end() && it->first == int(backend.group)) {
            Group *existing = it->second;
            guard.release();

            existing->update_backend(backend);
            delete group;
            return;
        }

        m_groups.insert(it, std::make_pair(backend.group, group));
    }
}

void Storage::get_group_ids(std::vector<int> & groups) const
{
    ReadGuard<RWMutex> guard(m_groups_lock);

    groups.reserve(m_groups.size());
    auto it = m_groups.begin();
    for (; it != m_groups.end(); ++it)
        groups.push_back(it->first);
}

void Storage::schedule_update_groups_and_couples(elliptics::session & session)
{
    std::vector<int> group_id(1);
    elliptics::key key("symmetric_groups");

    ReadGuard<RWMutex> guard(m_groups_lock);

    if (m_groups.empty()) {
        arm_timer();
        return;
    }

    UpdateJob *update = new UpdateJob(*this);
    UpdateJobToggle *toggle = new UpdateJobToggle(m_thread_pool, update, m_groups.size());
    m_thread_pool.dispatch_pending(update);

    for (auto it = m_groups.begin(); it != m_groups.end(); ++it) {
        Group *group = it->second;
        group_id[0] = group->get_id();

        GroupMetadataHandler *handler = new GroupMetadataHandler(session, group, toggle);

        elliptics::session *new_session = handler->get_session();
        new_session->set_namespace("metabalancer");
        new_session->set_groups(group_id);

        BH_LOG(*m_logger, DNET_LOG_DEBUG,
                "Scheduling metadata download for group %d", group_id[0]);

        elliptics::async_read_result res = new_session->read_data(key, group_id, 0, 0);
        res.connect(std::bind(&GroupMetadataHandler::result, handler, std::placeholders::_1),
                std::bind(&GroupMetadataHandler::final, handler, std::placeholders::_1));
    }
}

void Storage::update_groups()
{
    std::vector<Group*> groups;

    {
        ReadGuard<RWMutex> guard(m_groups_lock);
        groups.reserve(m_groups.size());
        auto it = m_groups.begin();
        for (; it != m_groups.end(); ++it)
            groups.push_back(it->second);
    }

    for (size_t i = 0; i < groups.size(); ++i)
        groups[i]->process_metadata();
}

void Storage::update_couples()
{
    std::vector<Couple*> couples;

    {
        ReadGuard<RWMutex> guard(m_couples_lock);
        couples.reserve(m_couples.size());
        auto it = m_couples.begin();
        for (; it != m_couples.end(); ++it)
            couples.push_back(it->second);
    }

    for (size_t i = 0; i < couples.size(); ++i)
        couples[i]->update_status();
}

void Storage::create_couple(const std::vector<int> & group_ids, Group *group)
{
    std::vector<Group*> groups;
    groups.reserve(group_ids.size());

    {
        WriteGuard<RWMutex> guard(m_groups_lock);
        for (size_t i = 0; i < group_ids.size(); ++i) {
            int id = group_ids[i];

            if (id == group->get_id()) {
                groups.push_back(group);
            } else {
                auto it = m_groups.lower_bound(id);
                if (it != m_groups.end() && it->first == id) {
                    groups.push_back(it->second);
                } else {
                    Group *new_group = new Group(id, *this);
                    m_groups.insert(it, std::make_pair(id, new_group));
                    groups.push_back(new_group);
                }
            }
        }
    }

    {
        CoupleKey key(group_ids);

        WriteGuard<RWMutex> guard(m_couples_lock);

        auto it = m_couples.lower_bound(key);
        if (it == m_couples.end() || it->first != key) {
            Couple *couple = new Couple(groups);
            couple->bind_groups();
            m_couples.insert(it, std::make_pair(key, couple));
        }
    }
}

void Storage::arm_timer()
{
    if (m_discovery_timer->arm(DiscoveryTimer::Subsequent) < 0) {
        int err = errno;
        BH_LOG(*m_logger, DNET_LOG_ERROR, "Failed to arm timer: %s", strerror(err));
    }
}
