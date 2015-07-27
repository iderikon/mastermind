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
#include "Discovery.h"
#include "DiscoveryTimer.h"
#include "Filter.h"
#include "FS.h"
#include "Group.h"
#include "Guard.h"
#include "Node.h"
#include "Storage.h"
#include "ThreadPool.h"
#include "WorkerApplication.h"

#include <elliptics/session.hpp>

using namespace ioremap;

class Storage::UpdateJobToggle
{
public:
    UpdateJobToggle(ThreadPool & thread_pool, ThreadPool::Job *job, int nr_groups)
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
    ThreadPool::Job *m_update_job;
    std::atomic<int> m_nr_groups;
};

namespace {

template<typename T>
void remove_duplicates(std::vector<T> & v)
{
    std::sort(v.begin(), v.end());
    auto it = std::unique(v.begin(), v.end());
    v.erase(it, v.end());
}

class UpdateJob : public ThreadPool::Job
{
public:
    UpdateJob(Storage & storage)
        : m_storage(storage)
    {}

    virtual void execute()
    {
        BH_LOG(m_storage.get_app().get_logger(), DNET_LOG_NOTICE,
                "Updating filesystems, groups and couples");

        m_storage.update_filesystems();
        m_storage.update_groups();
        m_storage.update_couples();

        m_storage.get_app().get_discovery().end();
        m_storage.arm_timer();
    }

private:
    Storage & m_storage;
};

class GroupMetadataHandler
{
public:
    GroupMetadataHandler(elliptics::session & session,
            Group *group, Storage::UpdateJobToggle *toggle)
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
    Storage::UpdateJobToggle *m_toggle;
};

class SnapshotJob : public ThreadPool::Job
{
public:
    SnapshotJob(Storage & storage, const Filter & filter,
            std::shared_ptr<on_get_snapshot> handler)
        :
        m_storage(storage),
        m_filter(filter),
        m_handler(handler)
    {}

    virtual void execute()
    {
        Storage::Entries entries;
        m_storage.select(m_filter, entries);

        rapidjson::StringBuffer buf;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
        m_storage.print_json(writer, entries);

        std::string reply = buf.GetString();
        m_handler->response()->write(reply);
        m_handler->response()->close();
    }

private:
    Storage & m_storage;
    Filter m_filter;
    std::shared_ptr<on_get_snapshot> m_handler;
};

class RefreshEntries : public ThreadPool::Job
{
public:
    RefreshEntries(const Storage::Entries & entries,
            std::shared_ptr<on_refresh> handler)
        :
        m_entries(entries),
        m_handler(handler)
    {}

    static void perform(const Storage::Entries & entries,
            std::shared_ptr<on_refresh> & handler)
    {
        for (Node *node : entries.nodes)
            node->update_filesystems();

        for (Group *group : entries.groups)
            group->process_metadata();

        for (Couple *couple : entries.couples)
            couple->update_status();

        handler->response()->write("Refresh done");
        handler->response()->close();
    }

    virtual void execute()
    {
        perform(m_entries, m_handler);
    }

private:
    Storage::Entries m_entries;
    std::shared_ptr<on_refresh> m_handler;
};

class RefreshGroupDownload : public ThreadPool::Job
{
public:
    RefreshGroupDownload(WorkerApplication & app,
            const Storage::Entries & entries,
            std::shared_ptr<on_refresh> handler)
        :
        m_app(app),
        m_session(app.get_discovery().get_session().clone()),
        m_entries(entries),
        m_handler(handler)
    {}

    virtual void execute()
    {
        if (m_entries.groups.empty())
            RefreshEntries::perform(m_entries, m_handler);
        else
            m_app.get_storage().schedule_refresh(m_entries, m_session, m_handler);
    }

private:
    WorkerApplication & m_app;
    elliptics::session m_session;
    Storage::Entries m_entries;
    std::shared_ptr<on_refresh> m_handler;
};

class RefreshStart : public ThreadPool::Job
{
public:
    RefreshStart(WorkerApplication & app, const Filter & filter,
            std::shared_ptr<on_refresh> handler)
        :
        m_app(app),
        m_filter(filter),
        m_handler(handler)
    {}

    virtual void execute()
    {
        Storage::Entries entries;
        m_app.get_storage().select(m_filter, entries);

        if (m_app.get_discovery().in_progress()) {
            m_handler->response()->write("Discovery is in progress");
            m_handler->response()->close();
            return;
        }

        if (!entries.nodes.empty())
            m_app.get_discovery().discover_nodes(entries.nodes);

        m_app.get_thread_pool().dispatch_after(new RefreshGroupDownload(
                    m_app, entries, m_handler));
    }

private:
    WorkerApplication & m_app;
    Filter m_filter;
    std::shared_ptr<on_refresh> m_handler;
};

} // unnamed namespace

void Storage::Entries::sort()
{
    std::sort(couples.begin(), couples.end());
    std::sort(groups.begin(), groups.end());
    std::sort(backends.begin(), backends.end());
    std::sort(nodes.begin(), nodes.end());
    std::sort(filesystems.begin(), filesystems.end());
}

Storage::Storage(WorkerApplication & app)
    : m_app(app)
{}

Storage::~Storage()
{}

bool Storage::add_node(const char *host, int port, int family)
{
    char node_id[128];
    sprintf(node_id, "%s:%d:%d", host, port, family);

    std::string node_id_str(node_id);

    {
        ReadGuard<RWMutex> guard(m_nodes_lock);

        auto it = m_nodes.find(node_id_str);
        if (it != m_nodes.end()) {
            BH_LOG(m_app.get_logger(), DNET_LOG_DEBUG, "Node %s already exists", node_id_str.c_str());
            return false;
        }
    }

    {
        Node node(*this, host, port, family);

        BH_LOG(m_app.get_logger(), DNET_LOG_INFO, "New node %s", node_id_str.c_str());

        WriteGuard<RWMutex> guard(m_nodes_lock);

        return m_nodes.insert(std::make_pair(std::string(node_id), node)).second;
    }
}

void Storage::get_nodes(std::vector<Node *> & nodes)
{
    ReadGuard<RWMutex> guard(m_nodes_lock);

    nodes.reserve(m_nodes.size());
    for (auto it = m_nodes.begin(); it != m_nodes.end(); ++it)
        nodes.push_back(&it->second);
}

bool Storage::get_node(const std::string & key, Node *& node)
{
    ReadGuard<RWMutex> guard(m_nodes_lock);

    auto it = m_nodes.find(key);
    if (it != m_nodes.end()) {
        node = &it->second;
        return true;
    }
    return false;
}

void Storage::handle_backend(Backend & backend, bool existed)
{
    if (existed) {
        ReadGuard<RWMutex> guard(m_groups_lock);
        auto it = m_groups.find(backend.get_stat().group);
        if (it != m_groups.end()) {
            guard.release();
            Group & group = it->second;
            if (!group.has_backend(backend))
                group.add_backend(backend);
            return;
        }
    }

    {
        WriteGuard<RWMutex> guard(m_groups_lock);
        auto it = m_groups.lower_bound(backend.get_stat().group);
        if (it != m_groups.end() && it->first == int(backend.get_stat().group)) {
            guard.release();
            it->second.add_backend(backend);
        } else {
            it = m_groups.insert(it, std::make_pair(backend.get_stat().group, Group(backend, *this)));
        }
        backend.set_group(&it->second);
    }
}

void Storage::get_group_ids(std::vector<int> & groups) const
{
    ReadGuard<RWMutex> guard(m_groups_lock);

    groups.reserve(m_groups.size());
    for (auto it = m_groups.begin(); it != m_groups.end(); ++it)
        groups.push_back(it->first);
}

void Storage::get_groups(std::vector<Group*> & groups)
{
    ReadGuard<RWMutex> guard(m_groups_lock);

    groups.reserve(m_groups.size());
    for (auto it = m_groups.begin(); it != m_groups.end(); ++it)
        groups.push_back(&it->second);
}

bool Storage::get_group(int id, Group *& group)
{
    ReadGuard<RWMutex> guard(m_groups_lock);

    auto it = m_groups.find(id);
    if (it != m_groups.end()) {
        group = &it->second;
        return true;
    }
    return false;
}

void Storage::get_couples(std::vector<Couple*> & couples)
{
    ReadGuard<RWMutex> guard(m_couples_lock);

    couples.reserve(m_couples.size());
    for (auto it = m_couples.begin(); it != m_couples.end(); ++it)
        couples.push_back(&it->second);
}

Namespace *Storage::get_namespace(const std::string & name)
{
    Namespace *ns;
    if (get_namespace(name, ns))
        return ns;

    WriteGuard<RWSpinLock> guard(m_namespaces_lock);
    auto it = m_namespaces.lower_bound(name);
    if (it == m_namespaces.end() || it->first != name)
        it = m_namespaces.insert(it, std::make_pair(name, Namespace(name)));
    return &it->second;
}

bool Storage::get_namespace(const std::string & name, Namespace *& ns)
{
    ReadGuard<RWSpinLock> guard(m_namespaces_lock);

    auto it = m_namespaces.find(name);
    if (it != m_namespaces.end()) {
        ns = &it->second;
        return true;
    }
    return false;
}

void Storage::get_namespaces(std::vector<Namespace*> & namespaces)
{
    ReadGuard<RWSpinLock> guard(m_namespaces_lock);

    namespaces.reserve(m_namespaces.size());
    for (auto it = m_namespaces.begin(); it != m_namespaces.end(); ++it)
        namespaces.push_back(&it->second);
}

void Storage::schedule_update(elliptics::session & session)
{
    ReadGuard<RWMutex> guard(m_groups_lock);

    if (m_groups.empty()) {
        arm_timer();
        return;
    }

    UpdateJob *update = new UpdateJob(*this);
    UpdateJobToggle *toggle = new UpdateJobToggle(m_app.get_thread_pool(), update, m_groups.size());
    m_app.get_thread_pool().dispatch_pending(update);

    for (auto it = m_groups.begin(); it != m_groups.end(); ++it)
        schedule_metadata_download(session, toggle, &it->second);
}

void Storage::schedule_refresh(const Entries & entries, elliptics::session & session,
        std::shared_ptr<on_refresh> handler)
{
    RefreshEntries *refresh = new RefreshEntries(entries, handler);
    UpdateJobToggle *toggle = new UpdateJobToggle(m_app.get_thread_pool(), refresh, entries.groups.size());
    m_app.get_thread_pool().dispatch_pending(refresh);

    for (Group *group : entries.groups)
        schedule_metadata_download(session, toggle, group);
}

void Storage::schedule_metadata_download(elliptics::session & session,
        UpdateJobToggle *toggle, Group *group)
{
    std::vector<int> group_id(1);
    group_id[0] = group->get_id();

    static const elliptics::key key("symmetric_groups");

    GroupMetadataHandler *handler = new GroupMetadataHandler(session, group, toggle);

    elliptics::session *new_session = handler->get_session();
    new_session->set_namespace("metabalancer");
    new_session->set_groups(group_id);

    BH_LOG(m_app.get_logger(), DNET_LOG_DEBUG,
            "Scheduling metadata download for group %d", group_id[0]);

    elliptics::async_read_result res = new_session->read_data(key, group_id, 0, 0);
    res.connect(std::bind(&GroupMetadataHandler::result, handler, std::placeholders::_1),
            std::bind(&GroupMetadataHandler::final, handler, std::placeholders::_1));
}

void Storage::update_filesystems()
{
    std::vector<Node*> nodes;
    get_nodes(nodes);

    for (Node *node : nodes)
        node->update_filesystems();
}

void Storage::update_groups()
{
    std::vector<Group*> groups;

    {
        ReadGuard<RWMutex> guard(m_groups_lock);
        groups.reserve(m_groups.size());
        auto it = m_groups.begin();
        for (; it != m_groups.end(); ++it)
            groups.push_back(&it->second);
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
            couples.push_back(&it->second);
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
                if (it == m_groups.end() || it->first != id)
                    it = m_groups.insert(it, std::make_pair(id, Group(id, *this)));
                groups.push_back(&it->second);
            }
        }
    }

    {
        CoupleKey key(group_ids);

        WriteGuard<RWMutex> guard(m_couples_lock);

        auto it = m_couples.lower_bound(key);
        if (it == m_couples.end() || it->first != key) {
            it = m_couples.insert(it, std::make_pair(key, Couple(groups)));

            Couple & couple = it->second;
            couple.bind_groups();
            group->get_namespace()->add_couple(&couple);
        }
    }
}

void Storage::arm_timer()
{
    if (m_app.get_discovery_timer().arm(DiscoveryTimer::Subsequent) < 0) {
        int err = errno;
        BH_LOG(m_app.get_logger(), DNET_LOG_ERROR, "Failed to arm timer: %s", strerror(err));
    }
}

void Storage::select(Filter & filter, Entries & entries)
{
    filter.sort();

    bool have_groups = false;
    bool have_couples = false;
    bool have_nodes = false;
    bool have_backends = false;
    bool have_fs = false;

    if (filter.item_types & Filter::Group) {
        if (!filter.groups.empty()) {
            std::vector<Group*> candidate_groups;
            {
                ReadGuard<RWMutex> guard(m_groups_lock);
                for (int id : filter.groups) {
                    auto it = m_groups.find(id);
                    if (it != m_groups.end())
                        candidate_groups.push_back(&it->second);
                }
            }
            for (Group *group : candidate_groups) {
                if (group->match(filter, ~Filter::Group))
                    entries.groups.push_back(group);
            }
            have_groups = true;
        }
    }

    if (filter.item_types & Filter::Couple) {
        std::vector<Couple*> candidate_couples;
        if (!filter.couples.empty()) {
            std::vector<int> group_ids;
            group_ids.reserve(4);

            ReadGuard<RWMutex> guard(m_couples_lock);

            for (const std::string & couple_key_str : filter.couples) {
                if (couple_key_str.empty())
                    continue;

                size_t start = 0;
                size_t pos;
                do {
                    pos = couple_key_str.find(':', start);
                    group_ids.push_back(std::stoi(couple_key_str.substr(start, pos - start)));
                    start = pos + 1;
                } while (pos != std::string::npos);

                auto it = m_couples.find(CoupleKey(group_ids));
                if (it != m_couples.end())
                    candidate_couples.push_back(&it->second);

                group_ids.clear();
            }

            guard.release();

            for (Couple *couple : candidate_couples) {
                if (couple->match(filter, ~Filter::Couple))
                    entries.couples.push_back(couple);
            }
            have_couples = true;
        } else if (have_groups) {
            for (Group *group : entries.groups) {
                if (group->get_couple() != NULL)
                    candidate_couples.push_back(group->get_couple());
            }
            remove_duplicates(candidate_couples);
            for (Couple *couple : candidate_couples) {
                if (couple->match(filter, ~Filter::Group))
                    entries.couples.push_back(couple);
            }
            have_couples = true;
        } else if (!filter.namespaces.empty()) {
            for (const std::string & name : filter.namespaces) {
                Namespace *ns;
                if (get_namespace(name, ns)) {
                    std::vector<Couple*> couples;
                    ns->get_couples(couples);
                    for (Couple *couple : couples) {
                        if (couple->match(filter, ~Filter::Namespace))
                            entries.couples.push_back(couple);
                    }
                }
            }
            have_couples = true;
        } else {
            std::vector<Couple*> couples;
            get_couples(couples);
            for (Couple *couple : couples) {
                if (couple->match(filter))
                    entries.couples.push_back(couple);
            }
        }
    }

    if (have_couples && !have_groups && (filter.item_types & Filter::Group)) {
        std::vector<Group*> candidate_groups;
        for (Couple *couple : entries.couples) {
            couple->get_groups(candidate_groups);
            for (Group *group : candidate_groups) {
                if (group->match(filter, ~Filter::Couple & ~Filter::Group))
                    entries.groups.push_back(group);
            }
            candidate_groups.clear();
        }
        have_groups = true;
    }

    if (filter.item_types & Filter::Node) {
        if (!filter.nodes.empty()) {
            std::vector<Node*> candidate_nodes;
            {
                ReadGuard<RWMutex> guard(m_nodes_lock);
                for (const std::string & key : filter.nodes) {
                    auto it = m_nodes.find(key);
                    if (it != m_nodes.end())
                        candidate_nodes.push_back(&it->second);
                }
            }
            for (Node *node : candidate_nodes) {
                if (node->match(filter, ~Filter::Node))
                    entries.nodes.push_back(node);
            }
            have_nodes = true;
        } else if (have_groups) {
            std::vector<Node*> candidate_nodes;
            std::vector<Backend*> backends;
            for (Group *group : entries.groups) {
                group->get_backends(backends);
                for (Backend *backend : backends)
                    candidate_nodes.push_back(&backend->get_node());
                backends.clear();
            }
            remove_duplicates(candidate_nodes);
            for (Node *node : candidate_nodes) {
                if (node->match(filter, ~Filter::Group))
                    entries.nodes.push_back(node);
            }
            have_nodes = true;
        } else if (have_couples) {
            std::vector<Node*> candidate_nodes;
            std::vector<Group*> groups;
            std::vector<Backend*> backends;
            for (Couple *couple : entries.couples) {
                couple->get_groups(groups);
                for (Group *group : groups) {
                    group->get_backends(backends);
                    for (Backend *backend : backends)
                        candidate_nodes.push_back(&backend->get_node());
                    backends.clear();
                }
                groups.clear();
            }
            remove_duplicates(candidate_nodes);
            for (Node *node : candidate_nodes) {
                if (node->match(filter, ~Filter::Couple))
                    entries.nodes.push_back(node);
            }
            have_nodes = true;
        } else {
            std::vector<Node*> nodes;
            get_nodes(nodes);
            for (Node *node : nodes) {
                if (node->match(filter))
                    entries.nodes.push_back(node);
            }
            have_nodes = true;
        }
    }

    if (filter.item_types & Filter::Backend) {
        if (!filter.backends.empty()) {
            Node *node = NULL;
            for (const std::string & backend_key : filter.backends) {
                size_t pos = backend_key.rfind('/');
                if (pos == std::string::npos)
                    continue;
                std::string node_key = backend_key.substr(0, pos);
                if (node == NULL || node->get_key() != node_key) {
                    if (!get_node(node_key, node))
                        continue;
                }
                Backend *backend;
                if (node->get_backend(std::stoi(backend_key.substr(pos + 1)), backend)) {
                    if (backend->match(filter, ~Filter::Backend))
                        entries.backends.push_back(backend);
                }
            }
            have_backends = true;
        } else if (have_groups) {
            std::vector<Backend*> backends;
            for (Group *group : entries.groups) {
                group->get_backends(backends);
                for (Backend *backend : backends) {
                    if (backend->match(filter, ~Filter::Group))
                        entries.backends.push_back(backend);
                }
                backends.clear();
            }
            have_backends = true;
        } else if (have_couples) {
            std::vector<Group*> groups;
            std::vector<Backend*> backends;
            for (Couple *couple : entries.couples) {
                couple->get_groups(groups);
                for (Group *group : groups) {
                    group->get_backends(backends);
                    for (Backend *backend : backends) {
                        if (backend->match(filter, ~Filter::Couple))
                            entries.backends.push_back(backend);
                    }
                    backends.clear();
                }
                groups.clear();
            }
            have_backends = true;
        } else if (have_nodes) {
            std::vector<Backend*> backends;
            for (Node *node : entries.nodes) {
                node->get_backends(backends);
                for (Backend *backend : backends) {
                    if (backend->match(filter, ~Filter::Node))
                        entries.backends.push_back(backend);
                }
                backends.clear();
            }
            have_backends = true;
        }
    }

    if (filter.item_types & Filter::FS) {
        if (!filter.filesystems.empty()) {
            Node *node = NULL;
            for (const std::string & fs_key : filter.filesystems) {
                size_t pos = fs_key.rfind('/');
                if (pos == std::string::npos)
                    continue;
                std::string node_key = fs_key.substr(0, pos);
                if (node == NULL || node->get_key() != node_key) {
                    if (!get_node(node_key, node))
                        continue;
                }
                FS *fs;
                if (node->get_fs(std::stoull(fs_key.substr(pos + 1)), fs)) {
                    if (fs->match(filter, ~Filter::FS))
                        entries.filesystems.push_back(fs);
                }
            }
            have_fs = true;
        } else if (have_backends) {
            std::vector<FS*> candidate_fs;
            for (Backend *backend : entries.backends) {
                FS *fs = backend->get_fs();
                if (fs == NULL)
                    continue;
                candidate_fs.push_back(fs);
            }
            remove_duplicates(candidate_fs);
            for (FS *fs : candidate_fs) {
                if (fs->match(filter, ~Filter::Backend))
                    entries.filesystems.push_back(fs);
            }
            have_fs = true;
        } else if (have_nodes) {
            std::vector<FS*> candidate_fs;
            for (Node *node : entries.nodes) {
                node->get_filesystems(candidate_fs);
                for (FS *fs : candidate_fs) {
                    if (fs->match(filter, ~Filter::Node))
                        entries.filesystems.push_back(fs);
                }
                candidate_fs.clear();
            }
            have_fs = true;
        } else if (have_groups) {
            std::vector<Backend*> backends;
            std::vector<FS*> candidate_fs;
            for (Group *group : entries.groups) {
                group->get_backends(backends);
                for (Backend *backend : backends) {
                    FS *fs = backend->get_fs();
                    if (fs == NULL)
                        continue;
                    candidate_fs.push_back(fs);
                }
                backends.clear();
            }
            remove_duplicates(candidate_fs);
            for (FS *fs : candidate_fs) {
                if (fs->match(filter, ~Filter::Group))
                    entries.filesystems.push_back(fs);
            }
            have_fs = true;
        } else if (have_couples) {
            std::vector<Group*> groups;
            std::vector<Backend*> backends;
            std::vector<FS*> candidate_fs;
            for (Couple *couple : entries.couples) {
                couple->get_groups(groups);
                for (Group *group : groups) {
                    group->get_backends(backends);
                    for (Backend *backend : backends) {
                        FS *fs = backend->get_fs();
                        if (fs == NULL)
                            continue;
                        candidate_fs.push_back(fs);
                    }
                    backends.clear();
                }
                groups.clear();
            }
            remove_duplicates(candidate_fs);
            for (FS *fs : candidate_fs) {
                if (fs->match(filter, ~Filter::Couple))
                    entries.filesystems.push_back(fs);
            }
            have_fs = true;
        } else {
            std::vector<Backend*> backends;
            std::vector<Node*> nodes;
            get_nodes(nodes);
            for (Node *node : nodes) {
                node->get_backends(backends);
                for (Backend *backend : backends) {
                    FS *fs = backend->get_fs();
                    if (fs == NULL)
                        continue;
                    if (fs->match(filter))
                        entries.filesystems.push_back(fs);
                }
                backends.clear();
            }
            have_fs = true;
        }
    }

    if (!have_backends && (filter.item_types & Filter::Backend)) {
        if (have_fs) {
            std::vector<Backend*> backends;
            for (FS *fs : entries.filesystems) {
                fs->get_backends(backends);
                for (Backend *backend : backends) {
                    if (backend->match(filter, ~Filter::FS))
                        entries.backends.push_back(backend);
                }
                backends.clear();
            }
            have_backends = true;
        } else {
            std::vector<Backend*> backends;
            std::vector<Node*> nodes;
            get_nodes(nodes);
            for (Node *node : nodes) {
                node->get_backends(backends);
                for (Backend *backend : backends) {
                    if (backend->match(filter))
                        entries.backends.push_back(backend);
                }
                backends.clear();
            }
            have_backends = true;
        }
    }

    if (have_backends || have_fs) {
        for (Backend *backend : entries.backends)
            entries.nodes.push_back(&backend->get_node());
        for (FS *fs : entries.filesystems)
            entries.nodes.push_back(&fs->get_node());
        remove_duplicates(entries.nodes);
    }
}

void Storage::get_snapshot(const Filter & filter, std::shared_ptr<on_get_snapshot> handler)
{
    m_app.get_discovery().take_over_snapshot(new SnapshotJob(*this, filter, handler));
}

void Storage::refresh(const Filter & filter, std::shared_ptr<on_refresh> handler)
{
    m_app.get_thread_pool().dispatch_after(new RefreshStart(m_app, filter, handler));
}

void Storage::print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer, Entries & entries)
{
    std::vector<Node*> nodes;
    get_nodes(nodes);

    std::vector<Group*> groups;
    get_groups(groups);

    std::vector<Couple*> couples;
    get_couples(couples);

    entries.sort();

    writer.StartObject();

    std::vector<Backend*> *backends = (entries.backends.empty() ? NULL : &entries.backends);
    std::vector<FS*> *filesystems = (entries.filesystems.empty() ? NULL : &entries.filesystems);

    writer.Key("nodes");
    writer.StartArray();
    for (Node *node : entries.nodes)
        node->print_json(writer, backends, filesystems);
    writer.EndArray();

    writer.Key("groups");
    writer.StartArray();
    for (Group *group : entries.groups)
        group->print_json(writer);
    writer.EndArray();

    writer.Key("couples");
    writer.StartArray();
    for (Couple *couple : entries.couples)
        couple->print_json(writer);
    writer.EndArray();

    writer.EndObject();
}
