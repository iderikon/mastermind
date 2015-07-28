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

#include "Backend.h"
#include "Couple.h"
#include "Filter.h"
#include "FS.h"
#include "Group.h"
#include "Guard.h"
#include "Namespace.h"
#include "Node.h"

#include <algorithm>

Couple::Couple(const std::vector<Group*> & groups)
    :
    m_status(INIT),
    m_status_text("")
{
    m_groups = groups;
}

bool Couple::check(const std::vector<int> & groups) const
{
    ReadGuard<RWSpinLock> guard(m_groups_lock);

    if (m_groups.size() != groups.size())
        return false;
    for (size_t i = 0; i < m_groups.size(); ++i)
        if (m_groups[i]->get_id() != groups[i])
            return false;

    return true;
}

void Couple::bind_groups()
{
    m_key.clear();
    for (size_t i = 0; i < m_groups.size(); ++i) {
        m_groups[i]->set_couple(this);
        m_key += std::to_string(m_groups[i]->get_id());
        if (i != (m_groups.size() - 1))
            m_key += ':';
    }
}

void Couple::get_group_ids(std::vector<int> & groups) const
{
    ReadGuard<RWSpinLock> guard(m_groups_lock);

    groups.reserve(m_groups.size());
    for (size_t i = 0; i < m_groups.size(); ++i)
        groups.push_back(m_groups[i]->get_id());
}

void Couple::get_groups(std::vector<Group*> & groups) const
{
    ReadGuard<RWSpinLock> guard(m_groups_lock);
    groups.assign(m_groups.begin(), m_groups.end());
}

void Couple::update_status()
{
    ReadGuard<RWSpinLock> guard(m_groups_lock);

    if (m_groups.empty()) {
        m_status = BAD;
        m_status_text = "Couple has no groups";
        return;
    }

    std::vector<Group::Status> statuses;

    Group *g = m_groups[0];
    statuses.push_back(g->get_status());

    bool have_frozen = g->get_frozen();

    for (size_t i = 1; i < m_groups.size(); ++i) {
        if (!g->metadata_equals(*m_groups[i])) {
            m_status = BAD;
            m_status_text = "Groups have different metadata";
            return;
        }

        statuses.push_back(m_groups[i]->get_status());
        if (m_groups[i]->get_frozen())
            have_frozen = true;
    }

    if (have_frozen) {
        m_status = FROZEN;
        m_status_text = "Some groups are frozen";
        return;
    }

    if (size_t(std::count(statuses.begin(), statuses.end(), Group::COUPLED)) == statuses.size()) {
        // TODO: forbidden unmatched group total space

        bool full = false;
        for (Group *group : m_groups) {
            if (group->full()) {
                full = true;
                break;
            }
        }

        if (full) {
            m_status = FULL;
            m_status_text = "Couple is FULL";
        } else {
            m_status = OK;
            m_status_text = "Couple is OK";
        }
        return;
    }

    for (size_t i = 0; i < statuses.size(); ++i) {
        Group::Status status = statuses[i];
        if (status == Group::INIT) {
            m_status = INIT;
            m_status_text = "Some groups are uninitialized";
        } else if (status == Group::BAD) {
            m_status = BAD;
            m_status_text = "Some groups are in state BAD";
        } else if (status == Group::BROKEN) {
            m_status = BROKEN;
            m_status_text = "Some groups are in state BROKEN";
        } else if (status == Group::RO || status == Group::MIGRATING) {
            m_status = BAD;
            m_status_text = "Some groups are read-only";
        }
    }

    m_status = BAD;
    m_status_text = "Couple is BAD for unknown reason";

    // TODO: account job
}

bool Couple::match(const Filter & filter, uint32_t item_types) const
{
    if ((item_types & Filter::Couple) && !filter.couples.empty()) {
        if (!std::binary_search(filter.couples.begin(), filter.couples.end(), m_key))
            return false;
    }

    bool check_groups = (item_types & Filter::Group) && !filter.groups.empty();
    bool check_namespace = (item_types && Filter::Namespace) && !filter.namespaces.empty();
    bool check_nodes = (item_types & Filter::Node) && !filter.nodes.empty();
    bool check_backends = (item_types & Filter::Backend) && !filter.backends.empty();
    bool check_fs = (item_types & Filter::FS) && !filter.filesystems.empty();

    if (!check_groups && !check_namespace && !check_nodes && !check_backends && !check_fs)
        return true;

    ReadGuard<RWSpinLock> guard(m_groups_lock);

    if (m_groups.empty())
        return false;

    if (check_namespace) {
        Namespace *ns = m_groups[0]->get_namespace();
        if (ns == NULL)
            return false;

        if (!std::binary_search(filter.namespaces.begin(), filter.namespaces.end(),
                    ns->get_name()))
            return false;
    }

    if (check_groups) {
        bool found = false;
        for (Group *group : m_groups) {
            if (std::binary_search(filter.groups.begin(), filter.groups.end(),
                        group->get_id())) {
                found = true;
                break;
            }
        }
        if (!found)
            return false;
    }

    bool found_backend = false;
    bool found_node = false;
    bool found_fs = false;

    std::vector<Backend*> backends;
    for (Group *group : m_groups) {
        group->get_backends(backends);
        for (Backend *backend : backends) {
            if (check_backends && !found_backend) {
                if (std::binary_search(filter.backends.begin(), filter.backends.end(),
                            backend->get_key()))
                    found_backend = true;
            }
            if (check_nodes && !found_node) {
                if (std::binary_search(filter.nodes.begin(), filter.nodes.end(),
                            backend->get_node().get_key()))
                    found_node = true;
            }
            if (check_fs && !found_fs) {
                if (backend->get_fs() != NULL && std::binary_search(filter.filesystems.begin(),
                            filter.filesystems.end(), backend->get_fs()->get_key()))
                    found_fs = true;
            }
            if (check_backends == found_backend && check_nodes == found_node && check_fs == found_fs)
                return true;
        }
        backends.clear();
    }

    return false;
}

void Couple::print_info(std::ostream & ostr) const
{
    ostr << "Couple {\n"
            "  key: " << m_key << "\n"
            "  groups: [ ";

    {
        ReadGuard<RWSpinLock> guard(m_groups_lock);
        for (size_t i = 0; i < m_groups.size(); ++i)
            ostr << m_groups[i]->get_id() << ' ';
    }

    ostr << "]\n"
            "  status: " << status_str(m_status) << "\n"
            "  status_text: '" << m_status_text << "'\n"
            "}";
}

void Couple::print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer) const
{
    writer.StartObject();

    writer.Key("groups");
    writer.StartArray();
    {
        ReadGuard<RWSpinLock> guard(m_groups_lock);
        for (Group *group : m_groups)
            writer.Uint64(group->get_id());
    }
    writer.EndArray();

    writer.Key("status");
    writer.String(status_str(m_status));
    writer.Key("status_text");
    writer.String(m_status_text);

    writer.EndObject();
}

const char *Couple::status_str(Status status)
{
    switch (status)
    {
    case INIT:
        return "INIT";
    case OK:
        return "OK";
    case FULL:
        return "FULL";
    case BAD:
        return "BAD";
    case BROKEN:
        return "BROKEN";
    case RO:
        return "RO";
    case FROZEN:
        return "FROZEN";
    case MIGRATING:
        return "MIGRATING";
    case SERVICE_ACTIVE:
        return "SERVICE_ACTIVE";
    case SERVICE_STALLED:
        return "SERVICE_STALLED";
    }
    return "UNKNOWN";
}
