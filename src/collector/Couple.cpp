/*
   Copyright (c) YANDEX LLC, 2015. All rights reserved.
   This file is part of Mastermind.

   Mastermind is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 3.0 of the License, or (at your option) any later version.

   Mastermind is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public
   License along with Mastermind.
*/

#include "Backend.h"
#include "Couple.h"
#include "Filter.h"
#include "FS.h"
#include "Group.h"
#include "Namespace.h"
#include "Node.h"

#include <algorithm>

Couple::Couple(const std::vector<std::reference_wrapper<Group>> & groups)
    :
    m_groups(groups),
    m_status(INIT),
    m_modified_time(0),
    m_update_status_duration(0)
{
    for (size_t i = 0; i < groups.size(); ++i) {
        m_key += std::to_string(groups[i].get().get_id());
        if (i != (groups.size() - 1))
            m_key += ':';
    }
}

void Couple::update_status(bool forbidden_unmatched_total)
{
    Stopwatch watch(m_update_status_duration);

    if (m_groups.empty()) {
        modify(m_status, BAD);
        modify(m_status_text, "Couple has no groups");
        return;
    }

    std::vector<Group::Status> statuses;

    Group & g = m_groups[0];
    statuses.push_back(g.get_status());

    bool have_frozen = g.get_frozen();

    for (size_t i = 1; i < m_groups.size(); ++i) {
        if (g.check_metadata_equals(m_groups[i]) != 0) {
            modify(m_status, BAD);
            modify(m_status_text, "Groups have different metadata");
            return;
        }

        statuses.push_back(m_groups[i].get().get_status());
        if (m_groups[i].get().get_frozen())
            have_frozen = true;
    }

    if (have_frozen) {
        modify(m_status, FROZEN);
        modify(m_status_text, "Some groups are frozen");
        return;
    }

    if (size_t(std::count(statuses.begin(), statuses.end(), Group::COUPLED)) == statuses.size()) {
        if (forbidden_unmatched_total) {
            for (size_t i = 1; i < m_groups.size(); ++i) {
                if (m_groups[i].get().get_total_space() != m_groups[0].get().get_total_space()) {
                    modify(m_status, BROKEN);
                    modify(m_status_text, "Couple has unequal total space in groups");
                    return;
                }
            }
        }

        bool full = false;
        for (Group & group : m_groups) {
            if (group.full()) {
                full = true;
                break;
            }
        }

        if (full) {
            modify(m_status, FULL);
            modify(m_status_text, "Couple is FULL");
        } else {
            modify(m_status, OK);
            modify(m_status_text, "Couple is OK");
        }
        return;
    }

    for (size_t i = 0; i < statuses.size(); ++i) {
        Group::Status status = statuses[i];
        if (status == Group::INIT) {
            modify(m_status, INIT);
            modify(m_status_text, "Some groups are uninitialized");
            return;
        } else if (status == Group::BAD) {
            modify(m_status, BAD);
            modify(m_status_text, "Some groups are in state BAD");
            return;
        } else if (status == Group::BROKEN) {
            modify(m_status, BROKEN);
            modify(m_status_text, "Some groups are in state BROKEN");
            return;
        } else if (status == Group::RO || status == Group::MIGRATING) {
            modify(m_status, BAD);
            modify(m_status_text, "Some groups are read-only");
            return;
        }
    }

    modify(m_status, BAD);
    modify(m_status_text, "Couple is BAD for unknown reason");

    // TODO: account job
}

void Couple::merge(const Couple & other, bool & have_newer)
{
    if (m_modified_time > other.m_modified_time) {
        have_newer = true;
        return;
    }

    m_status = other.m_status;
    m_status_text = other.m_status_text;
    m_update_status_duration = other.m_update_status_duration;
}

void Couple::get_items(std::vector<std::reference_wrapper<Group>> & groups) const
{
    groups.insert(groups.end(), m_groups.begin(), m_groups.end());
}

void Couple::get_items(std::vector<std::reference_wrapper<Namespace>> & namespaces) const
{
    if (!m_groups.empty())
        m_groups.front().get().get_items(namespaces);
}

void Couple::get_items(std::vector<std::reference_wrapper<Node>> & nodes) const
{
    for (Group & group : m_groups)
        group.get_items(nodes);
}

void Couple::get_items(std::vector<std::reference_wrapper<Backend>> & backends) const
{
    for (Group & group : m_groups)
        group.get_items(backends);
}

void Couple::get_items(std::vector<std::reference_wrapper<FS>> & filesystems) const
{
    for (Group & group : m_groups)
        group.get_items(filesystems);
}

void Couple::print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer,
        bool show_internals) const
{
    writer.StartObject();

    writer.Key("groups");
    writer.StartArray();
    for (Group & group : m_groups)
        writer.Uint64(group.get_id());
    writer.EndArray();

    writer.Key("status");
    writer.String(status_str(m_status));
    writer.Key("status_text");
    writer.String(m_status_text.c_str());

    if (show_internals) {
        writer.Key("update_status_duration");
        writer.Uint64(m_update_status_duration);
    }

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
