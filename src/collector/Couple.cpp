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
#include "Host.h"
#include "Namespace.h"
#include "Node.h"

#include <algorithm>

Couple::Couple(const std::vector<std::reference_wrapper<Group>> & groups)
    :
    m_groups(groups),
    m_internal_status(INIT_Init),
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

void Couple::update_status(bool forbidden_dc_sharing, bool forbidden_unmatched_total)
{
    Stopwatch watch(m_update_status_duration);

    if (m_groups.empty()) {
        if (m_internal_status != BAD_NoGroups) {
            m_internal_status = BAD_NoGroups;
            m_status = BAD;
            m_status_text = "Couple has no groups";
        }
        return;
    }

    std::vector<Group::Status> statuses;

    Group & g = m_groups[0];
    statuses.push_back(g.get_status());

    bool have_frozen = g.get_frozen();
    uint64_t most_recent = g.get_update_time();

    for (size_t i = 1; i < m_groups.size(); ++i) {
        uint64_t cur_update_time = m_groups[i].get().get_update_time();
        if (cur_update_time > most_recent)
            most_recent = cur_update_time;

        if (g.check_metadata_equals(m_groups[i]) != 0) {
            if (m_internal_status != BAD_DifferentMetadata) {
                if (m_modified_time < most_recent)
                    m_modified_time = most_recent;

                m_internal_status = BAD_DifferentMetadata;
                m_status = BAD;
                m_status_text = "Groups have different metadata";

                for (Group & group : m_groups)
                    group.set_coupled_status(false, m_modified_time);
            }
            account_job_in_status();
            return;
        }

        statuses.push_back(m_groups[i].get().get_status());
        if (m_groups[i].get().get_frozen())
            have_frozen = true;
    }

    if (have_frozen) {
        if (m_internal_status != FROZEN_Frozen) {
            if (m_modified_time < most_recent)
                m_modified_time = most_recent;

            m_internal_status = FROZEN_Frozen;
            m_status = FROZEN;
            m_status_text = "Some groups are frozen";

            for (Group & group : m_groups)
                group.set_coupled_status(true, m_modified_time);
        }
        return;
    }

    if (forbidden_dc_sharing) {
        if (check_dc_sharing() != 0)
            return;
    }

    if (size_t(std::count(statuses.begin(), statuses.end(), Group::COUPLED)) == statuses.size()) {
        if (forbidden_unmatched_total) {
            for (size_t i = 1; i < m_groups.size(); ++i) {
                if (m_groups[i].get().get_total_space() != m_groups[0].get().get_total_space()) {
                    if (m_internal_status != BROKEN_UnequalTotalSpace) {
                        if (m_modified_time < most_recent)
                            m_modified_time = most_recent;

                        m_internal_status = BROKEN_UnequalTotalSpace;
                        m_status = BROKEN;
                        m_status_text = "Couple has unequal total space in groups";

                        for (Group & group : m_groups)
                            group.set_coupled_status(false, m_modified_time);
                    }
                    return;
                }
            }
        }

        uint64_t backend_update = 0;
        bool full = false;
        for (Group & group : m_groups) {
            if (group.full()) {
                full = true;
                backend_update = group.get_backend_update_time();
                break;
            }
        }

        if (full) {
            if (m_internal_status != FULL_Full) {
                m_modified_time = std::max(backend_update, std::max(most_recent, m_modified_time));
                m_internal_status = FULL_Full;
                m_status = FULL;
                m_status_text = "Couple is FULL";
            }
        } else {
            if (m_internal_status != OK_OK) {
                if (m_modified_time < most_recent)
                    m_modified_time = most_recent;

                m_internal_status = OK_OK;
                m_status = OK;
                m_status_text = "Couple is OK";
            }
        }

        for (Group & group : m_groups)
            group.set_coupled_status(true, m_modified_time);

        return;
    }

    size_t i = 0;
    for (; i < statuses.size(); ++i) {
        Group::Status status = statuses[i];
        if (status == Group::INIT) {
            if (m_internal_status != BAD_GroupUninitialized) {
                if (m_modified_time < most_recent)
                    m_modified_time = most_recent;

                m_internal_status = BAD_GroupUninitialized;
                m_status = BAD;
                m_status_text = "Some groups are uninitialized";
            }
            break;
        } else if (status == Group::BAD) {
            if (m_internal_status != BAD_GroupBAD) {
                if (m_modified_time < most_recent)
                    m_modified_time = most_recent;

                m_internal_status = BAD_GroupBAD;
                m_status = BAD;
                m_status_text = "Some groups are in state BAD";
            }
            break;
        } else if (status == Group::BROKEN) {
            if (m_internal_status != BROKEN_GroupBROKEN) {
                if (m_modified_time < most_recent)
                    m_modified_time = most_recent;

                m_internal_status = BROKEN_GroupBROKEN;
                m_status = BROKEN;
                m_status_text = "Some groups are in state BROKEN";
            }
            break;
        } else if (status == Group::RO || status == Group::MIGRATING) {
            if (m_internal_status != BAD_ReadOnly) {
                if (m_modified_time < most_recent)
                    m_modified_time = most_recent;

                m_internal_status = BAD_ReadOnly;
                m_status = BAD;
                m_status_text = "Some groups are read-only";
            }
            break;
        }
    }

    // The condition (i == statuses.size()) is true when we inspected
    // all groups (i.e., the loop completed) but haven't encountered
    // any known case the couple becomes BAD.
    if (i == statuses.size() && m_internal_status != BAD_Unknown) {
        if (m_modified_time < most_recent)
            m_modified_time = most_recent;

        m_internal_status = BAD_Unknown;
        m_status = BAD;
        m_status_text = "Couple is BAD for unknown reason";
    }

    account_job_in_status();

    if (i < statuses.size()) {
        for (size_t j = 0; j < m_groups.size(); ++j) {
            if (j != i)
                m_groups[j].get().set_coupled_status(false, m_modified_time);
        }
        return;
    }
}

void Couple::merge(const Couple & other, bool & have_newer)
{
    if (m_modified_time > other.m_modified_time) {
        have_newer = true;
        return;
    }

    m_internal_status = other.m_internal_status;
    m_status = other.m_status;
    m_status_text = other.m_status_text;
    m_update_status_duration = other.m_update_status_duration;
}

void Couple::push_items(std::vector<std::reference_wrapper<Group>> & groups) const
{
    groups.insert(groups.end(), m_groups.begin(), m_groups.end());
}

void Couple::push_items(std::vector<std::reference_wrapper<Namespace>> & namespaces) const
{
    if (!m_groups.empty())
        m_groups.front().get().push_items(namespaces);
}

void Couple::push_items(std::vector<std::reference_wrapper<Node>> & nodes) const
{
    for (Group & group : m_groups)
        group.push_items(nodes);
}

void Couple::push_items(std::vector<std::reference_wrapper<Backend>> & backends) const
{
    for (Group & group : m_groups)
        group.push_items(backends);
}

void Couple::push_items(std::vector<std::reference_wrapper<FS>> & filesystems) const
{
    for (Group & group : m_groups)
        group.push_items(filesystems);
}

bool Couple::account_job_in_status()
{
    if (m_status != BAD)
        return false;

    for (Group & group : m_groups) {
        if (group.has_active_job()) {
            const Job & job = group.get_active_job();
            Job::Type type = job.get_type();
            Job::Status status = job.get_status();

            if (type != Job::MOVE_JOB && type != Job::RESTORE_GROUP_JOB)
                return false;

            if (status == Job::NEW || status == Job::EXECUTING) {
                m_internal_status = SERVICE_ACTIVE_ServiceActive;
                m_status = SERVICE_ACTIVE;

                m_status_text = "Couple has active job ";
                m_status_text += job.get_id();
            } else {
                m_internal_status = SERVICE_STALLED_ServiceStalled;
                m_status = SERVICE_STALLED;

                m_status_text = "Couple has stalled job ";
                m_status_text += job.get_id();
            }

            if (m_modified_time < group.get_update_time())
                m_modified_time = group.get_update_time();

            return true;
        }
    }

    return false;
}

int Couple::check_dc_sharing()
{
    std::vector<std::string> all_dcs;
    uint64_t most_recent_backend_ts = 0;

    for (Group & group : m_groups) {
        const auto & backends = group.get_backends();

        std::vector<std::string> dcs;
        dcs.reserve(backends.size());

        for (const Backend & backend : backends) {
            // Convert backend timestamp to nanoseconds
            uint64_t backend_ts = backend.get_stat().get_timestamp() * 1000ULL;
            if (most_recent_backend_ts < backend_ts)
                most_recent_backend_ts = backend_ts;

            const std::string & dc = backend.get_node().get_host().get_dc();
            if (dc.empty()) {
                if (m_internal_status != BAD_DcResolveFailed) {
                    std::ostringstream ostr;
                    ostr << "Group " << group.get_id() << ": Failed to resolve "
                            "DC for node " << backend.get_node().get_key();

                    m_internal_status = BAD_DcResolveFailed;
                    m_status = BAD;
                    m_status_text = ostr.str();

                    if (m_modified_time < most_recent_backend_ts)
                        m_modified_time = most_recent_backend_ts;
                }

                for (Group & group : m_groups)
                    group.set_coupled_status(false, m_modified_time);

                return -1;
            }
            dcs.push_back(dc);
        }

        std::sort(dcs.begin(), dcs.end());
        auto it = std::unique(dcs.begin(), dcs.end());
        if (it != dcs.end())
            dcs.erase(it, dcs.end());
        all_dcs.insert(all_dcs.end(), dcs.begin(), dcs.end());
    }

    std::sort(all_dcs.begin(), all_dcs.end());
    auto it = std::unique(all_dcs.begin(), all_dcs.end());
    if  (it != all_dcs.end()) {
        if (m_internal_status != BROKEN_DcSharing) {
            m_internal_status = BROKEN_DcSharing;
            m_status = BROKEN;

            m_status_text = "Couple has nodes sharing the same DC";

            if (m_modified_time < most_recent_backend_ts)
                m_modified_time = most_recent_backend_ts;

            return -1;
        }
    }

    return 0;
}

void Couple::print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer, bool show_internals) const
{
    writer.StartObject();

    writer.Key("id");
    writer.String(m_key.c_str());

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
        writer.Key("modified_time");
        writer.Uint64(m_modified_time);
        writer.Key("internal_status");
        writer.String(internal_status_str(m_internal_status));
    }

    writer.EndObject();
}

const char *Couple::internal_status_str(InternalStatus status)
{
    switch (status) {
    case INIT_Init:
        return "INIT_Init";
    case BAD_NoGroups:
        return "BAD_NoGroups";
    case BAD_DifferentMetadata:
        return "BAD_DifferentMetadata";
    case BAD_GroupUninitialized:
        return "BAD_GroupUninitialized";
    case BAD_GroupBAD:
        return "BAD_GroupBAD";
    case BAD_ReadOnly:
        return "BAD_ReadOnly";
    case BAD_DcResolveFailed:
        return "BAD_DcResolveFailed";
    case BAD_Unknown:
        return "BAD_Unknown";
    case BROKEN_DcSharing:
        return "BROKEN_DcSharing";
    case BROKEN_GroupBROKEN:
        return "BROKEN_GroupBROKEN";
    case BROKEN_UnequalTotalSpace:
        return "BROKEN_UnequalTotalSpace";
    case FROZEN_Frozen:
        return "FROZEN_Frozen";
    case FULL_Full:
        return "FULL_Full";
    case SERVICE_ACTIVE_ServiceActive:
        return "SERVICE_ACTIVE_ServiceActive";
    case SERVICE_STALLED_ServiceStalled:
        return "SERVICE_STALLED_ServiceStalled";
    case OK_OK:
        return "OK_OK";
    }
    return "UNKNOWN";
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
