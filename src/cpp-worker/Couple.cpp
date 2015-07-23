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
#include "Group.h"
#include "Guard.h"

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

void Couple::update_status()
{
    ReadGuard<RWSpinLock> guard(m_groups_lock);

    if (m_groups.empty()) {
        m_status = BAD;
        m_status_text = "couple has no groups";
        return;
    }

    std::vector<Group::Status> statuses;

    Group *g = m_groups[0];
    statuses.push_back(g->get_status());

    bool have_frozen = g->get_frozen();

    for (size_t i = 1; i < m_groups.size(); ++i) {
        if (!g->metadata_equals(*m_groups[i])) {
            m_status = BAD;
            m_status_text = "groups have different metadata";
            return;
        }

        statuses.push_back(m_groups[i]->get_status());
        if (m_groups[i]->get_frozen())
            have_frozen = true;
    }

    if (have_frozen) {
        m_status = FROZEN;
        m_status_text = "some groups are frozen";
        return;
    }

    if (size_t(std::count(statuses.begin(), statuses.end(), Group::COUPLED)) == statuses.size()) {
        // TODO: forbidden unmatched group total space
        // TODO: full
        m_status = OK;
        m_status_text = "couple is OK";
        return;
    }

    for (size_t i = 0; i < statuses.size(); ++i) {
        Group::Status status = statuses[i];
        if (status == Group::INIT) {
            m_status = INIT;
            m_status_text = "some groups are uninitialized";
        } else if (status == Group::BAD) {
            m_status = BAD;
            m_status_text = "some groups are in state BAD";
        } else if (status == Group::BROKEN) {
            m_status = BROKEN;
            m_status_text = "some groups are in state BROKEN";
        } else if (status == Group::RO || status == Group::MIGRATING) {
            m_status = BAD;
            m_status_text = "some groups are read-only";
        }
    }

    m_status = BAD;
    m_status_text = "couple is BAD for unknown reason";

    // TODO: account job
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
