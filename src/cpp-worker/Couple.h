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

#ifndef __558a6717_eede_4557_af95_a7447e1ae5ff
#define __558a6717_eede_4557_af95_a7447e1ae5ff

#include "RWSpinLock.h"

#include <iostream>
#include <rapidjson/writer.h>
#include <vector>

class Filter;
class Group;

class Couple
{
public:
    enum Status {
        INIT,
        OK,
        FULL,
        BAD,
        BROKEN,
        RO,
        FROZEN,
        MIGRATING,
        SERVICE_ACTIVE,
        SERVICE_STALLED
    };

    static const char *status_str(Status status);

public:
    Couple(const std::vector<Group*> & groups, bool forbidden_unmatched_space);

    const std::string & get_key() const
    { return m_key; }

    bool check(const std::vector<int> & groups) const;

    // no lock
    void bind_groups();

    void get_group_ids(std::vector<int> & groups) const;
    void get_groups(std::vector<Group*> & groups) const;

    void update_status();

    Status get_status() const
    { return m_status; }

    const char *get_status_text() const
    { return m_status_text; }

    uint64_t get_update_status_time() const
    { return m_update_status_time; }

    bool match(const Filter & filter, uint32_t item_types = 0xFFFFFFFF) const;

    void print_info(std::ostream & ostr) const;
    void print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer) const;

private:
    std::vector<Group*> m_groups;
    std::string m_key;
    mutable RWSpinLock m_groups_lock;

    bool m_forbidden_unmatched_space;

    Status m_status;
    const char *m_status_text;

    uint64_t m_update_status_time;
};

#endif

