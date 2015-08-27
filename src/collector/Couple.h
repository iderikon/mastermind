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

#ifndef __558a6717_eede_4557_af95_a7447e1ae5ff
#define __558a6717_eede_4557_af95_a7447e1ae5ff

#include "Metrics.h"

#include <iostream>
#include <rapidjson/writer.h>
#include <vector>

class Backend;
class Filter;
class FS;
class Group;
class Namespace;
class Node;
class Storage;

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
    Couple(const std::vector<std::reference_wrapper<Group>> & groups);

    const std::string & get_key() const
    { return m_key; }

    const std::vector<std::reference_wrapper<Group>> & get_groups() const
    { return m_groups; }

    // NB: get_items() may return duplicates
    void get_items(std::vector<std::reference_wrapper<Group>> & groups) const;
    void get_items(std::vector<std::reference_wrapper<Namespace>> & namespaces) const;
    void get_items(std::vector<std::reference_wrapper<Node>> & nodes) const;
    void get_items(std::vector<std::reference_wrapper<Backend>> & backends) const;
    void get_items(std::vector<std::reference_wrapper<FS>> & filesystems) const;

    void update_status(bool forbidden_unmatched_total);

    Status get_status() const
    { return m_status; }

    uint64_t get_update_status_duration() const
    { return m_update_status_duration; }

    void merge(const Couple & other, bool & have_newer);

    void print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer,
            bool show_internals) const;

private:
    template <typename T, typename V>
    void modify(T & member, const V & value)
    {
        if (member != value) {
            member = value;
            clock_get(m_modified_time);
        }
    }

private:
    std::string m_key;
    std::vector<std::reference_wrapper<Group>> m_groups;

    Status m_status;
    std::string m_status_text;

    uint64_t m_modified_time;
    uint64_t m_update_status_duration;
};

#endif

