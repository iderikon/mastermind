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
    Couple(Storage & storage, const std::vector<Group*> & groups);
    Couple(Storage & storage);

    void clone_from(const Couple & other);

    const std::string & get_key() const
    { return m_key; }

    bool check(const std::vector<int> & groups) const;

    void bind_groups();

    void get_group_ids(std::vector<int> & groups) const;
    void get_groups(std::vector<Group*> & groups) const;

    // NB: get_items() may return duplicates
    void get_items(std::vector<Group*> & groups) const;
    void get_items(std::vector<Namespace*> & namespaces) const;
    void get_items(std::vector<Node*> & nodes) const;
    void get_items(std::vector<Backend*> & backends) const;
    void get_items(std::vector<FS*> & filesystems) const;

    void update_status();

    Status get_status() const
    { return m_status; }

    const char *get_status_text() const
    { return m_status_text; }

    uint64_t get_update_status_time() const
    { return m_update_status_time; }

    void merge(const Couple & other);

    void print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer,
            bool show_internals) const;

private:
    Storage & m_storage;
    std::vector<Group*> m_groups;
    std::string m_key;

    Status m_status;
    const char *m_status_text;

    uint64_t m_update_status_time;
};

#endif

