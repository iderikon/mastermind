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

#ifndef __c065a812_f800_4562_8686_a77b1e60e201
#define __c065a812_f800_4562_8686_a77b1e60e201

#include <cstdint>
#include <rapidjson/writer.h>
#include <set>
#include <string>
#include <vector>

#include "RWSpinLock.h"

class Backend;
class Node;
class Storage;

struct FSStat
{
    uint64_t ts_sec;
    uint64_t ts_usec;
    uint64_t total_space;
};

class FS
{
public:
    enum Status {
        OK,
        BROKEN
    };

    static const char *status_str(Status status);

public:
    FS(Node & node, uint64_t fsid);

    Node & get_node()
    { return m_node; }

    const Node & get_node() const
    { return m_node; }

    uint64_t get_fsid() const
    { return m_fsid; }

    const std::string & get_key() const
    { return m_key; }

    const FSStat & get_stat() const
    { return m_stat; }

    void add_backend(Backend *backend);
    void remove_backend(Backend *backend);
    void get_backends(std::vector<Backend*> & backends) const;
    size_t get_backend_count() const;

    void update(const Backend & backend);
    void update_status();

    Status get_status() const
    { return m_status; }

    void set_status(Status status)
    { m_status = status; }

    bool match(const Filter & filter, uint32_t item_types = 0xFFFFFFFF) const;

    void print_info(std::ostream & ostr) const;
    void print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer) const;

private:
    Node & m_node;
    uint64_t m_fsid;

    std::string m_key;

    FSStat m_stat;

    std::set<Backend*> m_backends;
    mutable RWSpinLock m_backends_lock;

    Status m_status;
};

#endif

