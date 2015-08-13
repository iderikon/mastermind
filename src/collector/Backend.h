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

#ifndef __399b96bc_e6ca_4613_afad_b47d434d2bea
#define __399b96bc_e6ca_4613_afad_b47d434d2bea

class Group;
class Filter;
class FS;
class Node;

#include <cstdint>
#include <iostream>
#include <rapidjson/writer.h>

struct BackendStat
{
    BackendStat();

    uint64_t ts_sec;
    uint64_t ts_usec;
    uint64_t backend_id;
    uint64_t state;

    uint64_t vfs_blocks;
    uint64_t vfs_bavail;
    uint64_t vfs_bsize;

    uint64_t records_total;
    uint64_t records_removed;
    uint64_t records_removed_size;
    uint64_t base_size;

    uint64_t fsid;
    uint64_t defrag_state;
    uint64_t want_defrag;

    uint64_t read_ios;
    uint64_t write_ios;
    uint64_t error;

    uint64_t blob_size_limit;
    uint64_t max_blob_base_size;
    uint64_t blob_size;
    uint64_t group;
};

class Backend
{
public:
    enum Status
    {
        INIT = 0,
        OK,
        RO,
        BAD,
        STALLED,
        BROKEN
    };

    Backend(Node & node);

    void init(const BackendStat & stat);

    Node & get_node()
    { return m_node; }
    const Node & get_node() const
    { return m_node; }

    FS *get_fs() const
    { return m_fs; }

    void set_group(Group *group)
    { m_group = group; }
    Group *get_group() const
    { return m_group; }

    const std::string & get_key() const
    { return m_key; }

    const BackendStat & get_stat() const
    { return m_stat; }

    void update(const BackendStat & stat);

    void recalculate();

    uint64_t get_vfs_free_space() const
    { return m_vfs_free_space; }
    uint64_t get_vfs_total_space() const
    { return m_vfs_total_space; }
    uint64_t get_vfs_used_space() const
    { return m_vfs_used_space; }

    uint64_t get_free_space() const
    { return m_free_space; }
    uint64_t get_total_space() const
    { return m_total_space; }
    uint64_t get_used_space() const
    { return m_used_space; }
    uint64_t get_effective_space() const
    { return m_effective_space; }

    double get_fragmentation() const
    { return m_fragmentation; }

    int get_read_rps() const
    { return m_read_rps; }

    int get_write_rps() const
    { return m_write_rps; }

    bool full() const;

    Status get_status() const
    { return m_status; }

    bool match(const Filter & filter, uint32_t item_types = 0xFFFFFFFF) const;

    void print_info(std::ostream & ostr) const;
    void print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer) const;

    static const char *status_str(Status status);

private:
    Node & m_node;
    FS *m_fs;
    Group *m_group;

    std::string m_key;

    BackendStat m_stat;

    uint64_t m_vfs_free_space;
    uint64_t m_vfs_total_space;
    uint64_t m_vfs_used_space;

    uint64_t m_records;

    int64_t m_free_space;
    int64_t m_total_space;
    int64_t m_used_space;
    int64_t m_effective_space;
    int64_t m_effective_free_space;

    double m_fragmentation;

    int m_read_rps;
    int m_write_rps;
    int m_max_read_rps;
    int m_max_write_rps;

    Status m_status;
    bool m_read_only;
    bool m_disabled;
};

#endif

