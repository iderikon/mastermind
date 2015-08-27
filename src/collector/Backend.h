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

#ifndef __399b96bc_e6ca_4613_afad_b47d434d2bea
#define __399b96bc_e6ca_4613_afad_b47d434d2bea

class Couple;
class Group;
class Filter;
class FS;
class Namespace;
class Node;

#include <cstdint>
#include <iostream>
#include <rapidjson/writer.h>
#include <vector>

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

    void clone_from(const Backend & other);

    Node & get_node()
    { return m_node; }
    const Node & get_node() const
    { return m_node; }

    void set_fs(FS *fs)
    { m_fs = fs; }

    FS *get_fs() const
    { return m_fs; }

    void set_group(Group *group)
    { m_group = group; }
    Group *get_group() const
    { return m_group; }

    // NB: get_items() may return duplicates
    void get_items(std::vector<Couple*> & couples) const;
    void get_items(std::vector<Namespace*> & namespaces) const;
    void get_items(std::vector<Node*> & nodes) const;
    void get_items(std::vector<Group*> & groups) const;
    void get_items(std::vector<FS*> & filesystems) const;

    const std::string & get_key() const
    { return m_key; }

    const BackendStat & get_stat() const
    { return m_stat; }

    void update(const BackendStat & stat);

    void recalculate(uint64_t reserved_space);

    uint64_t get_vfs_free_space() const
    { return m_calculated.vfs_free_space; }
    uint64_t get_vfs_total_space() const
    { return m_calculated.vfs_total_space; }
    uint64_t get_vfs_used_space() const
    { return m_calculated.vfs_used_space; }

    uint64_t get_free_space() const
    { return m_calculated.free_space; }
    uint64_t get_total_space() const
    { return m_calculated.total_space; }
    uint64_t get_used_space() const
    { return m_calculated.used_space; }
    uint64_t get_effective_space() const
    { return m_calculated.effective_space; }

    double get_fragmentation() const
    { return m_calculated.fragmentation; }

    int get_read_rps() const
    { return m_calculated.read_rps; }

    int get_write_rps() const
    { return m_calculated.write_rps; }

    bool full() const;

    Status get_status() const
    { return m_calculated.status; }

    void merge(const Backend & other, bool & have_newer);

    void print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer,
            bool show_internals) const;

    static const char *status_str(Status status);

private:
    Node & m_node;
    FS *m_fs;
    Group *m_group;

    std::string m_key;

    BackendStat m_stat;

    struct {
        uint64_t vfs_free_space;
        uint64_t vfs_total_space;
        uint64_t vfs_used_space;

        uint64_t records;

        int64_t free_space;
        int64_t total_space;
        int64_t used_space;
        int64_t effective_space;
        int64_t effective_free_space;

        double fragmentation;

        int read_rps;
        int write_rps;
        int max_read_rps;
        int max_write_rps;

        Status status;
    } m_calculated;

    bool m_read_only;
    bool m_disabled;
};

#endif

