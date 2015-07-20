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

#ifndef __8424595a_92d5_49dd_ad9d_798dd37ce961
#define __8424595a_92d5_49dd_ad9d_798dd37ce961

#include "RWSpinLock.h"
#include "ThreadPool.h"

#include <iostream>
#include <map>
#include <string>
#include <vector>

class FS;
class Node;
class Storage;

struct BackendStat
{
    BackendStat();

    enum Status
    {
        INIT = 0,
        OK,
        RO,
        BAD,
        STALLED,
        BROKEN
    };

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


    uint64_t vfs_free_space;
    uint64_t vfs_total_space;
    uint64_t vfs_used_space;

    uint64_t records;

    uint64_t free_space;
    uint64_t total_space;
    uint64_t used_space;

    double fragmentation;

    int read_rps;
    int write_rps;
    int max_read_rps;
    int max_write_rps;

    FS *fs;
    Node *node;

    Status status;
    int disabled;
    int read_only;

    static const char *status_str(Status status);
};

std::ostream & operator << (std::ostream & ostr, const BackendStat & stat);

struct NodeStat
{
    NodeStat();

    uint64_t ts_sec;
    uint64_t ts_usec;
    uint64_t la1;
    uint64_t tx_bytes;
    uint64_t rx_bytes;

    double load_average;
    double tx_rate;
    double rx_rate;
};

class Node
{
public:
    enum DownloadState {
        DownloadStateEmpty = 0,
        DownloadStateBackend,
        DownloadStateProcfs
    };

    Node(Storage & storage, const char *host, int port, int family);

    Storage & get_storage() const
    { return m_storage; }

    const std::string & get_host() const
    { return m_host; }

    int get_port() const
    { return m_port; }

    int get_family() const
    { return m_family; }

    const NodeStat & get_stat() const
    { return m_stat; }

    void update(const NodeStat & stat);

    void handle_backend(const BackendStat & new_stat);

    void set_download_state(DownloadState state);

    DownloadState get_download_state() const
    { return m_download_state; }

    void add_download_data(const char *data, size_t size)
    { m_download_data.insert(m_download_data.end(), data, data + size); }

    ThreadPool::Job *create_backend_parse_job();

    ThreadPool::Job *create_procfs_parse_job();

    size_t get_backend_count() const;

    void print_info(std::ostream & ostr) const;

    static const char *download_state_str(DownloadState state);

private:
    Storage & m_storage;

    std::string m_host;
    int m_port;
    int m_family;

    DownloadState m_download_state;
    std::vector<char> m_download_data;

    NodeStat m_stat;

    std::map<int, BackendStat> m_backends;
    mutable RWSpinLock m_backends_lock;
};

#endif

