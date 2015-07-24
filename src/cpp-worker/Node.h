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

#include "Backend.h"
#include "RWSpinLock.h"
#include "ThreadPool.h"

#include <iostream>
#include <map>
#include <rapidjson/writer.h>
#include <string>
#include <vector>

class Filter;
class FS;
class Node;
class Storage;

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

    const std::string & get_key() const
    { return m_key; }

    const NodeStat & get_stat() const
    { return m_stat; }

    void update(const NodeStat & stat);

    void handle_backend(const BackendStat & new_stat);

    void set_download_state(DownloadState state)
    { m_download_state = state; }

    DownloadState get_download_state() const
    { return m_download_state; }

    void add_download_data(const char *data, size_t size)
    { m_download_data.insert(m_download_data.end(), data, data + size); }

    void drop_download_data()
    { m_download_data.clear(); }

    ThreadPool::Job *create_backend_parse_job();

    ThreadPool::Job *create_procfs_parse_job();

    size_t get_backend_count() const;
    void get_backends(std::vector<Backend*> & backends);
    bool get_backend(int id, Backend *& backend);

    void update_filesystems();

    FS *get_fs(uint64_t fsid);
    bool get_fs(uint64_t fsid, FS *& fs);
    void get_filesystems(std::vector<FS*> & filesystems);

    bool match(const Filter & filter, uint32_t item_types = 0xFFFFFFFF) const;

    void print_info(std::ostream & ostr) const;
    void print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer) const;

    static const char *download_state_str(DownloadState state);

private:
    Storage & m_storage;

    std::string m_host;
    int m_port;
    int m_family;

    std::string m_key;

    DownloadState m_download_state;
    std::vector<char> m_download_data;

    NodeStat m_stat;

    std::map<int, Backend> m_backends;
    mutable RWSpinLock m_backends_lock;

    std::map<uint64_t, FS> m_filesystems;
    mutable RWSpinLock m_filesystems_lock;
};

#endif

