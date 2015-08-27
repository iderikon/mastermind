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

#ifndef __8424595a_92d5_49dd_ad9d_798dd37ce961
#define __8424595a_92d5_49dd_ad9d_798dd37ce961

#include "Backend.h"

#include <functional>
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
    Node(Storage & storage, const char *host, int port, int family);
    Node(Storage & storage);

    void clone_from(const Node & other);

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

    void add_download_data(const char *data, size_t size)
    { m_download_data.append(data, size); }

    void drop_download_data()
    { m_download_data.clear(); }

    static void parse_stats(void *arg);

    std::map<int, Backend> & get_backends()
    { return m_backends; }
    bool get_backend(int id, Backend *& backend);

    // NB: get_items() may return duplicates
    void get_items(std::vector<std::reference_wrapper<Couple>> & couples);
    void get_items(std::vector<std::reference_wrapper<Namespace>> & namespaces);
    void get_items(std::vector<std::reference_wrapper<Backend>> & backends);
    void get_items(std::vector<std::reference_wrapper<Group>> & groups);
    void get_items(std::vector<std::reference_wrapper<FS>> & filesystems);

    std::vector<std::reference_wrapper<Backend>> pick_new_backends()
    { return std::move(m_new_backends); }

    void update_filesystems();

    void merge(const Node & other, bool & have_newer);

    bool get_fs(uint64_t fsid, FS *& fs);

    std::map<uint64_t, FS> & get_filesystems()
    { return m_filesystems; }

    void print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer,
            const std::vector<std::reference_wrapper<Backend>> & backends,
            const std::vector<std::reference_wrapper<FS>> & filesystems,
            bool print_backends,
            bool print_fs,
            bool show_internals) const;

private:
    void check_fs_change(Backend & backend, uint64_t new_fsid);

    FS & get_fs(uint64_t fsid);

    void merge_backends(const Node & other_node, bool & have_newer);

public:
    struct ClockStat
    {
        uint64_t procfs_parse;
        uint64_t backend_parse;
        uint64_t update_fs;
    };

    const ClockStat & get_clock_stat() const
    { return m_clock; }

private:
    Storage & m_storage;

    std::string m_host;
    int m_port;
    int m_family;

    std::string m_key;

    std::string m_download_data;

    NodeStat m_stat;

    std::map<int, Backend> m_backends;
    std::map<uint64_t, FS> m_filesystems;

    std::vector<std::reference_wrapper<Backend>> m_new_backends;

    ClockStat m_clock;
};

#endif

