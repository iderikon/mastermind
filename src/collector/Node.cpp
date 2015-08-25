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

#include <elliptics/logger.hpp>
#include <rapidjson/reader.h>

#include "Storage.h"
#include "BackendParser.h"
#include "Filter.h"
#include "FS.h"
#include "Metrics.h"
#include "Node.h"
#include "ProcfsParser.h"
#include "WorkerApplication.h"

#include <cmath>

NodeStat::NodeStat()
{
    std::memset(this, 0, sizeof(*this));
}

Node::Node(Storage & storage, const char *host, int port, int family)
    :
    m_storage(storage),
    m_host(host),
    m_port(port),
    m_family(family)
{
    m_key = m_host;
    m_key += ':';
    m_key += std::to_string(m_port);
    m_key += ':';
    m_key += std::to_string(m_family);

    m_download_data.reserve(4096);

    std::memset(&m_stat, 0, sizeof(m_stat));
    std::memset(&m_clock, 0, sizeof(m_clock));
}

Node::Node(Storage & storage)
    :
    m_storage(storage),
    m_port(0),
    m_family(0)
{
    std::memset(&m_stat, 0, sizeof(m_stat));
    std::memset(&m_clock, 0, sizeof(m_clock));
}

void Node::clone_from(const Node & other)
{
    m_host = other.m_host;
    m_port = other.m_port;
    m_family = other.m_family;
    m_key = other.m_key;
    m_download_data = other.m_download_data;

    merge(other);
}

void Node::update(const NodeStat & stat)
{
    double ts1 = double(m_stat.ts_sec) + double(m_stat.ts_usec) / 1000000.0;
    double ts2 = double(stat.ts_sec) + double(stat.ts_usec) / 1000000.0;
    double d_ts = ts2 - ts1;

    if (d_ts > 1.0) {
        if (m_stat.tx_bytes < stat.tx_bytes)
            m_stat.tx_rate = double(stat.tx_bytes - m_stat.tx_bytes) / d_ts;
        if (m_stat.rx_bytes < stat.rx_bytes)
            m_stat.rx_rate = double(stat.rx_bytes - m_stat.rx_bytes) / d_ts;
    }

    m_stat.load_average = double(stat.la1) / 100.0;

    m_stat.ts_sec = stat.ts_sec;
    m_stat.ts_usec = stat.ts_usec;
    m_stat.la1 = stat.la1;
    m_stat.tx_bytes = stat.tx_bytes;
    m_stat.rx_bytes = stat.rx_bytes;
}

void Node::handle_backend(const BackendStat & new_stat)
{
    auto it = m_backends.lower_bound(new_stat.backend_id);

    bool found = it != m_backends.end() && it->first == int(new_stat.backend_id);
    if (!found && !new_stat.state)
        return;

    if (found) {
        it->second.update(new_stat);
    } else {
        it = m_backends.insert(it, std::make_pair(new_stat.backend_id, Backend(*this)));
        it->second.init(new_stat);
    }

    m_new_backends.push_back(&it->second);
}

void Node::parse_stats(void *arg)
{
    Node *self = (Node *) arg;

    Stopwatch procfs_watch(self->m_clock.procfs_parse);

    ProcfsParser procfs_parser;
    {
        rapidjson::Reader reader;
        rapidjson::StringStream ss(self->m_download_data.c_str());
        reader.Parse(ss, procfs_parser);
    }

    if (!procfs_parser.good()) {
        BH_LOG(self->m_storage.get_app().get_logger(), DNET_LOG_ERROR,
                "Error parsing procfs statistics");
        self->m_download_data.clear();
        return;
    }

    const NodeStat & node_stat = procfs_parser.get_stat();
    self->update(node_stat);

    procfs_watch.stop();

    Stopwatch backend_watch(self->m_clock.backend_parse);

    BackendParser backend_parser(node_stat.ts_sec, node_stat.ts_usec,
            std::bind(&Node::handle_backend, self, std::placeholders::_1));

    {
        rapidjson::Reader reader;
        rapidjson::StringStream ss(self->m_download_data.c_str());
        reader.Parse(ss, backend_parser);
    }

    self->m_download_data.clear();

    if (!backend_parser.good()) {
        BH_LOG(self->m_storage.get_app().get_logger(), DNET_LOG_ERROR,
                "Error parsing backend statistics");
        return;
    }
}

bool Node::get_backend(int id, Backend *& backend)
{
    auto it = m_backends.find(id);
    if (it != m_backends.end()) {
        backend = &it->second;
        return true;
    }
    return false;
}

void Node::get_items(std::vector<Couple*> & couples)
{
    for (auto it = m_backends.begin(); it != m_backends.end(); ++it) {
        const Backend & backend = it->second;
        backend.get_items(couples);
    }
}

void Node::get_items(std::vector<Namespace*> & namespaces)
{
    for (auto it = m_backends.begin(); it != m_backends.end(); ++it) {
        const Backend & backend = it->second;
        backend.get_items(namespaces);
    }
}

void Node::get_items(std::vector<Backend*> & backends)
{
    for (auto it = m_backends.begin(); it != m_backends.end(); ++it)
        backends.push_back(&it->second);
}

void Node::get_items(std::vector<Group*> & groups)
{
    for (auto it = m_backends.begin(); it != m_backends.end(); ++it) {
        Group *group = it->second.get_group();
        if (group != nullptr)
            groups.push_back(group);
    }
}

void Node::get_items(std::vector<FS*> & filesystems)
{
    for (auto it = m_filesystems.begin(); it != m_filesystems.end(); ++it)
        filesystems.push_back(&it->second);
}

void Node::pick_new_backends(std::vector<Backend*> & backends)
{
    backends.clear();
    m_new_backends.swap(backends);
}

void Node::update_filesystems()
{
    Stopwatch watch(m_clock.update_fs);

    for (auto it = m_filesystems.begin(); it != m_filesystems.end(); ++it)
        it->second.update_status();
}

void Node::merge(const Node & other)
{
    uint64_t my_ts = m_stat.ts_sec * 1000000 + m_stat.ts_usec;
    uint64_t other_ts = other.m_stat.ts_sec * 1000000 + other.m_stat.ts_usec;
    if (my_ts < other_ts) {
        std::memcpy(&m_stat, &other.m_stat, sizeof(m_stat));
        std::memcpy(&m_clock, &other.m_clock, sizeof(m_clock));
    }

    Storage::merge_map(*this, m_backends, other.m_backends);
    Storage::merge_map(*this, m_filesystems, other.m_filesystems);
}

FS *Node::get_fs(uint64_t fsid)
{
    auto it = m_filesystems.lower_bound(fsid);

    if (it == m_filesystems.end() || it->first != fsid)
        it = m_filesystems.insert(it, std::make_pair(fsid, FS(*this, fsid)));

    return &it->second;
}

bool Node::get_fs(uint64_t fsid, FS *& fs)
{
    auto it = m_filesystems.find(fsid);
    if (it != m_filesystems.end()) {
        fs = &it->second;
        return true;
    }
    return false;
}

void Node::print_info(std::ostream & ostr) const
{
    ostr << "Node {\n"
            "  host: " << m_host << "\n"
            "  port: " << m_port << "\n"
            "  family: " << m_family << "\n"
            "  Stat {\n"
            "    ts: " << timeval_user_friendly(m_stat.ts_sec, m_stat.ts_usec) << "\n"
            "    la: " << m_stat.la1 << "\n"
            "    tx_bytes: " << m_stat.tx_bytes << "\n"
            "    rx_bytes: " << m_stat.rx_bytes << "\n"
            "    load_average: " << m_stat.load_average << "\n"
            "    tx_rate: " << m_stat.tx_rate << "\n"
            "    rx_rate: " << m_stat.rx_rate << "\n"
            "  }\n"
            "  number of backends: " << m_backends.size() << "\n"
            "}";
}

void Node::print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer,
        const std::vector<Backend*> & backends,
        const std::vector<FS*> & filesystems,
        bool print_backends,
        bool print_fs) const
{
    writer.StartObject();

    writer.Key("timestamp");
    writer.StartObject();
    writer.Key("tv_sec");
    writer.Uint64(m_stat.ts_sec);
    writer.Key("tv_usec");
    writer.Uint64(m_stat.ts_usec);
    writer.EndObject();

    writer.Key("host");
    writer.String(m_host.c_str());
    writer.Key("port");
    writer.Uint64(m_port);
    writer.Key("family");
    writer.Uint64(m_family);

    writer.Key("tx_bytes");
    writer.Uint64(m_stat.tx_bytes);
    writer.Key("rx_bytes");
    writer.Uint64(m_stat.rx_bytes);
    writer.Key("load_average");
    writer.Double(m_stat.load_average);
    writer.Key("tx_rate");
    writer.Double(m_stat.tx_rate);
    writer.Key("rx_rate");
    writer.Double(m_stat.rx_rate);

    if (print_backends) {
        writer.Key("backends");
        writer.StartArray();
        for (auto it = m_backends.begin(); it != m_backends.end(); ++it) {
            if (backends.empty() || std::binary_search(backends.begin(),
                        backends.end(), &it->second))
                it->second.print_json(writer);
        }
        writer.EndArray();
    }

    if (print_fs) {
        writer.Key("filesystems");
        writer.StartArray();
        for (auto it = m_filesystems.begin(); it != m_filesystems.end(); ++it) {
            if (filesystems.empty() || std::binary_search(filesystems.begin(),
                        filesystems.end(), &it->second))
                it->second.print_json(writer);
        }
        writer.EndArray();
    }

    writer.EndObject();
}
