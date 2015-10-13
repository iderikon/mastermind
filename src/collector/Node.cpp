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
    m_key = key(host, port, family);

    std::memset(&m_stat, 0, sizeof(m_stat));
    std::memset(&m_clock, 0, sizeof(m_clock));

    m_download_data.reserve(4096);
}

Node::Node(Storage & storage)
    :
    m_storage(storage),
    m_port(0),
    m_family(0)
{
    std::memset(&m_stat, 0, sizeof(m_stat));
    std::memset(&m_clock, 0, sizeof(m_clock));

    m_download_data.reserve(4096);
}

std::string Node::key(const char *host, int port, int family)
{
    std::ostringstream ostr;
    ostr << host << ':' << port << ':' << family;
    return ostr.str();
}

void Node::clone_from(const Node & other)
{
    m_host = other.m_host;
    m_port = other.m_port;
    m_family = other.m_family;
    m_key = other.m_key;
    m_download_data = other.m_download_data;

    bool have_newer;
    merge(other, have_newer);
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
                "Error parsing backend statistics for node %s", self->m_key.c_str());
        return;
    }
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

FS & Node::get_fs(uint64_t fsid)
{
    auto it = m_filesystems.lower_bound(fsid);

    if (it == m_filesystems.end() || it->first != fsid)
        it = m_filesystems.insert(it, std::make_pair(fsid, FS(*this, fsid)));

    return it->second;
}

void Node::handle_backend(const BackendStat & new_stat)
{
    // iterator will be used below as insertion hint
    auto it = m_backends.lower_bound(new_stat.backend_id);

    bool found = it != m_backends.end() && it->first == int(new_stat.backend_id);
    if (!found && !new_stat.state)
        return;

    uint64_t old_fsid = 0;
    if (found) {
        old_fsid = it->second.get_stat().fsid;
        it->second.update(new_stat);
    } else {
        it = m_backends.insert(it, std::make_pair(new_stat.backend_id, Backend(*this)));
        it->second.init(new_stat);
        m_new_backends.push_back(it->second);
    }

    Backend & backend = it->second;

    uint64_t new_fsid = backend.get_stat().fsid;
    FS & new_fs = get_fs(new_fsid);
    if (new_fsid != old_fsid) {
        if (found) {
            BH_LOG(m_storage.get_app().get_logger(), DNET_LOG_INFO,
                    "Updating backend %s: FS changed from %lu to %lu",
                    backend.get_key().c_str(), old_fsid, new_fsid);
        }

        if (old_fsid)
            get_fs(old_fsid).remove_backend(backend);
        backend.set_fs(new_fs);
        new_fs.add_backend(backend);
    }

    backend.recalculate(m_storage.get_app().get_config().reserved_space);
    new_fs.update(backend);
    backend.update_status();
}

void Node::update_filesystems()
{
    Stopwatch watch(m_clock.update_fs);

    for (auto it = m_filesystems.begin(); it != m_filesystems.end(); ++it)
        it->second.update_status();
}

void Node::merge_backends(const Node & other_node, bool & have_newer)
{
    auto my = m_backends.begin();
    auto other = other_node.m_backends.begin();

    while (other != other_node.m_backends.end()) {
        while (my != m_backends.end() && my->first < other->first)
            ++my;
        if (my != m_backends.end() && my->first == other->first) {
            Backend & my_backend = my->second;
            const Backend & other_backend = other->second;

            if (my_backend.get_stat().get_timestamp() < other_backend.get_stat().get_timestamp()) {
                uint64_t old_fsid = my_backend.get_stat().fsid;
                uint64_t new_fsid = other_backend.get_stat().fsid;

                if (old_fsid != new_fsid) {
                    BH_LOG(m_storage.get_app().get_logger(), DNET_LOG_INFO,
                            "Merging backend %s: FS changed from %lu to %lu",
                            my_backend.get_key().c_str(), old_fsid, new_fsid);

                    if (old_fsid)
                        get_fs(old_fsid).remove_backend(my_backend);
                    FS & new_fs = get_fs(new_fsid);
                    my_backend.set_fs(new_fs);
                    new_fs.add_backend(my_backend);
                }
            }
            my_backend.merge(other_backend, have_newer);
        } else {
            my = m_backends.insert(my, std::make_pair(other->first, Backend(*this)));
            Backend & my_backend = my->second;

            my_backend.clone_from(other->second);
            FS & fs = get_fs(my_backend.get_stat().fsid);
            fs.add_backend(my_backend);
            my_backend.set_fs(fs);
            m_new_backends.push_back(my_backend);
        }
        ++other;
    }

    if (m_backends.size() > other_node.m_backends.size())
        have_newer = true;
}

void Node::merge(const Node & other, bool & have_newer)
{
    uint64_t my_ts = m_stat.ts_sec * 1000000UL + m_stat.ts_usec;
    uint64_t other_ts = other.m_stat.ts_sec * 1000000UL + other.m_stat.ts_usec;
    if (my_ts < other_ts) {
        std::memcpy(&m_stat, &other.m_stat, sizeof(m_stat));
        std::memcpy(&m_clock, &other.m_clock, sizeof(m_clock));
    } else if (my_ts > other_ts) {
        have_newer = true;
    }

    merge_backends(other, have_newer);
    Storage::merge_map(*this, m_filesystems, other.m_filesystems, have_newer);
}

void Node::push_items(std::vector<std::reference_wrapper<Couple>> & couples)
{
    for (auto it = m_backends.begin(); it != m_backends.end(); ++it) {
        const Backend & backend = it->second;
        backend.push_items(couples);
    }
}

void Node::push_items(std::vector<std::reference_wrapper<Namespace>> & namespaces)
{
    for (auto it = m_backends.begin(); it != m_backends.end(); ++it) {
        const Backend & backend = it->second;
        backend.push_items(namespaces);
    }
}

void Node::push_items(std::vector<std::reference_wrapper<Backend>> & backends)
{
    for (auto it = m_backends.begin(); it != m_backends.end(); ++it)
        backends.push_back(it->second);
}

void Node::push_items(std::vector<std::reference_wrapper<Group>> & groups)
{
    for (auto it = m_backends.begin(); it != m_backends.end(); ++it)
        it->second.push_items(groups);
}

void Node::push_items(std::vector<std::reference_wrapper<FS>> & filesystems)
{
    for (auto it = m_filesystems.begin(); it != m_filesystems.end(); ++it)
        filesystems.push_back(it->second);
}

void Node::print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer,
        const std::vector<std::reference_wrapper<Backend>> & backends,
        const std::vector<std::reference_wrapper<FS>> & filesystems,
        bool print_backends,
        bool print_fs,
        bool show_internals) const
{
    writer.StartObject();

    writer.Key("timestamp");
    writer.StartObject();
    writer.Key("tv_sec");
    writer.Uint64(m_stat.ts_sec);
    writer.Key("tv_usec");
    writer.Uint64(m_stat.ts_usec);
    if (show_internals) {
        writer.Key("user_friendly");
        writer.String(timeval_user_friendly(m_stat.ts_sec, m_stat.ts_usec).c_str());
    }
    writer.EndObject();

    writer.Key("host");
    writer.String(m_host.c_str());
    writer.Key("port");
    writer.Uint64(m_port);
    writer.Key("family");
    writer.Uint64(m_family);
    writer.Key("dc");
    writer.String(m_dc.c_str());

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

    if (show_internals) {
        writer.Key("la");
        writer.Uint64(m_stat.la1);

        writer.Key("clock_stat");
        writer.StartObject();
        writer.Key("procfs_parse");
        writer.Uint64(m_clock.procfs_parse);
        writer.Key("backend_parse");
        writer.Uint64(m_clock.backend_parse);
        writer.Key("update_fs");
        writer.Uint64(m_clock.update_fs);
        writer.EndObject();
    }

    if (print_backends) {
        writer.Key("backends");
        writer.StartArray();
        for (auto it = m_backends.begin(); it != m_backends.end(); ++it) {
            struct {
                bool operator () (const Backend & b1, const Backend & b2) const
                { return &b1 < &b2; }
            } comp;
            if (backends.empty() || std::binary_search(backends.begin(),
                        backends.end(), it->second, comp))
                it->second.print_json(writer, show_internals);
        }
        writer.EndArray();
    }

    if (print_fs) {
        writer.Key("filesystems");
        writer.StartArray();
        for (auto it = m_filesystems.begin(); it != m_filesystems.end(); ++it) {
            struct {
                bool operator () (const FS & fs1, const FS & fs2) const
                { return &fs1 < &fs2; }
            } comp;
            if (filesystems.empty() || std::binary_search(filesystems.begin(),
                        filesystems.end(), it->second, comp))
                it->second.print_json(writer, show_internals);
        }
        writer.EndArray();
    }

    writer.EndObject();
}
