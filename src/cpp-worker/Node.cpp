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

#include <rapidjson/reader.h>

#include "Storage.h"
#include "BackendParser.h"
#include "Filter.h"
#include "FS.h"
#include "Guard.h"
#include "Node.h"
#include "ProcfsParser.h"
#include "TimestampParser.h"
#include "WorkerApplication.h"

#include <cmath>

namespace {

class ProcfsJob : public ThreadPool::Job
{
public:
    ProcfsJob(Node *node)
        :
        m_node(node)
    {}

    void pick_data(std::vector<char> & data)
    { m_data.swap(data); }

    virtual void execute();

private:
    Node *m_node;
    std::vector<char> m_data;
};

class BackendJob : public ThreadPool::Job
{
public:
    BackendJob(Node *node)
        :
        m_node(node)
    {}

    void pick_data(std::vector<char> & data)
    { m_data.swap(data); }

    virtual void execute();

private:
    Node *m_node;
    std::vector<char> m_data;
};

void ProcfsJob::execute()
{
    ProcfsParser parser;

    rapidjson::Reader reader;
    rapidjson::StringStream ss(&m_data[0]);
    reader.Parse(ss, parser);

    if (!parser.good()) {
        BH_LOG(m_node->get_storage().get_app().get_logger(), DNET_LOG_ERROR,
                "Error parsing procfs statistics");
        return;
    }

    m_node->update(parser.get_stat());
}

void BackendJob::execute()
{
    TimestampParser ts_parser;

    {
        rapidjson::Reader reader;
        rapidjson::StringStream ss(&m_data[0]);
        reader.Parse(ss, ts_parser);
    }

    if (!ts_parser.good()) {
        BH_LOG(m_node->get_storage().get_app().get_logger(), DNET_LOG_ERROR,
                "Error parsing timestamp in backend statistics");
        return;
    }

    BackendParser parser(ts_parser.get_ts_sec(), ts_parser.get_ts_usec(), m_node);

    {
        rapidjson::Reader reader;
        rapidjson::StringStream ss(&m_data[0]);
        reader.Parse(ss, parser);
    }

    if (!parser.good()) {
        BH_LOG(m_node->get_storage().get_app().get_logger(), DNET_LOG_ERROR,
                "Error parsing backend statistics");
        return;
    }
}

} // unnamed namespace

NodeStat::NodeStat()
{
    std::memset(this, 0, sizeof(*this));
}

Node::Node(Storage & storage, const char *host, int port, int family)
    :
    m_storage(storage),
    m_host(host),
    m_port(port),
    m_family(family),
    m_download_state(DownloadStateEmpty)
{
    m_key = m_host;
    m_key += ':';
    m_key += std::to_string(m_port);
    m_key += ':';
    m_key += std::to_string(m_family);

    m_download_data.reserve(4096);
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
    WriteGuard<RWSpinLock> guard(m_backends_lock);

    auto it = m_backends.lower_bound(new_stat.backend_id);

    bool found = it != m_backends.end() && it->first == int(new_stat.backend_id);
    if (!found && !new_stat.state)
        return;

    if (found) {
        guard.release();
        it->second.update(new_stat);
    } else {
        it = m_backends.insert(it, std::make_pair(new_stat.backend_id, Backend(*this)));
        guard.release();
        it->second.init(new_stat);
    }

    m_storage.handle_backend(it->second, found);
}

ThreadPool::Job *Node::create_backend_parse_job()
{
    BackendJob *job = new BackendJob(this);
    job->pick_data(m_download_data);
    return job;
}

ThreadPool::Job *Node::create_procfs_parse_job()
{
    ProcfsJob *job = new ProcfsJob(this);
    job->pick_data(m_download_data);
    return job;
}

size_t Node::get_backend_count() const
{
    ReadGuard<RWSpinLock> guard(m_backends_lock);
    return m_backends.size();
}

void Node::get_backends(std::vector<Backend*> & backends)
{
    ReadGuard<RWSpinLock> guard(m_backends_lock);
    backends.reserve(m_backends.size());
    for (auto it = m_backends.begin(); it != m_backends.end(); ++it)
        backends.push_back(&it->second);
}

bool Node::get_backend(int id, Backend *& backend)
{
    ReadGuard<RWSpinLock> guard(m_backends_lock);

    auto it = m_backends.find(id);
    if (it != m_backends.end()) {
        backend = &it->second;
        return true;
    }
    return false;
}

void Node::update_filesystems()
{
    std::vector<FS*> filesystems;
    get_filesystems(filesystems);

    for (FS *fs : filesystems)
        fs->update_status();
}

FS *Node::get_fs(uint64_t fsid)
{
    {
        ReadGuard<RWSpinLock> guard(m_filesystems_lock);
        auto it = m_filesystems.find(fsid);
        if (it != m_filesystems.end())
            return &it->second;
    }

    {
        WriteGuard<RWSpinLock> guard(m_filesystems_lock);
        auto it = m_filesystems.lower_bound(fsid);
        if (it == m_filesystems.end() || it->first != fsid)
            it = m_filesystems.insert(it, std::make_pair(fsid, FS(*this, fsid)));
        return &it->second;
    }
}

bool Node::get_fs(uint64_t fsid, FS *& fs)
{
    ReadGuard<RWSpinLock> guard(m_filesystems_lock);

    auto it = m_filesystems.find(fsid);
    if (it != m_filesystems.end()) {
        fs = &it->second;
        return true;
    }
    return false;
}

void Node::get_filesystems(std::vector<FS*> & filesystems)
{
    ReadGuard<RWSpinLock> guard(m_filesystems_lock);

    filesystems.reserve(m_filesystems.size());
    for (auto it = m_filesystems.begin(); it != m_filesystems.end(); ++it)
        filesystems.push_back(&it->second);
}

bool Node::match(const Filter & filter, uint32_t item_types) const
{
    bool check_nodes = (item_types & Filter::Node) && !filter.nodes.empty();
    if (check_nodes) {
        if (!std::binary_search(filter.nodes.begin(), filter.nodes.end(), m_key))
            return false;
    }

    bool check_backends = (item_types & Filter::Backend) && !filter.backends.empty();
    bool check_fs = (item_types & Filter::FS) && !filter.filesystems.empty();
    bool check_groups = (item_types & Filter::Group) && !filter.groups.empty();
    bool check_couples = (item_types & Filter::Couple) && !filter.couples.empty();
    bool check_namespaces = (item_types & Filter::Namespace) && !filter.namespaces.empty();

    if (!check_backends && !check_fs && !check_groups && !check_couples && !check_namespaces)
        return true;

    bool found_backend = false;
    bool found_fs = false;
    bool found_group = false;
    bool found_couple = false;
    bool found_namespace = false;

    ReadGuard<RWSpinLock> guard(m_backends_lock);

    for (auto it = m_backends.begin(); it != m_backends.end(); ++it) {
        const Backend & backend = it->second;

        if (check_backends && !found_backend) {
            if (std::binary_search(filter.backends.begin(), filter.backends.end(),
                        backend.get_key()))
                found_backend = true;
        }
        if (check_fs && !found_fs) {
            if (backend.get_fs() != NULL && std::binary_search(filter.filesystems.begin(),
                        filter.filesystems.end(), backend.get_fs()->get_key()))
                found_fs = true;
        }
        if (check_groups && !found_group) {
            if (std::binary_search(filter.groups.begin(), filter.groups.end(),
                        backend.get_stat().group))
                found_group = true;
        }
        if (check_couples && !found_couple) {
            if (backend.get_group() != NULL && backend.get_group()->get_couple() != NULL &&
                    std::binary_search(filter.couples.begin(), filter.couples.end(),
                        backend.get_group()->get_couple()->get_key()))
                found_couple = true;
        }
        if (check_namespaces && !found_namespace) {
            if (backend.get_group() != NULL && backend.get_group()->get_namespace() != NULL &&
                    std::binary_search(filter.namespaces.begin(), filter.namespaces.end(),
                        backend.get_group()->get_namespace()->get_name()))
                found_namespace = true;
        }

        if (check_backends == found_backend && check_fs == found_fs && check_groups == found_group &&
                check_couples == found_couple && check_namespaces == found_namespace)
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
            "  download_state: " << download_state_str(m_download_state) << "\n"
            "  Stat {\n"
            "    ts: " << TimestampParser::ts_user_friendly(m_stat.ts_sec, m_stat.ts_usec) << "\n"
            "    la: " << m_stat.la1 << "\n"
            "    tx_bytes: " << m_stat.tx_bytes << "\n"
            "    rx_bytes: " << m_stat.rx_bytes << "\n"
            "    load_average: " << m_stat.load_average << "\n"
            "    tx_rate: " << m_stat.tx_rate << "\n"
            "    rx_rate: " << m_stat.rx_rate << "\n"
            "  }\n"
            "  number of backends: " << get_backend_count() << "\n"
            "}";
}

void Node::print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer) const
{
    writer.StartObject();

    writer.Key("timestamp");
    writer.StartObject();
    writer.Key("tv_sec");
    writer.Uint64(m_stat.ts_sec);
    writer.Key("tv_usec");
    writer.Uint64(m_stat.ts_usec);
    writer.EndObject();

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

    writer.Key("filesystems");
    writer.StartArray();
    {
        ReadGuard<RWSpinLock> guard(m_filesystems_lock);
        for (auto it = m_filesystems.begin(); it != m_filesystems.end(); ++it)
            it->second.print_json(writer);
    }
    writer.EndArray();

    writer.Key("backends");
    writer.StartArray();
    {
        ReadGuard<RWSpinLock> guard(m_backends_lock);
        for (auto it = m_backends.begin(); it != m_backends.end(); ++it)
            it->second.print_json(writer);
    }
    writer.EndArray();

    writer.EndObject();
}

const char *Node::download_state_str(DownloadState state)
{
    switch (state)
    {
    case DownloadStateEmpty:
        return "DownloadStateEmpty";
    case DownloadStateBackend:
        return "DownloadStateBackend";
    case DownloadStateProcfs:
        return "DownloadStateProcfs";
    }
    return "UNKNOWN";
}
