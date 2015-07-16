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

BackendStat::BackendStat()
{
    std::memset(this, 0, sizeof(*this));
}

std::ostream & operator << (std::ostream & ostr, const BackendStat & stat)
{
    ostr << "BackendStat {"
         << "\n    ts_sec: " << stat.ts_sec
         << "\n    ts_usec: " << stat.ts_usec
         << "\n    backend_id: " << stat.backend_id
         << "\n    state: " << stat.state
         << "\n    vfs_blocks: " << stat.vfs_blocks
         << "\n    vfs_bavail: " << stat.vfs_bavail
         << "\n    vfs_bsize: " << stat.vfs_bsize
         << "\n    records_total: " << stat.records_total
         << "\n    records_removed: " << stat.records_removed
         << "\n    records_removed_size: " << stat.records_removed_size
         << "\n    base_size: " << stat.base_size
         << "\n    fsid: " << stat.fsid
         << "\n    defrag_state: " << stat.defrag_state
         << "\n    want_defrag: " << stat.want_defrag
         << "\n    read_ios: " << stat.read_ios
         << "\n    write_ios: " << stat.write_ios
         << "\n    error: " << stat.error
         << "\n    blob_size_limit: " << stat.blob_size_limit
         << "\n    max_blob_base_size: " << stat.max_blob_base_size
         << "\n    blob_size: " << stat.blob_size
         << "\n    group: " << stat.group
         << "\n    vfs_free_space: " << stat.vfs_free_space
         << "\n    vfs_total_space: " << stat.vfs_total_space
         << "\n    vfs_used_space: " << stat.vfs_used_space
         << "\n    records: " << stat.records
         << "\n    free_space: " << stat.free_space
         << "\n    total_space: " << stat.total_space
         << "\n    used_space: " << stat.used_space
         << "\n    fragmentation: " << stat.fragmentation
         << "\n    read_rps: " << stat.read_rps
         << "\n    write_rps: " << stat.write_rps
         << "\n    max_read_rps: " << stat.max_read_rps
         << "\n    max_write_rps: " << stat.max_write_rps
         << "\n}";
    return ostr;
}

Node::Node(Storage & storage, const char *host, int port, int family)
    :
    m_storage(storage),
    m_host(host),
    m_port(port),
    m_family(family),
    m_download_state(DownloadStateEmpty)
{}

void Node::set_download_state(DownloadState state)
{
    m_download_state = state;
    m_download_data.reserve(4096);
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
        BackendStat & existing = it->second;
        guard.release();

        double ts1 = double(existing.ts_sec) + double(existing.ts_usec) / 1000000.0;
        double ts2 = double(new_stat.ts_sec) + double(new_stat.ts_usec) / 1000000.0;
        double d_ts = ts2 - ts1;

        if (d_ts > 1.0) {
            existing.read_rps = int(double(new_stat.read_ios - existing.read_ios) / d_ts);
            existing.write_rps = int(double(new_stat.write_ios - existing.write_ios) / d_ts);

            // XXX RPS_FORMULA
            existing.max_read_rps = int(std::max(double(existing.read_rps) /
                        std::max(m_stat.load_average, 0.01), 100.0));
            existing.max_write_rps = int(std::max(double(existing.write_rps) /
                        std::max(m_stat.load_average, 0.01), 100.0));
        }
    } else {
        it = m_backends.insert(it, std::make_pair(new_stat.backend_id, new_stat));
        guard.release();
    }

    BackendStat & stat = it->second;

    stat.ts_sec = new_stat.ts_sec;
    stat.ts_usec = new_stat.ts_usec;
    stat.backend_id = new_stat.backend_id;
    stat.state = new_stat.state;

    stat.vfs_blocks = new_stat.vfs_blocks;
    stat.vfs_bavail = new_stat.vfs_bavail;
    stat.vfs_bsize = new_stat.vfs_bsize;

    stat.records_total = new_stat.records_total;
    stat.records_removed = new_stat.records_removed;
    stat.records_removed_size = new_stat.records_removed_size;

    stat.fsid = new_stat.fsid;
    stat.defrag_state = new_stat.defrag_state;
    stat.want_defrag = new_stat.want_defrag;

    stat.read_ios = new_stat.read_ios;
    stat.write_ios = new_stat.write_ios;
    stat.error = new_stat.error;

    stat.blob_size_limit = new_stat.blob_size_limit;
    stat.max_blob_base_size = new_stat.max_blob_base_size;
    stat.blob_size = new_stat.blob_size;
    stat.group = new_stat.group;

    stat.vfs_total_space = stat.vfs_blocks * stat.vfs_bsize;
    stat.vfs_free_space = stat.vfs_bavail * stat.vfs_bsize;
    stat.vfs_used_space = stat.vfs_total_space - stat.vfs_free_space;

    stat.records = stat.records_total - stat.records_removed;
    stat.fragmentation = double(stat.records_removed) /
        double(std::max(stat.records_total, 1UL));

    if (stat.blob_size_limit) {
        // vfs_total_space can be less than blob_size_limit in case of misconfiguration
        stat.total_space = std::min(stat.blob_size_limit, stat.vfs_total_space);
        stat.used_space = stat.base_size;
        stat.free_space = std::min(
                uint64_t(std::max(0L, int64_t(stat.total_space) - int64_t(stat.used_space))),
                stat.vfs_free_space);
    } else {
        stat.total_space = stat.vfs_total_space;
        stat.free_space = stat.vfs_free_space;
        stat.used_space = stat.vfs_used_space;
    }

    if (stat.error || stat.disabled)
        stat.status = BackendStat::STALLED;
#if 0
    else if (stat.fs->status == FS::BROKEN)
        stat.status = BackendStat::BROKEN;
#endif
    else if (stat.read_only)
        stat.status = BackendStat::RO;
    else
        stat.status = BackendStat::OK;

    m_storage.handle_backend(stat);
}

size_t Node::get_backend_count() const
{
    ReadGuard<RWSpinLock> guard(m_backends_lock);
    return m_backends.size();
}
