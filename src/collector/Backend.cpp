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

#include "Backend.h"
#include "Filter.h"
#include "FS.h"
#include "Metrics.h"
#include "Node.h"
#include "WorkerApplication.h"

#include "Storage.h"

BackendStat::BackendStat()
{
    std::memset(this, 0, sizeof(*this));
}

Backend::Backend(Node & node)
    :
    m_node(node),
    m_fs(nullptr),
    m_group(nullptr),
    m_read_only(false)
{
    std::memset(&m_calculated, 0, sizeof(m_calculated));
}

void Backend::init(const BackendStat & stat)
{
    memcpy(&m_stat, &stat, sizeof(m_stat));
    m_key = m_node.get_key() + '/' + std::to_string(stat.backend_id);
}

void Backend::clone_from(const Backend & other)
{
    m_key = other.m_key;

    std::memcpy(&m_stat, &other.m_stat, sizeof(m_stat));
    std::memcpy(&m_calculated, &other.m_calculated, sizeof(m_calculated));

    m_read_only = other.m_read_only;
}

bool Backend::full() const
{
    if (m_calculated.used_space >= m_calculated.effective_space)
        return true;
    if (m_calculated.effective_free_space <= 0)
        return true;
    return false;
}

void Backend::update(const BackendStat & stat)
{
    double ts1 = double(m_stat.get_timestamp()) / 1000000.0;
    double ts2 = double(stat.get_timestamp()) / 1000000.0;
    double d_ts = ts2 - ts1;

    if (d_ts > 1.0) {
        m_calculated.read_rps = int(double(stat.read_ios - m_stat.read_ios) / d_ts);
        m_calculated.write_rps = int(double(stat.write_ios - m_stat.write_ios) / d_ts);

        // XXX RPS_FORMULA
        m_calculated.max_read_rps = int(std::max(double(m_calculated.read_rps) /
                    std::max(m_node.get_stat().load_average, 0.01), 100.0));
        m_calculated.max_write_rps = int(std::max(double(m_calculated.write_rps) /
                    std::max(m_node.get_stat().load_average, 0.01), 100.0));
    }

    std::memcpy(&m_stat, &stat, sizeof(m_stat));
}

void Backend::set_fs(FS & fs)
{
    m_fs = &fs;
}

void Backend::recalculate(uint64_t reserved_space)
{
    m_calculated.vfs_total_space = m_stat.vfs_blocks * m_stat.vfs_bsize;
    m_calculated.vfs_free_space = m_stat.vfs_bavail * m_stat.vfs_bsize;
    m_calculated.vfs_used_space = m_calculated.vfs_total_space - m_calculated.vfs_free_space;

    m_calculated.records = m_stat.records_total - m_stat.records_removed;
    m_calculated.fragmentation = double(m_stat.records_removed) / double(std::max(m_stat.records_total, 1UL));

    if (m_stat.blob_size_limit) {
        // vfs_total_space can be less than blob_size_limit in case of misconfiguration
        m_calculated.total_space = std::min(m_stat.blob_size_limit, m_calculated.vfs_total_space);
        m_calculated.used_space = m_stat.base_size;
        m_calculated.free_space = std::min(int64_t(m_calculated.vfs_free_space),
                std::max(0L, m_calculated.total_space - m_calculated.used_space));
    } else {
        m_calculated.total_space = m_calculated.vfs_total_space;
        m_calculated.free_space = m_calculated.vfs_free_space;
        m_calculated.used_space = m_calculated.vfs_used_space;
    }

    double share = double(m_calculated.total_space) / double(m_calculated.vfs_total_space);
    int64_t free_space_req_share = std::ceil(double(reserved_space) * share);
    m_calculated.effective_space = std::max(0L, m_calculated.total_space - free_space_req_share);

    m_calculated.effective_free_space =
        std::max(m_calculated.free_space - (m_calculated.total_space - m_calculated.effective_space), 0L);
}

void Backend::update_status()
{
    if (m_stat.error || !m_stat.state || m_fs == nullptr)
        m_calculated.status = STALLED;
    else if (m_fs->get_status() == FS::BROKEN)
        m_calculated.status = BROKEN;
    else if (m_read_only)
        m_calculated.status = RO;
    else
        m_calculated.status = OK;
}

void Backend::set_group(Group & group)
{
    m_group = &group;
}

void Backend::merge(const Backend & other, bool & have_newer)
{
    uint64_t my_ts = m_stat.get_timestamp();
    uint64_t other_ts = other.m_stat.get_timestamp();
    if (my_ts < other_ts) {
        std::memcpy(&m_stat, &other.m_stat, sizeof(m_stat));
        std::memcpy(&m_calculated, &other.m_calculated, sizeof(m_calculated));
    } else if (my_ts > other_ts) {
        have_newer = true;
    }
}

void Backend::push_items(std::vector<std::reference_wrapper<Couple>> & couples) const
{
    if (m_group != nullptr)
        m_group->push_items(couples);
}

void Backend::push_items(std::vector<std::reference_wrapper<Namespace>> & namespaces) const
{
    if (m_group != nullptr)
        m_group->push_items(namespaces);
}

void Backend::push_items(std::vector<std::reference_wrapper<Node>> & nodes) const
{
    nodes.push_back(m_node);
}

void Backend::push_items(std::vector<std::reference_wrapper<Group>> & groups) const
{
    if (m_group != nullptr)
        groups.push_back(*m_group);
}

void Backend::push_items(std::vector<std::reference_wrapper<FS>> & filesystems) const
{
    if (m_fs != nullptr)
        filesystems.push_back(*m_fs);
}

void Backend::print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer,
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

    writer.Key("node");
    writer.String(m_node.get_key().c_str());
    writer.Key("backend_id");
    writer.Uint64(m_stat.backend_id);
    writer.Key("addr");
    writer.String(m_key.c_str());
    writer.Key("state");
    writer.Uint64(m_stat.state);
    writer.Key("vfs_blocks");
    writer.Uint64(m_stat.vfs_blocks);
    writer.Key("vfs_bavail");
    writer.Uint64(m_stat.vfs_bavail);
    writer.Key("vfs_bsize");
    writer.Uint64(m_stat.vfs_bsize);
    writer.Key("records_total");
    writer.Uint64(m_stat.records_total);
    writer.Key("records_removed");
    writer.Uint64(m_stat.records_removed);
    writer.Key("records_removed_size");
    writer.Uint64(m_stat.records_removed_size);
    writer.Key("base_size");
    writer.Uint64(m_stat.base_size);
    writer.Key("fsid");
    writer.Uint64(m_stat.fsid);
    writer.Key("defrag_state");
    writer.Uint64(m_stat.defrag_state);
    writer.Key("want_defrag");
    writer.Uint64(m_stat.want_defrag);
    writer.Key("read_ios");
    writer.Uint64(m_stat.read_ios);
    writer.Key("write_ios");
    writer.Uint64(m_stat.write_ios);
    writer.Key("error");
    writer.Uint64(m_stat.error);
    writer.Key("blob_size_limit");
    writer.Uint64(m_stat.blob_size_limit);
    writer.Key("max_blob_base_size");
    writer.Uint64(m_stat.max_blob_base_size);
    writer.Key("blob_size");
    writer.Uint64(m_stat.blob_size);
    writer.Key("group");
    writer.Uint64(m_stat.group);

    writer.Key("vfs_free_space");
    writer.Uint64(m_calculated.vfs_free_space);
    writer.Key("vfs_total_space");
    writer.Uint64(m_calculated.vfs_total_space);
    writer.Key("vfs_used_space");
    writer.Uint64(m_calculated.vfs_used_space);
    writer.Key("records");
    writer.Uint64(m_calculated.records);
    writer.Key("free_space");
    writer.Uint64(m_calculated.free_space);
    writer.Key("total_space");
    writer.Uint64(m_calculated.total_space);
    writer.Key("used_space");
    writer.Uint64(m_calculated.used_space);
    writer.Key("effective_space");
    writer.Uint64(m_calculated.effective_space);
    writer.Key("effective_free_space");
    writer.Uint64(m_calculated.effective_free_space);
    writer.Key("fragmentation");
    writer.Double(m_calculated.fragmentation);
    writer.Key("read_rps");
    writer.Uint64(m_calculated.read_rps);
    writer.Key("write_rps");
    writer.Uint64(m_calculated.write_rps);
    writer.Key("max_read_rps");
    writer.Uint64(m_calculated.max_read_rps);
    writer.Key("max_write_rps");
    writer.Uint64(m_calculated.max_write_rps);
    writer.Key("status");
    writer.String(status_str(m_calculated.status));

    // XXX
    writer.Key("read_only");
    writer.Bool(m_read_only);

    writer.EndObject();
}

const char *Backend::status_str(Status status)
{
    switch (status) {
    case INIT:
        return "INIT";
    case OK:
        return "OK";
    case RO:
        return "RO";
    case BAD:
        return "BAD";
    case STALLED:
        return "STALLED";
    case BROKEN:
        return "BROKEN";
    }
    return "UNKNOWN";
}
