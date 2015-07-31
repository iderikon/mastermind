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

#include "Backend.h"
#include "Filter.h"
#include "FS.h"
#include "Metrics.h"
#include "Node.h"
#include "Storage.h"
#include "WorkerApplication.h"

Backend::Backend(Node & node)
    :
    m_node(node),
    m_fs(NULL),
    m_group(NULL),
    m_vfs_free_space(0),
    m_vfs_total_space(0),
    m_vfs_used_space(0),
    m_records(0),
    m_free_space(0),
    m_total_space(0),
    m_used_space(0),
    m_effective_space(0),
    m_fragmentation(0.0),
    m_read_rps(0),
    m_write_rps(0),
    m_max_read_rps(0),
    m_max_write_rps(0),
    m_status(INIT),
    m_read_only(false),
    m_disabled(false)
{}

void Backend::init(const BackendStat & stat)
{
    memcpy(&m_stat, &stat, sizeof(m_stat));
    m_fs = m_node.get_fs(stat.fsid);
    m_key = m_node.get_key() + '/' + std::to_string(stat.backend_id);
    recalculate();
}

void Backend::update(const BackendStat & stat)
{
    double ts1 = double(m_stat.ts_sec) + double(m_stat.ts_usec) / 1000000.0;
    double ts2 = double(stat.ts_sec) + double(stat.ts_usec) / 1000000.0;
    double d_ts = ts2 - ts1;

    if (d_ts > 1.0) {
        m_read_rps = int(double(stat.read_ios - m_stat.read_ios) / d_ts);
        m_write_rps = int(double(stat.write_ios - m_stat.write_ios) / d_ts);

        // XXX RPS_FORMULA
        m_max_read_rps = int(std::max(double(m_read_rps) /
                    std::max(m_node.get_stat().load_average, 0.01), 100.0));
        m_max_write_rps = int(std::max(double(m_write_rps) /
                    std::max(m_node.get_stat().load_average, 0.01), 100.0));
    }

    if (m_stat.fsid != stat.fsid) {
        m_fs->remove_backend(this);
        m_fs = m_node.get_fs(stat.fsid);
        m_fs->add_backend(this);
    }

    std::memcpy(&m_stat, &stat, sizeof(m_stat));
    recalculate();
}

void Backend::recalculate()
{
    m_vfs_total_space = m_stat.vfs_blocks * m_stat.vfs_bsize;
    m_vfs_free_space = m_stat.vfs_bavail * m_stat.vfs_bsize;
    m_vfs_used_space = m_vfs_total_space - m_vfs_free_space;

    m_records = m_stat.records_total - m_stat.records_removed;
    m_fragmentation = double(m_stat.records_removed) / double(std::max(m_stat.records_total, 1UL));

    if (m_stat.blob_size_limit) {
        // vfs_total_space can be less than blob_size_limit in case of misconfiguration
        m_total_space = std::min(m_stat.blob_size_limit, m_vfs_total_space);
        m_used_space = m_stat.base_size;
        m_free_space = std::min(m_vfs_free_space,
                uint64_t(std::max(0L, int64_t(m_total_space) - int64_t(m_used_space))));
    } else {
        m_total_space = m_vfs_total_space;
        m_free_space = m_vfs_free_space;
        m_used_space = m_vfs_used_space;
    }

    double share = double(m_total_space) / double(m_vfs_total_space);
    int64_t free_space_req_share =
        std::ceil(double(m_node.get_storage().get_app().get_config().reserved_space) * share);
    m_effective_space = std::max(0L, int64_t(m_total_space) - free_space_req_share);

    m_fs->update(*this);

    if (m_stat.error || m_disabled)
        m_status = STALLED;
    else if (m_fs->get_status() == FS::BROKEN)
        m_status = BROKEN;
    else if (m_read_only)
        m_status = RO;
    else
        m_status = OK;
}

bool Backend::match(const Filter & filter, uint32_t item_types) const
{
    if ((item_types & Filter::Group) && !filter.groups.empty()) {
        if (!std::binary_search(filter.groups.begin(), filter.groups.end(),
                    m_stat.group))
            return false;
    }

    if ((item_types & Filter::Backend) && !filter.backends.empty()) {
        if (!std::binary_search(filter.backends.begin(), filter.backends.end(),
                    get_key()))
            return false;
    }

    if ((item_types & Filter::Node) && !filter.nodes.empty()) {
        if (!std::binary_search(filter.nodes.begin(), filter.nodes.end(),
                    m_node.get_key()))
            return false;
    }

    if ((item_types & Filter::Couple) && !filter.couples.empty()) {
        if (m_group == NULL || m_group->get_couple() == NULL)
            return false;

        if (!std::binary_search(filter.couples.begin(), filter.couples.end(),
                    m_group->get_couple()->get_key()))
            return false;
    }

    if ((item_types & Filter::Namespace) && !filter.namespaces.empty()) {
        if (m_group == NULL || m_group->get_namespace() == NULL)
            return false;

        if (!std::binary_search(filter.namespaces.begin(), filter.namespaces.end(),
                    m_group->get_namespace()->get_name()))
            return false;
    }

    if ((item_types & Filter::FS) && !filter.filesystems.empty()) {
        if (m_fs == NULL)
            return false;

        if (!std::binary_search(filter.filesystems.begin(), filter.filesystems.end(),
                    m_fs->get_key()))
            return false;
    }

    return true;
}

void Backend::print_info(std::ostream & ostr) const
{
    ostr << "Backend {\n"
            "  node: " << m_node.get_key() << "\n"
            "  fs: " << (m_fs != NULL ? m_fs->get_key().c_str() : "NULL") << "\n"
            "  BackendStat {\n"
            "    ts: " << timeval_user_friendly(m_stat.ts_sec, m_stat.ts_usec) << "\n"
            "    backend_id: " << m_stat.backend_id << "\n"
            "    state: " << m_stat.state << "\n"
            "    vfs_blocks: " << m_stat.vfs_blocks << "\n"
            "    vfs_bavail: " << m_stat.vfs_bavail << "\n"
            "    vfs_bsize: " << m_stat.vfs_bsize << "\n"
            "    records_total: " << m_stat.records_total << "\n"
            "    records_removed: " << m_stat.records_removed << "\n"
            "    records_removed_size: " << m_stat.records_removed_size << "\n"
            "    base_size: " << m_stat.base_size << "\n"
            "    fsid: " << m_stat.fsid << "\n"
            "    defrag_state: " << m_stat.defrag_state << "\n"
            "    want_defrag: " << m_stat.want_defrag << "\n"
            "    read_ios: " << m_stat.read_ios << "\n"
            "    write_ios: " << m_stat.write_ios << "\n"
            "    error: " << m_stat.error << "\n"
            "    blob_size_limit: " << m_stat.blob_size_limit << "\n"
            "    max_blob_base_size: " << m_stat.max_blob_base_size << "\n"
            "    blob_size: " << m_stat.blob_size << "\n"
            "    group: " << m_stat.group << "\n"
            "  }\n"
            "  vfs_free_space: " << m_vfs_free_space << "\n"
            "  vfs_total_space: " << m_vfs_total_space << "\n"
            "  vfs_used_space: " << m_vfs_used_space << "\n"
            "  records: " << m_records << "\n"
            "  free_space: " << m_free_space << "\n"
            "  total_space: " << m_total_space << "\n"
            "  used_space: " << m_used_space << "\n"
            "  effective_space: " << m_effective_space << "\n"
            "  fragmentation: " << m_fragmentation << "\n"
            "  read_rps: " << m_read_rps << "\n"
            "  write_rps: " << m_write_rps << "\n"
            "  max_read_rps: " << m_max_read_rps << "\n"
            "  max_write_rps: " << m_max_write_rps << "\n"
            "  status: " << status_str(m_status) << "\n"
            "  disabled: " << m_disabled << "\n"
            "  read_only: " << m_read_only << "\n"
            "}";
}

void Backend::print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer) const
{
    writer.StartObject();

    writer.Key("timestamp");
    writer.StartObject();
    writer.Key("tv_sec");
    writer.Uint64(m_stat.ts_sec);
    writer.Key("tv_usec");
    writer.Uint64(m_stat.ts_usec);
    writer.EndObject();

    writer.Key("node");
    writer.String(m_node.get_key().c_str());
    writer.Key("backend_id");
    writer.Uint64(m_stat.backend_id);
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
    writer.Uint64(m_vfs_free_space);
    writer.Key("vfs_total_space");
    writer.Uint64(m_vfs_total_space);
    writer.Key("vfs_used_space");
    writer.Uint64(m_vfs_used_space);
    writer.Key("records");
    writer.Uint64(m_records);
    writer.Key("free_space");
    writer.Uint64(m_free_space);
    writer.Key("total_space");
    writer.Uint64(m_total_space);
    writer.Key("used_space");
    writer.Uint64(m_used_space);
    writer.Key("fragmentation");
    writer.Double(m_fragmentation);
    writer.Key("read_rps");
    writer.Uint64(m_read_rps);
    writer.Key("write_rps");
    writer.Uint64(m_write_rps);
    writer.Key("max_read_rps");
    writer.Uint64(m_max_read_rps);
    writer.Key("max_write_rps");
    writer.Uint64(m_max_write_rps);
    writer.Key("status");
    writer.String(status_str(m_status));

    // XXX
    writer.Key("read_only");
    writer.Bool(m_read_only);
    writer.Key("disabled");
    writer.Bool(m_disabled);

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

