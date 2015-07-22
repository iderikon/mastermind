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
#include "FS.h"
#include "Node.h"
#include "Storage.h"
#include "TimestampParser.h"

Backend::Backend(Node & node)
    :
    m_node(node),
    m_vfs_free_space(0),
    m_vfs_total_space(0),
    m_vfs_used_space(0),
    m_records(0),
    m_free_space(0),
    m_total_space(0),
    m_used_space(0),
    m_fragmentation(0.0),
    m_read_rps(0),
    m_write_rps(0),
    m_max_read_rps(0),
    m_max_write_rps(0),
    m_status(INIT),
    m_read_only(0),
    m_disabled(false)
{}

void Backend::init(const BackendStat & stat)
{
    memcpy(&m_stat, &stat, sizeof(m_stat));
    m_fs = m_node.get_storage().get_fs(m_node.get_host(), stat.fsid);
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
        m_fs = m_node.get_storage().get_fs(m_node.get_host(), stat.fsid);
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

void Backend::print_info(std::ostream & ostr) const
{
    ostr << "Backend {\n"
            "  node: " << m_node.get_key() << "\n"
            "  fs: " << (m_fs != NULL ? m_fs->get_key().c_str() : "NULL") << "\n"
            "  BackendStat {\n"
            "    ts: " << TimestampParser::ts_user_friendly(m_stat.ts_sec, m_stat.ts_usec) << "\n"
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

