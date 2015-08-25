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
#include "Storage.h"
#include "WorkerApplication.h"

FS::FS(Node & node, uint64_t fsid)
    :
    m_node(node),
    m_fsid(fsid),
    m_status(OK)
{
    std::memset(&m_stat, 0, sizeof(m_stat));
    m_key = node.get_key() + "/" + std::to_string(fsid);
}

FS::FS(Node & node)
    :
    m_node(node),
    m_fsid(0),
    m_status(OK)
{
    std::memset(&m_stat, 0, sizeof(m_stat));
}

void FS::clone_from(const FS & other)
{
    m_fsid = other.m_fsid;
    m_key = other.m_key;
    std::memcpy(&m_stat, &other.m_stat, sizeof(m_stat));
    m_status = other.m_status;

    if (!other.m_backends.empty()) {
        BH_LOG(m_node.get_storage().get_app().get_logger(), DNET_LOG_ERROR,
                "Internal inconsistency detected: cloning FS '%s' from other "
                "one with non-empty set of backends", m_key.c_str());
    }
}

void FS::update(const Backend & backend)
{
    const BackendStat & stat = backend.get_stat();
    m_stat.ts_sec = stat.ts_sec;
    m_stat.ts_usec = stat.ts_usec;
    m_stat.total_space = backend.get_vfs_total_space();
}

void FS::get_items(std::vector<Couple*> & couples) const
{
    for (Backend *backend : m_backends)
        backend->get_items(couples);
}

void FS::get_items(std::vector<Namespace*> & namespaces) const
{
    for (Backend *backend : m_backends)
        backend->get_items(namespaces);
}

void FS::get_items(std::vector<Backend*> & backends) const
{
    backends.insert(backends.end(), m_backends.begin(), m_backends.end());
}

void FS::get_items(std::vector<Group*> & groups) const
{
    for (Backend *backend : m_backends) {
        Group *group = backend->get_group();
        if (group != nullptr)
            groups.push_back(group);
    }
}

void FS::get_items(std::vector<Node*> & nodes) const
{
    nodes.push_back(&m_node);
}

void FS::update_status()
{
    Status prev = m_status;

    uint64_t total_space = 0;
    for (Backend *backend : m_backends) {
        Backend::Status status = backend->get_status();
        if (status != Backend::OK && status != Backend::BROKEN)
            continue;
        total_space += backend->get_total_space();
    }

    m_status = (total_space <= m_stat.total_space) ? OK : BROKEN;
    if (m_status != prev)
        BH_LOG(m_node.get_storage().get_app().get_logger(), DNET_LOG_INFO,
                "FS %s/%lu status change %d -> %d",
                m_node.get_key().c_str(), m_fsid, int(prev), int(m_status));
}

void FS::merge(const FS & other)
{
    uint64_t my_ts = m_stat.ts_sec * 1000000 + m_stat.ts_usec;
    uint64_t other_ts = other.m_stat.ts_sec * 1000000 + other.m_stat.ts_usec;
    if (my_ts < other_ts) {
        std::memcpy(&m_stat, &other.m_stat, sizeof(m_stat));
        m_status = other.m_status;
    }
}

void FS::print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer,
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
    writer.String(m_node.get_host().c_str());
    writer.Key("fsid");
    writer.Uint64(m_fsid);
    writer.Key("total_space");
    writer.Uint64(m_stat.total_space);
    writer.Key("status");
    writer.String(status_str(m_status));

    writer.EndObject();
}

const char *FS::status_str(Status status)
{
    switch (status)
    {
    case OK:
        return "OK";
    case BROKEN:
        return "BROKEN";
    }
    return "UNKNOWN";
}
