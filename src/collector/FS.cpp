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
#include "Host.h"
#include "Metrics.h"
#include "Node.h"
#include "WorkerApplication.h"

#include "Storage.h"

FS::FS(Node & node, uint64_t fsid)
    :
    m_node(node),
    m_fsid(fsid),
    m_status(OK),
    m_status_text("No updates yet")
{
    std::memset(&m_stat, 0, sizeof(m_stat));
    m_key = node.get_key() + "/" + std::to_string(fsid);
}

FS::FS(Node & node)
    :
    m_node(node),
    m_fsid(0),
    m_status(OK),
    m_status_text("No updates yet")
{
    std::memset(&m_stat, 0, sizeof(m_stat));
}

void FS::clone_from(const FS & other)
{
    m_fsid = other.m_fsid;
    m_key = other.m_key;
    std::memcpy(&m_stat, &other.m_stat, sizeof(m_stat));
    m_status = other.m_status;
    m_status_text = other.m_status_text;
}

void FS::update(const Backend & backend)
{
    const BackendStat & stat = backend.get_stat();
    m_stat.ts_sec = stat.ts_sec;
    m_stat.ts_usec = stat.ts_usec;
    m_stat.total_space = backend.get_vfs_total_space();
}

void FS::update_status()
{
    uint64_t total_space = 0;
    for (Backend & backend : m_backends) {
        Backend::Status status = backend.get_status();
        if (status != Backend::OK && status != Backend::BROKEN)
            continue;
        total_space += backend.get_total_space();
    }

    if (total_space <= m_stat.total_space) {
        m_status = OK;
        m_status_text = "Filesystem is OK";
    } else {
        m_status = BROKEN;

        std::ostringstream ostr;
        ostr << "Total space calculated from backends is " << total_space
             << " which is greater than " << m_stat.total_space
             << " from monitor stats";
        m_status_text = ostr.str();
    }
}

void FS::merge(const FS & other, bool & have_newer)
{
    uint64_t my_ts = m_stat.ts_sec * 1000000 + m_stat.ts_usec;
    uint64_t other_ts = other.m_stat.ts_sec * 1000000 + other.m_stat.ts_usec;
    if (my_ts < other_ts) {
        std::memcpy(&m_stat, &other.m_stat, sizeof(m_stat));
        m_status = other.m_status;
        m_status_text = other.m_status_text;
    } else if (my_ts > other_ts) {
        have_newer = true;
    }
}

void FS::push_items(std::vector<std::reference_wrapper<Couple>> & couples) const
{
    for (Backend & backend : m_backends)
        backend.push_items(couples);
}

void FS::push_items(std::vector<std::reference_wrapper<Namespace>> & namespaces) const
{
    for (Backend & backend : m_backends)
        backend.push_items(namespaces);
}

void FS::push_items(std::vector<std::reference_wrapper<Backend>> & backends) const
{
    backends.insert(backends.end(), m_backends.begin(), m_backends.end());
}

void FS::push_items(std::vector<std::reference_wrapper<Group>> & groups) const
{
    for (Backend & backend : m_backends)
        backend.push_items(groups);
}

void FS::push_items(std::vector<std::reference_wrapper<Node>> & nodes) const
{
    nodes.push_back(m_node);
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

    writer.Key("node");
    writer.String(m_node.get_key().c_str());
    writer.Key("fsid");
    writer.Uint64(m_fsid);
    writer.Key("total_space");
    writer.Uint64(m_stat.total_space);
    writer.Key("status");
    writer.String(status_str(m_status));
    writer.Key("status_text");
    writer.String(m_status_text.c_str());

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
