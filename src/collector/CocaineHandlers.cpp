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
#include "CocaineHandlers.h"
#include "Couple.h"
#include "Discovery.h"
#include "FilterParser.h"
#include "FS.h"
#include "Group.h"
#include "Metrics.h"
#include "Node.h"
#include "Storage.h"

namespace {

int parse_filter(const std::string & request, Filter & filter)
{
    FilterParser parser(filter);

    rapidjson::Reader reader;
    rapidjson::StringStream ss(request.c_str());
    reader.Parse(ss, parser);

    if (!parser.good())
        return -1;

    return 0;
}

} // unnamed namespace

void on_summary::on_chunk(const char *chunk, size_t size)
{
    m_app.get_collector().summary(shared_from_this());
}

void on_group_info::on_chunk(const char *chunk, size_t size)
{
    try {
        m_group_id = std::stoi(chunk);
    } catch (...) {
        response()->error(-1, "invalid integer");
        response()->close();
    }

    m_app.get_collector().group_info(shared_from_this());
}

void on_list_nodes::on_chunk(const char *chunk, size_t size)
{
    m_app.get_collector().list_nodes(shared_from_this());
}

void on_node_info::on_chunk(const char *chunk, size_t size)
{
    m_node_name.assign(chunk, size);
    m_app.get_collector().node_info(shared_from_this());
}

void on_node_list_backends::on_chunk(const char *chunk, size_t size)
{
    m_node_name.assign(chunk, size);
    m_app.get_collector().node_list_backends(shared_from_this());
}

void on_backend_info::on_chunk(const char *chunk, size_t size)
{
    const std::string key(chunk, size);

    if (!Storage::split_node_num(key, m_node_name, m_backend_id)) {
        std::ostringstream ostr;
        ostr << "Invalid backend key '" << key << "'\n"
                "Syntax: <host>:<port>:<family>/<backend id>";
        response()->error(-1, ostr.str());
        response()->close();
        return;
    }

    m_app.get_collector().backend_info(shared_from_this());
}

void on_fs_info::on_chunk(const char *chunk, size_t size)
{
    const std::string key(chunk, size);

    if (!Storage::split_node_num(key, m_node_name, m_fsid)) {
        std::ostringstream ostr;
        ostr << "Invalid FS key '" << key << "'\n"
                "Syntax: <host>:<port>:<family>/<fs id>";
        response()->error(-1, ostr.str());
        response()->close();
        return;
    }

    m_app.get_collector().fs_info(shared_from_this());
}

void on_list_namespaces::on_chunk(const char *chunk, size_t size)
{
    m_app.get_collector().list_namespaces(shared_from_this());
}

void on_group_couple_info::on_chunk(const char *chunk, size_t size)
{
    std::string group_str(chunk, size);
    try {
        m_group_id = std::stoi(group_str);
    } catch (...) {
        std::ostringstream ostr;
        ostr << "Invalid group id '" << group_str << '\'';
        response()->error(-1, ostr.str());
        response()->close();
    }

    m_app.get_collector().group_couple_info(shared_from_this());
}

void on_force_update::on_chunk(const char *chunk, size_t size)
{
    BH_LOG(m_app.get_logger(), DNET_LOG_INFO, "Request to force update");
    m_app.get_collector().force_update(shared_from_this());
}

void on_get_snapshot::on_chunk(const char *chunk, size_t size)
{
    std::string request(chunk, size);

    BH_LOG(m_app.get_logger(), DNET_LOG_INFO, "Snapshot requested: '%s'", request.c_str());

    if (!request.empty()) {
        if (parse_filter(request, m_filter) != 0) {
            response()->error(-1, "Incorrect filter syntax");
            response()->close();
            return;
        }
    }

    m_app.get_collector().get_snapshot(shared_from_this());
}

void on_refresh::on_chunk(const char *chunk, size_t size)
{
    std::string request(chunk, size);

    BH_LOG(m_app.get_logger(), DNET_LOG_INFO, "Refresh requested: '%s'", request.c_str());

    if (!request.empty()) {
        if (parse_filter(request, m_filter) != 0) {
            response()->error(-1, "Incorrect filter syntax");
            response()->close();
            return;
        }
    }

    m_app.get_collector().refresh(shared_from_this());
}
