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

struct NSVolumeSort
{
    bool operator () (Namespace *ns1, Namespace *ns2) const
    {
        return (ns1->get_couples().size() > ns2->get_couples().size());
    }
};

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
        m_group_id = std::stol(chunk);
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
#if 0
    std::ostringstream ostr;

    do {
        const char *slash = std::strchr(chunk, '/');
        if (slash == NULL) {
            ostr << "Invalid backend id '" << chunk << "'\n"
                    "Syntax: <host>:<port>:<family>/<backend id>";
            break;
        }

        std::string node_name(chunk, slash - chunk);
        int backend_id = atoi(slash + 1);

        Node *node;
        if (!m_app.get_storage().get_node(node_name, node)) {
            ostr << "Node " << node_name << " does not exist";
            break;
        }

        Backend *backend;
        if (!node->get_backend(backend_id, backend)) {
            ostr << "Backend " << backend_id << " does not exist";
            break;
        }

        backend->print_info(ostr);
    } while (0);

    response()->write(ostr.str());
#endif
    response()->close();
}

void on_fs_info::on_chunk(const char *chunk, size_t size)
{
#if 0
    std::ostringstream ostr;
    std::string key(chunk);

    do {
        size_t pos = key.rfind('/');
        if (pos == std::string::npos) {
            ostr << "Invalid FS key '" << key << '\'';
            break;
        }

        unsigned long long fsid = std::stoull(key.substr(pos + 1));
        std::string node_key = key.substr(0, pos);

        Node *node;
        if (!m_app.get_storage().get_node(node_key, node)) {
            ostr << "Node '" << node_key << "' does not exist";
            break;
        }

        FS *fs;
        if (!node->get_fs(uint64_t(fsid), fs)) {
            ostr << "Found no FS '" << key << '\'';
            break;
        }

        fs->print_info(ostr);
    } while (0);

    response()->write(ostr.str());
#endif
    response()->close();
}

void on_fs_list_backends::on_chunk(const char *chunk, size_t size)
{
#if 0
    std::ostringstream ostr;
    std::string key(chunk);

    do {
        size_t pos = key.rfind('/');
        if (pos == std::string::npos) {
            ostr << "Invalid FS key '" << key << '\'';
            break;
        }

        unsigned long long fsid = std::stoull(key.substr(pos + 1));
        std::string node_key = key.substr(0, pos);

        Node *node;
        if (!m_app.get_storage().get_node(node_key, node)) {
            ostr << "Node '" << node_key << "' does not exist";
            break;
        }

        FS *fs;
        if (!node->get_fs(uint64_t(fsid), fs)) {
            ostr << "Found no FS '" << key << '\'';
            break;
        }

        std::vector<Backend*> backends;
        fs->get_backends(backends);

        ostr << "There are " << backends.size() << " backends\n";
        if (backends.empty())
            break;

        for (size_t i = 0; i < backends.size(); ++i) {
            const Backend & backend = *backends[i];
            ostr << "  " << backend.get_node().get_key() << '/' << backend.get_stat().backend_id << '\n';
        }
    } while (0);

    response()->write(ostr.str());
#endif
    response()->close();
}

void on_list_namespaces::on_chunk(const char *chunk, size_t size)
{
#if 0
    std::vector<Namespace*> namespaces;
    m_app.get_storage().get_namespaces(namespaces);

    std::ostringstream ostr;
    ostr << "There are " << namespaces.size() << " namespaces\n";

    std::sort(namespaces.begin(), namespaces.end(), NSVolumeSort());

    for (size_t i = 0; i < namespaces.size(); ++i) {
        Namespace *ns = namespaces[i];
        ostr << "  '" << ns->get_name() << "' (" << ns->get_couple_count() << " couples)\n";
    }

    response()->write(ostr.str());
#endif
    response()->close();
}

void on_group_couple_info::on_chunk(const char *chunk, size_t size)
{
#if 0
    std::ostringstream ostr;

    do {
        char c;
        int group_id;
        if (sscanf(chunk, "%d%c", &group_id, &c) != 1) {
            ostr << "Invalid group id " << group_id;
            break;
        }

        Group *group;
        if (!m_app.get_storage().get_group(group_id, group)) {
            ostr << "Group " << group_id << " does not exist";
            break;
        }

        Couple *couple = group->get_couple();
        if (couple == NULL) {
            ostr << "Couple is NULL";
            break;
        }

        couple->print_info(ostr);
    } while (0);

    response()->write(ostr.str());
#endif
    response()->close();
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
/* TODO
    std::string request(chunk, size);

    BH_LOG(m_app.get_logger(), DNET_LOG_INFO, "Refresh requested: '%s'", request.c_str());

    Filter filter;

    if (!request.empty()) {
        if (parse_filter(request, filter) != 0) {
            response()->error(-1, "Incorrect filter syntax");
            response()->close();
            return;
        }
    }

    m_app.get_storage().refresh(filter, shared_from_this());
*/
}
