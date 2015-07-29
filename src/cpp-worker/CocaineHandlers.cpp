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
#include "CocaineHandlers.h"
#include "Couple.h"
#include "Discovery.h"
#include "DiscoveryTimer.h"
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
        return (ns1->get_couple_count() > ns2->get_couple_count());
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
    Storage & storage = m_app.get_storage();

    std::vector<Node*> nodes;
    storage.get_nodes(nodes);

    size_t nr_backends = 0;
    for (size_t i = 0; i < nodes.size(); ++i)
        nr_backends += nodes[i]->get_backend_count();

    std::vector<Group*> groups;
    storage.get_groups(groups);

    std::vector<Couple*> couples;
    storage.get_couples(couples);

    std::vector<FS*> filesystems;
    std::vector<FS*> node_filesystems;
    for (size_t i = 0; i < nodes.size(); ++i) {
        nodes[i]->get_filesystems(node_filesystems);
        filesystems.insert(filesystems.end(), node_filesystems.begin(), node_filesystems.end());
        node_filesystems.clear();
    }

    std::map<Group::Status, int> group_status;
    for (size_t i = 0; i < groups.size(); ++i)
        ++group_status[groups[i]->get_status()];

    std::map<Couple::Status, int> couple_status;
    for (size_t i = 0; i < couples.size(); ++i)
        ++couple_status[couples[i]->get_status()];

    std::map<FS::Status, int> fs_status;
    for (size_t i = 0; i < filesystems.size(); ++i)
        ++fs_status[filesystems[i]->get_status()];

    std::ostringstream ostr;

    ostr << "Storage contains:\n"
         << nodes.size() << " nodes\n";

    ostr << filesystems.size() << " filesystems\n  ( ";
    for (auto it = fs_status.begin(); it != fs_status.end(); ++it)
        ostr << it->second << ' ' << FS::status_str(it->first) << ' ';

    ostr << ")\n" << nr_backends << " backends\n";

    ostr << groups.size() << " groups\n  ( ";
    for (auto it = group_status.begin(); it != group_status.end(); ++it)
        ostr << it->second << ' ' << Group::status_str(it->first) << ' ';

    ostr << ")\n" << couples.size() << " couples\n  ( ";
    for (auto it = couple_status.begin(); it != couple_status.end(); ++it)
        ostr << it->second << ' ' << Couple::status_str(it->first) << ' ';
    ostr << ")\n";

    std::vector<Namespace*> namespaces;
    storage.get_namespaces(namespaces);

    ostr << namespaces.size() << " namespaces\n";

    {
        const Discovery::ClockStat & stat = m_app.get_discovery().get_clock_stat();
        ostr << "Discovery metrics:\n"
                "  Total update time: " << SECONDS(stat.full) << " s\n"
                "  Resolve node list: " << SECONDS(stat.resolve_nodes) << " s\n"
                "  Download nodes description: " << SECONDS(stat.discover_nodes) << " s\n"
                "  Finish processing monitor stats: " << SECONDS(stat.finish_monitor_stats) << " s\n";
    }

    {
        const Storage::ClockStat & stat = m_app.get_storage().get_clock_stat();
        ostr << "  Schedule metadata download time: " << SECONDS(stat.schedule_update_time) << " s\n"
                "  Metadata download total time: " << SECONDS(stat.metadata_download_total_time) << " s\n"
                "  Status update time: " << SECONDS(stat.status_update_time) << " s\n";
    }

    {
        SerialDistribution distrib_procfs;
        SerialDistribution distrib_backend;
        SerialDistribution distrib_update_fs;

        for (Node *node : nodes) {
            const Node::ClockStat & stat = node->get_clock_stat();
            distrib_procfs.add_sample(stat.procfs);
            distrib_backend.add_sample(stat.backend);
            distrib_update_fs.add_sample(stat.update_fs);
        }

        ostr << "Distribution for node procfs processing:\n" << distrib_procfs.str() << "\n"
                "Distribution for node backend processing:\n" << distrib_backend.str() << "\n"
                "Distribution for node fs update:\n" << distrib_update_fs.str() << '\n';
    }

    {
        SerialDistribution distrib;
        for (Group *group : groups)
            distrib.add_sample(group->get_metadata_process_time());
        ostr << "Distribution for group metadata processing:\n" << distrib.str() << '\n';
    }

    {
        SerialDistribution distrib;
        for (Couple *couple : couples)
            distrib.add_sample(couple->get_update_status_time());
        ostr << "Distribution for couple update_status:\n" << distrib.str() << '\n';
    }

    response()->write(ostr.str());
    response()->close();
}

void on_group_info::on_chunk(const char *chunk, size_t size)
{
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
            ostr << "Group " << group_id << " is not found";
            break;
        }

        group->print_info(ostr);
    } while (0);

    response()->write(ostr.str());
    response()->close();
}

void on_list_nodes::on_chunk(const char *chunk, size_t size)
{
    std::vector<Node*> nodes;
    m_app.get_storage().get_nodes(nodes);

    std::ostringstream ostr;
    ostr << "There are " << nodes.size() << " nodes\n";

    for (size_t i = 0; i < nodes.size(); ++i) {
        Node *node = nodes[i];
        if (node == NULL) {
            ostr << "  <NULL>\n";
            continue;
        }
        ostr << "  " << node->get_host() << ':' << node->get_port()
             << ':' << node->get_family() << '\n';
    }

    response()->write(ostr.str());
    response()->close();
}

void on_node_info::on_chunk(const char *chunk, size_t size)
{
    std::ostringstream ostr;
    do {
        Node *node;
        if (!m_app.get_storage().get_node(chunk, node)) {
            ostr << "Node " << chunk << " does not exist";
            break;
        }

        node->print_info(ostr);
    } while (0);

    response()->write(ostr.str());
    response()->close();
}

void on_node_list_backends::on_chunk(const char *chunk, size_t size)
{
    const char *name = chunk;
    std::ostringstream ostr;
    do {
        Node *node;
        if (!m_app.get_storage().get_node(name, node)) {
            ostr << "Node " << name << " does not exist";
            break;
        }

        if (node == NULL) {
            ostr << "Node is NULL";
            break;
        }

        std::vector<Backend*> backends;
        node->get_backends(backends);

        ostr << "Node has " << backends.size() << " backends\n";

        for (size_t i = 0; i < backends.size(); ++i)
            ostr << "  " << name << '/' << backends[i]->get_stat().backend_id << '\n';
    } while (0);

    response()->write(ostr.str());
    response()->close();
}

void on_backend_info::on_chunk(const char *chunk, size_t size)
{
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
    response()->close();
}

void on_fs_info::on_chunk(const char *chunk, size_t size)
{
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
    response()->close();
}

void on_fs_list_backends::on_chunk(const char *chunk, size_t size)
{
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
    response()->close();
}

void on_list_namespaces::on_chunk(const char *chunk, size_t size)
{
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
    response()->close();
}

void on_group_couple_info::on_chunk(const char *chunk, size_t size)
{
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
    response()->close();
}

void on_force_update::on_chunk(const char *chunk, size_t size)
{
    BH_LOG(m_app.get_logger(), DNET_LOG_INFO, "Request to force update");

    std::string resp("Update has been scheduled");

    do {
        if (m_app.get_discovery().in_progress()) {
            resp = "Discovery is in progress";
            BH_LOG(m_app.get_logger(), DNET_LOG_INFO, resp);
            break;
        }

        m_app.get_discovery_timer().disarm();
        m_app.get_discovery().schedule_start();
    } while (0);

    response()->write(resp);
    response()->close();
}

void on_get_snapshot::on_chunk(const char *chunk, size_t size)
{
    std::string request(chunk, size);

    BH_LOG(m_app.get_logger(), DNET_LOG_INFO, "Snapshot requested: '%s'", request.c_str());

    Filter filter;

    if (!request.empty()) {
        if (parse_filter(request, filter) != 0) {
            response()->error(-1, "Incorrect filter syntax");
            response()->close();
            return;
        }
    }

    m_app.get_storage().get_snapshot(filter, shared_from_this());
}

void on_refresh::on_chunk(const char *chunk, size_t size)
{
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
}
