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

#include "CocaineHandlers.h"
#include "FS.h"
#include "Metrics.h"
#include "Node.h"
#include "WorkerApplication.h"

Collector::Collector(WorkerApplication & app)
    :
    m_app(app),
    m_discovery(app),
    m_storage(app),
    m_merge_time(0)
{
    m_queue = dispatch_queue_create("collector", DISPATCH_QUEUE_CONCURRENT);
}

int Collector::init()
{
    if (m_discovery.init_curl())
        return -1;
    if (m_discovery.init_elliptics())
        return -1;
    return 0;
}

void Collector::start()
{
    BH_LOG(m_app.get_logger(), DNET_LOG_INFO, "Collector: starting");
    dispatch_async_f(m_queue, this, &Collector::step1_start_round);
}

void Collector::finalize_round(Round *round)
{
    dispatch_barrier_async_f(m_queue, round, &Collector::step5_merge);
}

void Collector::step1_start_round(void *arg)
{
    Collector & self = *static_cast<Collector*>(arg);

    BH_LOG(self.m_app.get_logger(), DNET_LOG_INFO, "Collector round: step 1");

    Round *round = new Round(self);
    self.m_discovery.resolve_nodes(*round);
    round->start();
}

void Collector::step1_start_forced(void *arg)
{
    std::unique_ptr<std::shared_ptr<on_force_update>> handler_ptr(
            static_cast<std::shared_ptr<on_force_update>*>(arg));
    Collector & self = (*handler_ptr)->get_app().get_collector();

    BH_LOG(self.m_app.get_logger(), DNET_LOG_INFO, "Collector user-requested round: step 1");

    Round *round = new Round(self, *handler_ptr);
    self.m_discovery.resolve_nodes(*round);
    round->start();
}

void Collector::step5_merge(void *arg)
{
    std::unique_ptr<Round> round(static_cast<Round*>(arg));
    Collector & self = round->get_collector();

    Stopwatch watch(self.m_merge_time);

    self.m_storage.merge(round->get_storage());

    watch.stop();

    Round::ClockStat & round_clock = round->get_clock();
    clock_stop(round_clock.total);

    Round::Type type = round->get_type();
    if (type == Round::REGULAR || type == Round::FORCED_FULL) {
        self.m_round_clock = round->get_clock();
        if (type == Round::REGULAR) {
            dispatch_after_f(dispatch_time(DISPATCH_TIME_NOW, 60000000000L),
                    self.m_queue, &self, &Collector::step1_start_round);
        } else {
            std::shared_ptr<on_force_update> handler = round->get_on_force_handler();

            std::ostringstream ostr;
            ostr << "Update completed in " << SECONDS(round_clock.total) << " seconds";
            handler->response()->write(ostr.str());
            handler->response()->close();
        }
    }
}

void Collector::force_update(std::shared_ptr<on_force_update> handler)
{
    dispatch_async_f(m_queue, new std::shared_ptr<on_force_update>(handler), &Collector::step1_start_forced);
}

void Collector::get_snapshot(std::shared_ptr<on_get_snapshot> handler)
{
    dispatch_async_f(m_queue, new std::shared_ptr<on_get_snapshot>(handler), &Collector::execute_get_snapshot);
}

void Collector::group_info(std::shared_ptr<on_group_info> handler)
{
    dispatch_async_f(m_queue, new std::shared_ptr<on_group_info>(handler), &Collector::execute_group_info);
}

void Collector::list_nodes(std::shared_ptr<on_list_nodes> handler)
{
    dispatch_async_f(m_queue, new std::shared_ptr<on_list_nodes>(handler), &Collector::execute_list_nodes);
}

void Collector::node_info(std::shared_ptr<on_node_info> handler)
{
    dispatch_async_f(m_queue, new std::shared_ptr<on_node_info>(handler), &Collector::execute_node_info);
}

void Collector::summary(std::shared_ptr<on_summary> handler)
{
    dispatch_async_f(m_queue, new std::shared_ptr<on_summary>(handler), &Collector::execute_summary);
}

void Collector::backend_info(std::shared_ptr<on_backend_info> handler)
{
    dispatch_async_f(m_queue, new std::shared_ptr<on_backend_info>(handler), &Collector::execute_backend_info);
}

void Collector::fs_info(std::shared_ptr<on_fs_info> handler)
{
    dispatch_async_f(m_queue, new std::shared_ptr<on_fs_info>(handler), &Collector::execute_fs_info);
}

void Collector::fs_list_backends(std::shared_ptr<on_fs_list_backends> handler)
{
    dispatch_async_f(m_queue, new std::shared_ptr<on_fs_list_backends>(handler), &Collector::execute_fs_list_backends);
}

void Collector::group_couple_info(std::shared_ptr<on_group_couple_info> handler)
{
    dispatch_async_f(m_queue, new std::shared_ptr<on_group_couple_info>(handler), &Collector::execute_group_couple_info);
}

void Collector::list_namespaces(std::shared_ptr<on_list_namespaces> handler)
{
    dispatch_async_f(m_queue, new std::shared_ptr<on_list_namespaces>(handler), &Collector::execute_list_namespaces);
}

void Collector::node_list_backends(std::shared_ptr<on_node_list_backends> handler)
{
    dispatch_async_f(m_queue, new std::shared_ptr<on_node_list_backends>(handler), &Collector::execute_node_list_backends);
}

void Collector::execute_get_snapshot(void *arg)
{
    std::unique_ptr<std::shared_ptr<on_get_snapshot>> handler_ptr(
            static_cast<std::shared_ptr<on_get_snapshot>*>(arg));
    Collector & self = (*handler_ptr)->get_app().get_collector();

    Filter & filter = (*handler_ptr)->get_filter();
    std::string result;

    if (filter.empty())
        self.m_storage.print_json(filter.item_types, result);
    else
        self.m_storage.print_json(filter, result);

    (*handler_ptr)->response()->write(result);
    (*handler_ptr)->response()->close();
}

void Collector::execute_group_info(void *arg)
{
    std::unique_ptr<std::shared_ptr<on_group_info>> handler_ptr(
            static_cast<std::shared_ptr<on_group_info>*>(arg));
    Collector & self = (*handler_ptr)->get_app().get_collector();

    std::ostringstream ostr;

    int group_id = (*handler_ptr)->get_group_id();
    Group *group = nullptr;
    if (!self.m_storage.get_group(group_id, group) || group == nullptr) {
        ostr << "Group " << group_id << " is not found";
        (*handler_ptr)->response()->error(-1, ostr.str());
    } else {
        group->print_info(ostr);
        (*handler_ptr)->response()->write(ostr.str());
    }

    (*handler_ptr)->response()->close();
}

void Collector::execute_list_nodes(void *arg)
{
    std::unique_ptr<std::shared_ptr<on_group_info>> handler_ptr(
            static_cast<std::shared_ptr<on_group_info>*>(arg));
    Collector & self = (*handler_ptr)->get_app().get_collector();

    std::map<std::string, Node> & nodes = self.m_storage.get_nodes();

    std::ostringstream ostr;
    ostr << "There are " << nodes.size() << " nodes\n";

    for (auto it = nodes.begin(); it != nodes.end(); ++it) {
        Node & node = it->second;
        ostr << "  " << node.get_host() << ':' << node.get_port()
             << ':' << node.get_family() << '\n';
    }

    (*handler_ptr)->response()->write(ostr.str());
    (*handler_ptr)->response()->close();
}

void Collector::execute_node_info(void *arg)
{
    std::unique_ptr<std::shared_ptr<on_node_info>> handler_ptr(
            static_cast<std::shared_ptr<on_node_info>*>(arg));
    Collector & self = (*handler_ptr)->get_app().get_collector();

    std::ostringstream ostr;

    const std::string & node_name = (*handler_ptr)->get_node_name();
    Node *node = nullptr;
    if (!self.m_storage.get_node(node_name, node) || node == nullptr) {
        ostr << "Node " << node_name << " does not exist";
        (*handler_ptr)->response()->error(-1, ostr.str());
    } else {
        node->print_info(ostr);
        (*handler_ptr)->response()->write(ostr.str());
    }

    (*handler_ptr)->response()->close();
}

void Collector::execute_summary(void *arg)
{
    std::unique_ptr<std::shared_ptr<on_summary>> handler_ptr(
            static_cast<std::shared_ptr<on_summary>*>(arg));
    Collector & self = (*handler_ptr)->get_app().get_collector();

    std::map<std::string, Node> & nodes = self.m_storage.get_nodes();
    std::map<int, Group> & groups = self.m_storage.get_groups();
    std::map<std::string, Couple> & couples = self.m_storage.get_couples();

    std::map<Group::Status, int> group_status;
    std::map<Couple::Status, int> couple_status;
    std::map<FS::Status, int> fs_status;
    size_t nr_backends = 0;
    size_t nr_filesystems = 0;

    for (auto it = groups.begin(); it != groups.end(); ++it)
        ++group_status[it->second.get_status()];

    for (auto it = couples.begin(); it != couples.end(); ++it)
        ++couple_status[it->second.get_status()];

    for (auto nit = nodes.begin(); nit != nodes.end(); ++nit) {
        Node & node = nit->second;
        nr_backends += node.get_backends().size();

        const std::map<uint64_t, FS> & fs = node.get_filesystems();
        nr_filesystems += fs.size();
        for (auto fsit = fs.begin(); fsit != fs.end(); ++fsit)
            ++fs_status[fsit->second.get_status()];
    }

    std::ostringstream ostr;

    ostr << "Storage contains:\n"
         << nodes.size() << " nodes\n";

    ostr << nr_filesystems << " filesystems\n  ( ";
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

    ostr << self.m_storage.get_namespaces().size() << " namespaces\n";

    ostr << "Round metrics:\n"
            "  Total time: " << SECONDS(self.m_round_clock.total) << " s\n"
            "  HTTP download time: " << SECONDS(self.m_round_clock.perform_download) << " s\n"
            "  Remaining JSON parsing after HTTP download completed: "
                << SECONDS(self.m_round_clock.finish_monitor_stats) << " s\n"
            "  Metadata download: " << SECONDS(self.m_round_clock.metadata_download) << " s\n"
            "  Storage update: " << SECONDS(self.m_round_clock.storage_update) << " s\n"
            "  Storage merge: " << SECONDS(self.m_merge_time) << " s\n";

    {
        SerialDistribution distrib_procfs_parse;
        SerialDistribution distrib_backend_parse;
        SerialDistribution distrib_update_fs;

        for (auto it = nodes.begin(); it != nodes.end(); ++it) {
            const Node::ClockStat & stat = it->second.get_clock_stat();
            distrib_procfs_parse.add_sample(stat.procfs_parse);
            distrib_backend_parse.add_sample(stat.backend_parse);
            distrib_update_fs.add_sample(stat.update_fs);
        }

        ostr << "\nDistribution for node procfs parsing:\n" << distrib_procfs_parse.str() << "\n"
                "Distribution for node backend parsing:\n" << distrib_backend_parse.str() << "\n"
                "Distribution for node fs update:\n" << distrib_update_fs.str() << '\n';
    }

    {
        SerialDistribution distrib;
        for (auto it = groups.begin(); it != groups.end(); ++it)
            distrib.add_sample(it->second.get_metadata_process_time());
        ostr << "Distribution for group metadata processing:\n" << distrib.str() << '\n';
    }

    {
        SerialDistribution distrib;
        for (auto it = couples.begin(); it != couples.end(); ++it)
            distrib.add_sample(it->second.get_update_status_time());
        ostr << "Distribution for couple update_status:\n" << distrib.str() << '\n';
    }

    (*handler_ptr)->response()->write(ostr.str());
    (*handler_ptr)->response()->close();
}

void Collector::execute_backend_info(void *arg)
{
    std::unique_ptr<std::shared_ptr<on_backend_info>> handler_ptr(
            static_cast<std::shared_ptr<on_backend_info>*>(arg));
    // Collector & self = (*handler_ptr)->get_app().get_collector();
}

void Collector::execute_fs_info(void *arg)
{
    std::unique_ptr<std::shared_ptr<on_fs_info>> handler_ptr(
            static_cast<std::shared_ptr<on_fs_info>*>(arg));
    // Collector & self = (*handler_ptr)->get_app().get_collector();
}

void Collector::execute_fs_list_backends(void *arg)
{
    std::unique_ptr<std::shared_ptr<on_fs_list_backends>> handler_ptr(
            static_cast<std::shared_ptr<on_fs_list_backends>*>(arg));
    // Collector & self = (*handler_ptr)->get_app().get_collector();
}

void Collector::execute_group_couple_info(void *arg)
{
    std::unique_ptr<std::shared_ptr<on_group_couple_info>> handler_ptr(
            static_cast<std::shared_ptr<on_group_couple_info>*>(arg));
    // Collector & self = (*handler_ptr)->get_app().get_collector();
}

void Collector::execute_list_namespaces(void *arg)
{
    std::unique_ptr<std::shared_ptr<on_list_namespaces>> handler_ptr(
            static_cast<std::shared_ptr<on_list_namespaces>*>(arg));
    // Collector & self = (*handler_ptr)->get_app().get_collector();
}

void Collector::execute_node_list_backends(void *arg)
{
    std::unique_ptr<std::shared_ptr<on_node_list_backends>> handler_ptr(
            static_cast<std::shared_ptr<on_node_list_backends>*>(arg));
    Collector & self = (*handler_ptr)->get_app().get_collector();

    std::ostringstream ostr;

    Node *node = nullptr;
    const std::string & node_name = (*handler_ptr)->get_node_name();
    if (!self.m_storage.get_node(node_name, node) || node == nullptr) {
        ostr << "Node " << node_name << " does not exist";
        (*handler_ptr)->response()->error(-1, ostr.str());
        (*handler_ptr)->response()->close();
        return;
    }

    std::map<int, Backend> & backends = node->get_backends();
    ostr << "Node has " << backends.size() << " backends\n";
    for (auto it = backends.begin(); it != backends.end(); ++it)
        ostr << "  " << it->second.get_key() << '\n';

    (*handler_ptr)->response()->write(ostr.str());
    (*handler_ptr)->response()->close();
}
