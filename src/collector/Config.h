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

#ifndef __106a1cb9_f3de_4358_86b0_b357cbd4855c
#define __106a1cb9_f3de_4358_86b0_b357cbd4855c

#include <cstdint>
#include <sstream>
#include <string>
#include <vector>

struct Config
{
    struct NodeInfo
    {
        std::string host;
        int port;
        int family;
    };

    Config()
        :
        monitor_port(10025),
        wait_timeout(10),
        forbidden_dht_groups(0),
        forbidden_unmatched_group_total_space(0),
        forbidden_dc_sharing_among_groups(0),
        reserved_space(112742891519),
        node_backend_stat_stale_timeout(120),
        dnet_log_mask(3),
        net_thread_num(3),
        io_thread_num(3),
        nonblocking_io_thread_num(3),
        metadata_connect_timeout_ms(5000)
    {}

    uint64_t monitor_port;
    uint64_t wait_timeout;
    uint64_t forbidden_dht_groups;
    uint64_t forbidden_unmatched_group_total_space;
    uint64_t forbidden_dc_sharing_among_groups;
    uint64_t reserved_space;
    uint64_t node_backend_stat_stale_timeout;
    uint64_t dnet_log_mask;
    uint64_t net_thread_num;
    uint64_t io_thread_num;
    uint64_t nonblocking_io_thread_num;
    std::vector<NodeInfo> nodes;
    std::string metadata_url;
    uint64_t metadata_connect_timeout_ms;
    std::string jobs_db;
    std::string collector_inventory;
    std::string cache_group_path_prefix;

    void print(std::ostringstream & ostr) const
    {
        ostr <<
            "monitor_port: "                          << monitor_port << "\n"
            "wait_timeout: "                          << wait_timeout << "\n"
            "forbidden_dht_groups: "                  << forbidden_dht_groups << "\n"
            "forbidden_unmatched_group_total_space: " << forbidden_unmatched_group_total_space << "\n"
            "forbidden_dc_sharing_among_groups: "     << forbidden_dc_sharing_among_groups << "\n"
            "reserved_space: "                        << reserved_space << "\n"
            "node_backend_stat_stale_timeout: "       << node_backend_stat_stale_timeout << "\n"
            "dnet_log_mask: "                         << dnet_log_mask << "\n"
            "net_thread_num: "                        << net_thread_num << "\n"
            "io_thread_num: "                         << io_thread_num << "\n"
            "nonblocking_io_thread_num: "             << nonblocking_io_thread_num << "\n"
            "metadata_url: "                          << metadata_url << "\n"
            "metadata_connect_timeout_ms: "           << metadata_connect_timeout_ms << "\n"
            "jobs_db: "                               << jobs_db << "\n"
            "collector_inventory: "                   << collector_inventory << "\n"
            "cache_group_path_prefix: "               << cache_group_path_prefix << "\n"
            "nodes:\n";
        for (const NodeInfo & node : nodes)
            ostr << "  " << node.host << ':' << node.port << ':' << node.family << '\n';
    }

    constexpr static const char *config_file        = "/etc/elliptics/mastermind.conf";
    constexpr static const char *log_file           = "/var/log/mastermind/mastermind-collector.log";
    constexpr static const char *elliptics_log_file = "/var/log/mastermind/elliptics-collector.log";
};

#endif

