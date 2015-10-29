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
#include <ostream>
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
        reserved_space(112742891519),
        node_backend_stat_stale_timeout(120),
        dnet_log_mask(3),
        net_thread_num(3),
        io_thread_num(3),
        nonblocking_io_thread_num(3)
    {
        metadata.options.connectTimeoutMS = 5000;
    }

    uint64_t monitor_port;
    uint64_t wait_timeout;
    uint64_t forbidden_dht_groups;
    uint64_t forbidden_unmatched_group_total_space;
    uint64_t reserved_space;
    uint64_t node_backend_stat_stale_timeout;
    uint64_t dnet_log_mask;
    uint64_t net_thread_num;
    uint64_t io_thread_num;
    uint64_t nonblocking_io_thread_num;
    std::string cache_group_path_prefix;
    std::vector<NodeInfo> nodes;

    struct {
        std::string url;
        struct {
            uint64_t connectTimeoutMS;
        } options;
        struct {
            std::string db;
        } jobs;
    } metadata;

    constexpr static const char *config_file        = "/etc/elliptics/mastermind.conf";
    constexpr static const char *log_file           = "/var/log/mastermind/mastermind-collector.log";
    constexpr static const char *elliptics_log_file = "/var/log/mastermind/elliptics-collector.log";
};

inline std::ostream & operator << (std::ostream & ostr, const Config & config)
{
    ostr <<
        "monitor_port: "                          << config.monitor_port << "\n"
        "wait_timeout: "                          << config.wait_timeout << "\n"
        "forbidden_dht_groups: "                  << config.forbidden_dht_groups << "\n"
        "forbidden_unmatched_group_total_space: " << config.forbidden_unmatched_group_total_space << "\n"
        "reserved_space: "                        << config.reserved_space << "\n"
        "node_backend_stat_stale_timeout: "       << config.node_backend_stat_stale_timeout << "\n"
        "dnet_log_mask: "                         << config.dnet_log_mask << "\n"
        "net_thread_num: "                        << config.net_thread_num << "\n"
        "io_thread_num: "                         << config.io_thread_num << "\n"
        "nonblocking_io_thread_num: "             << config.nonblocking_io_thread_num << "\n"
        "metadata: {\n"
        "  url: "                                 << config.metadata.url << "\n"
        "  options: {\n"
        "    metadata_connect_timeout_ms: "       << config.metadata.options.connectTimeoutMS << "\n"
        "  }\n"
        "  jobs: {\n"
        "    db: "                                << config.metadata.jobs.db << "\n"
        "  }\n"
        "}\n"
        "cache_group_path_prefix: "               << config.cache_group_path_prefix << "\n"
        "nodes:\n";
    for (const Config::NodeInfo & node : config.nodes)
        ostr << "  " << node.host << ':' << node.port << ':' << node.family << '\n';
    return ostr;
}

#endif

