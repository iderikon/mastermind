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

#include "Config.h"
#include "Discovery.h"
#include "Round.h"
#include "WorkerApplication.h"

#include <curl/curl.h>
#include <mongo/client/dbclient.h>

#include <cstring>
#include <set>

using namespace ioremap;

namespace {

struct dnet_addr_compare
{
    bool operator () (const dnet_addr & a, const dnet_addr & b) const
    {
        return memcmp(&a, &b, sizeof(dnet_addr)) < 0;
    }
};

} // unnamed namespace

Discovery::Discovery(WorkerApplication & app)
    :
    m_app(app)
{}

Discovery::~Discovery()
{}

int Discovery::init_curl()
{
    if (curl_global_init(CURL_GLOBAL_ALL)) {
        BH_LOG(m_app.get_logger(), DNET_LOG_ERROR, "Failed to initialize libcurl");
        return -1;
    }
    return 0;
}

int Discovery::init_elliptics()
{
    const Config & config = m_app.get_config();

    dnet_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.wait_timeout = config.wait_timeout;
    cfg.net_thread_num = config.net_thread_num;
    cfg.io_thread_num = config.io_thread_num;
    cfg.nonblocking_io_thread_num = config.nonblocking_io_thread_num;

    m_node.reset(new elliptics::node(
                elliptics::logger(m_app.get_elliptics_logger(), blackhole::log::attributes_t()), cfg));

    BH_LOG(m_app.get_logger(), DNET_LOG_NOTICE, "Initializing discovery");

    for (size_t i = 0; i < config.nodes.size(); ++i) {
        const Config::NodeInfo & info = config.nodes[i];

        try {
            m_node->add_remote(elliptics::address(info.host, info.port, info.family));
        } catch (std::exception & e) {
            BH_LOG(m_app.get_logger(), DNET_LOG_WARNING, "Failed to add remote '%s': %s\n", info.host, e.what());
            continue;
        } catch (...) {
            BH_LOG(m_app.get_logger(), DNET_LOG_WARNING, "Failed to add remote '%s' with unknown reason", info.host);
            continue;
        }
    }

    m_session.reset(new elliptics::session(*m_node));
    m_session->set_cflags(DNET_FLAGS_NOLOCK);

    return 0;
}

int Discovery::init_mongo()
{
    mongo::Status status = mongo::client::initialize(mongo::client::Options().setIPv6Enabled(true));
    if (!status.isOK()) {
        BH_LOG(m_app.get_logger(), DNET_LOG_ERROR,
                "Failed to initialize mongo client: %s", status.toString().c_str());
        return -1;
    }

    return 0;
}

void Discovery::resolve_nodes(Round & round)
{
    if (m_session == NULL) {
        BH_LOG(m_app.get_logger(), DNET_LOG_WARNING, "resolve_nodes: session is empty");
        return;
    }

    std::set<dnet_addr, dnet_addr_compare> addresses;

    std::vector<dnet_route_entry> routes;
    routes = m_session->get_routes();

    for (size_t i = 0; i < routes.size(); ++i)
        addresses.insert(routes[i].addr);

    for (auto it = addresses.begin(); it != addresses.end(); ++it) {
        const dnet_addr & addr = *it;

        const char *host = dnet_addr_host_string(&addr);
        int port = dnet_addr_port(&addr);

        round.add_node(host, port, addr.family);
    }
}

void Discovery::stop_mongo()
{
    mongo::client::shutdown();
}

void Discovery::stop_elliptics()
{
    m_session.reset();
    m_node.reset();
}

void Discovery::stop_curl()
{
    curl_global_cleanup();
}
