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

#include "Storage.h"
#include "CocaineHandlers.h"
#include "ConfigParser.h"
#include "Discovery.h"
#include "DiscoveryTimer.h"
#include "WorkerApplication.h"

#include <elliptics/logger.hpp>
#include <rapidjson/reader.h>
#include <rapidjson/filereadstream.h>

#include <cstdio>
#include <exception>

namespace {

class worker_error : public std::exception
{
public:
    worker_error(const char *text)
        : m_text(text)
    {}

    virtual const char *what() const throw()
    { return m_text; }

private:
    const char *m_text;
};

} // unnamed namespace

WorkerApplication::WorkerApplication(cocaine::framework::dispatch_t & d)
{
    m_thread_pool = new ThreadPool;
    m_storage = new Storage(*this);
    m_discovery = new Discovery(*this);
    m_discovery_timer = new DiscoveryTimer(*this, 60);

    m_logger = new ioremap::elliptics::file_logger(LOG_FILE, DNET_LOG_DEBUG);
    if (!m_logger)
        throw worker_error("failed to open log file " LOG_FILE);

    if (load_config())
        throw worker_error("failed to load config");

    if (m_discovery->init())
        throw worker_error("failed to initialize discovery");

    if (m_discovery_timer->init())
        throw worker_error("failed to start discovery timer");

    d.on<on_summary>("summary", *this);
    d.on<on_group_info>("group_info", *this);
    d.on<on_list_nodes>("list_nodes", *this);
    d.on<on_node_info>("node_info", *this);
    d.on<on_node_list_backends>("node_list_backends", *this);
    d.on<on_backend_info>("backend_info", *this);
    d.on<on_fs_info>("fs_info", *this);
    d.on<on_fs_list_backends>("fs_list_backends", *this);
    d.on<on_list_namespaces>("list_namespaces", *this);

    m_thread_pool->start();
    m_discovery_timer->start();
}

WorkerApplication::~WorkerApplication()
{
    delete m_discovery_timer;
    delete m_discovery;
    delete m_storage;
    delete m_thread_pool;
    delete m_logger;
}

int WorkerApplication::load_config()
{
    ConfigParser parser(m_config);

    FILE *f = fopen(CONFIG_FILE, "rb");
    if (f == NULL) {
        BH_LOG(*m_logger, DNET_LOG_ERROR, "Cannot open " CONFIG_FILE);
        return -1;
    }

    static char buf[65536];
    rapidjson::FileReadStream is(f, buf, sizeof(buf));
    rapidjson::Reader reader;
    reader.Parse(is, parser);

    if (!parser.good()) {
        BH_LOG(*m_logger, DNET_LOG_ERROR, "Error parsing " CONFIG_FILE);
        return -1;
    }

    return 0;
}
