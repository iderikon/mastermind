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

WorkerApplication::WorkerApplication()
    :
    m_logger(NULL),
    m_elliptics_logger(NULL),
    m_storage(*this),
    m_discovery(*this),
    m_discovery_timer(*this, 60)
{}

WorkerApplication::WorkerApplication(cocaine::framework::dispatch_t & d)
    :
    m_storage(*this),
    m_discovery(*this),
    m_discovery_timer(*this, 60)
{
    init();

    d.on<on_summary>("summary", *this);
    d.on<on_group_info>("group_info", *this);
    d.on<on_list_nodes>("list_nodes", *this);
    d.on<on_node_info>("node_info", *this);
    d.on<on_node_list_backends>("node_list_backends", *this);
    d.on<on_backend_info>("backend_info", *this);
    d.on<on_fs_info>("fs_info", *this);
    d.on<on_fs_list_backends>("fs_list_backends", *this);
    d.on<on_list_namespaces>("list_namespaces", *this);
    d.on<on_group_couple_info>("group_couple_info", *this);

    d.on<on_force_update>("force_update", *this);
    d.on<on_get_snapshot>("get_snapshot", *this);
    d.on<on_refresh>("refresh", *this);

    start();
}

WorkerApplication::~WorkerApplication()
{
    delete m_elliptics_logger;
    delete m_logger;
}

void WorkerApplication::init()
{
    load_config();

    m_logger = new ioremap::elliptics::file_logger(
            LOG_FILE, ioremap::elliptics::log_level(m_config.dnet_log_mask));
    if (!m_logger)
        throw worker_error("failed to open log file " LOG_FILE);

    m_elliptics_logger = new ioremap::elliptics::file_logger(
            ELLIPTICS_LOG_FILE, ioremap::elliptics::log_level(m_config.dnet_log_mask));
    if (!m_elliptics_logger)
        throw worker_error("failed to open log file " ELLIPTICS_LOG_FILE);

    if (m_discovery.init())
        throw worker_error("failed to initialize discovery");

    if (m_discovery_timer.init())
        throw worker_error("failed to start discovery timer");

}

void WorkerApplication::start()
{
    m_thread_pool.start();
    m_discovery_timer.start();
}

int WorkerApplication::load_config()
{
    ConfigParser parser(m_config);

    FILE *f = fopen(CONFIG_FILE, "rb");
    if (f == NULL)
        throw worker_error("Cannot open " CONFIG_FILE);

    static char buf[65536];
    rapidjson::FileReadStream is(f, buf, sizeof(buf));
    rapidjson::Reader reader;
    reader.Parse(is, parser);

    if (!parser.good())
        throw worker_error("Error parsing " CONFIG_FILE);

    if (m_config.reserved_space == 0)
        throw worker_error("Incorrect value 0 for reserved_space");

    return 0;
}
