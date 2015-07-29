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

#ifndef __8004357e_4c54_4db0_8553_edf86980616b
#define __8004357e_4c54_4db0_8553_edf86980616b

#include "Config.h"

#include <cocaine/framework/dispatch.hpp>
#include <elliptics/logger.hpp>

class ThreadPool;
class Storage;
class Discovery;
class DiscoveryTimer;

class WorkerApplication
{
public:
    WorkerApplication(cocaine::framework::dispatch_t & d);
    ~WorkerApplication();

    ioremap::elliptics::logger_base & get_logger()
    { return *m_logger; }

    ioremap::elliptics::logger_base & get_elliptics_logger()
    { return *m_elliptics_logger; }

    const Config & get_config() const
    { return m_config; }

    ThreadPool & get_thread_pool()
    { return *m_thread_pool; }

    Storage & get_storage()
    { return *m_storage; }

    Discovery & get_discovery()
    { return *m_discovery; }

    DiscoveryTimer & get_discovery_timer()
    { return *m_discovery_timer; }

private:
    int load_config();

private:
    ioremap::elliptics::logger_base *m_logger;
    ioremap::elliptics::logger_base *m_elliptics_logger;

    Config m_config;

    ThreadPool *m_thread_pool;

    Storage *m_storage;

    Discovery *m_discovery;
    DiscoveryTimer *m_discovery_timer;
};

#endif

