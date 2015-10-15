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

#ifndef __8004357e_4c54_4db0_8553_edf86980616b
#define __8004357e_4c54_4db0_8553_edf86980616b

#include "Collector.h"
#include "Config.h"
#include "Inventory.h"

#include <cocaine/framework/dispatch.hpp>
#include <elliptics/logger.hpp>

#include <memory>

class WorkerApplication
{
public:
    WorkerApplication();
    WorkerApplication(cocaine::framework::dispatch_t & d);

    void init();
    void start();

    ioremap::elliptics::logger_base & get_logger()
    { return *m_logger; }

    void set_logger(ioremap::elliptics::logger_base *logger)
    { m_logger.reset(logger); }

    ioremap::elliptics::logger_base & get_elliptics_logger()
    { return *m_elliptics_logger; }

    Config & get_config()
    { return m_config; }

    Collector & get_collector()
    { return m_collector; }

    Inventory & get_inventory()
    { return m_inventory; }

private:
    void load_config();

private:
    std::unique_ptr<ioremap::elliptics::logger_base> m_logger;
    std::unique_ptr<ioremap::elliptics::logger_base> m_elliptics_logger;

    Config m_config;
    Collector m_collector;
    Inventory m_inventory;
};

#endif

