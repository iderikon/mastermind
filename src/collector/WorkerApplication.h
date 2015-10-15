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

    Collector & get_collector()
    { return m_collector; }

private:
    static void load_config();

private:
    Collector m_collector;
};

namespace app
{

ioremap::elliptics::logger_base & logger();
ioremap::elliptics::logger_base & elliptics_logger();

const Config & config();

}

#endif

