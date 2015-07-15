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

#include <memory>
#include <utility>

#include <elliptics/logger.hpp>
#include <rapidjson/reader.h>
#include <rapidjson/filereadstream.h>

#include "Storage.h"
#include "ConfigParser.h"
#include "Discovery.h"
#include "DiscoveryTimer.h"
#include "Node.h"

using namespace ioremap;

int parse_config(ConfigParser & parser, elliptics::logger_base *logger)
{
    FILE *f = fopen(CONFIG_FILE, "rb");
    if (f == NULL) {
        BH_LOG(*logger, DNET_LOG_ERROR, "Cannot open " CONFIG_FILE);
        return -1;
    }

    static char buf[65536];
    rapidjson::FileReadStream is(f, buf, sizeof(buf));
    rapidjson::Reader reader;
    reader.Parse(is, parser);

    if (!parser.good()) {
        BH_LOG(*logger, DNET_LOG_ERROR, "Error parsing " CONFIG_FILE);
        return -1;
    }

    return 0;
}

int main()
{
    std::shared_ptr<elliptics::logger_base> logger;
    logger.reset(new elliptics::file_logger(CONFIG_FILE, DNET_LOG_DEBUG));

    ConfigParser parser;
    if (parse_config(parser, logger.get()))
        return 1;

    ThreadPool thread_pool;
    Storage storage(thread_pool, parser.get_config(), logger);

    Discovery discovery(storage, thread_pool, logger);
    if (discovery.init(parser.get_config()))
        return 1;

    DiscoveryTimer timer(discovery, thread_pool, 60);
    if (timer.init() < 0)
        return 1;

    storage.set_discovery_timer(&timer);

    thread_pool.start();
    timer.start();

    for (;;)
        select(0, NULL, NULL, NULL, NULL);

    return 0;
}
