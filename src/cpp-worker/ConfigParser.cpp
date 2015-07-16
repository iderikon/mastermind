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

#include "ConfigParser.h"

#include <cstddef>

enum ConfigKey
{
    Elliptics          = 2,
    ForbiddenDhtGroups = 4,
    Nodes              = 8,
    MonitorPort        = 0x10
};

static const Parser::Folder config_1[] = {
    { "elliptics",            0, Elliptics          },
    { "forbidden_dht_groups", 0, ForbiddenDhtGroups },
    { NULL, 0, 0 }
};

static const Parser::Folder config_2[] = {
    { "nodes",        Elliptics, Nodes },
    { "monitor_port", Elliptics, MonitorPort },
    { NULL, 0, 0 }
};

static const Parser::Folder * const config_folders[] = {
    config_1,
    config_2
};

static const Parser::UIntInfo config_uint_info[] = {
    { Elliptics|MonitorPort, SET, offsetof(Config, monitor_port)         },
    { ForbiddenDhtGroups,    SET, offsetof(Config, forbidden_dht_groups) },
    { 0, 0, 0 }
};

ConfigParser::ConfigParser(Config & config)
    :
    super(config_folders, sizeof(config_folders)/sizeof(config_folders[0]),
            config_uint_info, (uint8_t *) &config),
    m_array_depth(0),
    m_config(config)
{
    m_current_node.port = -1;
    m_current_node.family = -1;
}

bool ConfigParser::String(const char* str, rapidjson::SizeType length, bool copy)
{
    if (m_keys == (Elliptics|Nodes|1) && m_array_depth == 2)
        m_current_node.host = std::string(str, length);
    return true;
}

bool ConfigParser::StartArray()
{
    ++m_array_depth;
    return true;
}

bool ConfigParser::UInteger(uint64_t val)
{
    if (m_keys == (Elliptics|Nodes|1) && m_array_depth == 2) {
        if (m_current_node.port < 0)
            m_current_node.port = int(val);
        else
            m_current_node.family = int(val);
    } else {
        return super::UInteger(val);
    }
    return true;
}

bool ConfigParser::EndArray(rapidjson::SizeType nr_elements)
{
    if (m_keys == (Elliptics|Nodes|1)) {
        if (m_array_depth == 2) {
            m_config.nodes.push_back(m_current_node);

            m_current_node.host.clear();
            m_current_node.port = -1;
            m_current_node.family = -1;
        } else if (m_array_depth == 1) {
            clear_key();
        }
    }

    --m_array_depth;
    return true;
}
