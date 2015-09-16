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

#include "ConfigParser.h"

#include <cstddef>

namespace {

enum ConfigKey
{
    Elliptics                         = 2,
    ForbiddenDhtGroups                = 4,
    ForbiddenUnmatchedGroupTotalSpace = 8,
    ReservedSpace                     = 0x10,
    DnetLogMask                       = 0x20,
    NetThreadNum                      = 0x40,
    IoThreadNum                       = 0x80,
    NonblockingIoThreadNum            = 0x100,
    Nodes                             = 0x200,
    MonitorPort                       = 0x400,
    WaitTimeout                       = 0x800,
    Metadata                          = 0x1000,
    Url                               = 0x2000,
    Jobs                              = 0x4000,
    Db                                = 0x8000,
    Options                           = 0x10000,
    ConnectTimeoutMS                  = 0x20000
};

Parser::Folder config_1[] = {
    { "elliptics",                             0, Elliptics                         },
    { "forbidden_dht_groups",                  0, ForbiddenDhtGroups                },
    { "forbidden_unmatched_group_total_space", 0, ForbiddenUnmatchedGroupTotalSpace },
    { "reserved_space",                        0, ReservedSpace                     },
    { "dnet_log_mask",                         0, DnetLogMask                       },
    { "net_thread_num",                        0, NetThreadNum                      },
    { "io_thread_num",                         0, IoThreadNum                       },
    { "nonblocking_io_thread_num",             0, NonblockingIoThreadNum            },
    { "metadata",                              0, Metadata                          },
    { NULL, 0, 0 }
};

Parser::Folder config_2[] = {
    { "nodes",        Elliptics, Nodes       },
    { "monitor_port", Elliptics, MonitorPort },
    { "wait_timeout", Elliptics, WaitTimeout },
    { "url",          Metadata,  Url         },
    { "jobs",         Metadata,  Jobs        },
    { "options",      Metadata,  Options     },
    { NULL, 0, 0 }
};

Parser::Folder config_3[] = {
    { "db",               Metadata|Jobs,    Db               },
    { "connectTimeoutMS", Metadata|Options, ConnectTimeoutMS },
    { NULL, 0, 0 }
};

Parser::Folder *config_folders[] = {
    config_1,
    config_2,
    config_3
};

Parser::UIntInfo config_uint_info[] = {
    { Elliptics|MonitorPort,             SET, offsetof(Config, monitor_port)                          },
    { Elliptics|WaitTimeout,             SET, offsetof(Config, wait_timeout)                          },
    { ForbiddenDhtGroups,                SET, offsetof(Config, forbidden_dht_groups)                  },
    { ForbiddenUnmatchedGroupTotalSpace, SET, offsetof(Config, forbidden_unmatched_group_total_space) },
    { ReservedSpace,                     SET, offsetof(Config, reserved_space)                        },
    { DnetLogMask,                       SET, offsetof(Config, dnet_log_mask)                         },
    { NetThreadNum,                      SET, offsetof(Config, net_thread_num)                        },
    { IoThreadNum,                       SET, offsetof(Config, io_thread_num)                         },
    { NonblockingIoThreadNum,            SET, offsetof(Config, nonblocking_io_thread_num)             },
    { Metadata|Options|ConnectTimeoutMS, SET, offsetof(Config, metadata_connect_timeout_ms)           },
    { 0, 0, 0 }
};

Parser::StringInfo config_string_info[] = {
    { Metadata|Url,     offsetof(Config, metadata_url) },
    { Metadata|Jobs|Db, offsetof(Config, jobs_db)      },
    { 0, 0 }
};

} // unnamed namespace

ConfigParser::ConfigParser(Config & config)
    :
    super(config_folders, sizeof(config_folders)/sizeof(config_folders[0]),
            config_uint_info, config_string_info, (uint8_t *) &config),
    m_array_depth(0),
    m_config(config)
{
    m_current_node.port = -1;
    m_current_node.family = -1;
}

bool ConfigParser::String(const char* str, rapidjson::SizeType length, bool copy)
{
    if (m_keys == (Elliptics|Nodes|1) && m_array_depth == 2) {
        m_current_node.host = std::string(str, length);
        return true;
    }

    return super::String(str, length, copy);
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
