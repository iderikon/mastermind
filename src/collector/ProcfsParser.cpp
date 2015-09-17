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

#include "ProcfsParser.h"

#include <cstddef>

namespace {

enum ProcfsKey
{
    Timestamp        = 2,
    TvSec            = 4,
    TvUsec           = 8,
    Procfs           = 0x10,
    Vm               = 0x20,
    La               = 0x40,
    Net              = 0x80,
    NetInterfaces    = 0x100,
    NetInterfaceName = 0x200,
    Receive          = 0x400,
    Transmit         = 0x800,
    Bytes            = 0x1000
};

std::vector<Parser::FolderVector> procfs_folders = {
    {
        { "timestamp", 0, Timestamp },
        { "procfs",    0, Procfs    }
    },
    {
        { "tv_sec",  Timestamp, TvSec  },
        { "tv_usec", Timestamp, TvUsec },

        { "vm",      Procfs,    Vm     },
        { "net",     Procfs,    Net    }
    },
    {
        { "la",             Procfs|Vm,  La            },
        { "net_interfaces", Procfs|Net, NetInterfaces }
    },
    {
        { NOT_MATCH "lo", Procfs|Net|NetInterfaces, NetInterfaceName }
    },
    {
        { "receive",  Procfs|Net|NetInterfaces|NetInterfaceName, Receive  },
        { "transmit", Procfs|Net|NetInterfaces|NetInterfaceName, Transmit }
    },
    {
        { "bytes", Procfs|Net|NetInterfaces|NetInterfaceName|Receive,  Bytes },
        { "bytes", Procfs|Net|NetInterfaces|NetInterfaceName|Transmit, Bytes }
    }
};

Parser::UIntInfoVector procfs_uint_info = {
    { Timestamp|TvSec,                                          SET, offsetof(NodeStat, ts_sec)   },
    { Timestamp|TvUsec,                                         SET, offsetof(NodeStat, ts_usec)  },
    { Procfs|Vm|La,                                             SET, offsetof(NodeStat, la1)      },
    { Procfs|Net|NetInterfaces|NetInterfaceName|Receive|Bytes,  SUM, offsetof(NodeStat, rx_bytes) },
    { Procfs|Net|NetInterfaces|NetInterfaceName|Transmit|Bytes, SUM, offsetof(NodeStat, tx_bytes) }
};

Parser::StringInfoVector procfs_string_info;

} // unnamed namespace

ProcfsParser::ProcfsParser()
    :
    super(procfs_folders, procfs_uint_info, procfs_string_info, (uint8_t *) &m_stat)
{}
