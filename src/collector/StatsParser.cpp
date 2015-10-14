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

#include "StatsParser.h"

#include <cstddef>

namespace {

const uint64_t Backends           = 2ULL;
const uint64_t BackendFolder      = 4ULL;
const uint64_t Backend            = 8ULL;
const uint64_t BackendId          = 0x10ULL;
const uint64_t Status             = 0x20ULL;
const uint64_t Dstat              = 0x40ULL;
const uint64_t Vfs                = 0x80ULL;
const uint64_t ReadIos            = 0x100ULL;
const uint64_t WriteIos           = 0x200ULL;
const uint64_t Error              = 0x400ULL;
const uint64_t Blocks             = 0x800ULL;
const uint64_t Bavail             = 0x1000ULL;
const uint64_t Bsize              = 0x2000ULL;
const uint64_t Fsid               = 0x4000ULL;
const uint64_t SummaryStats       = 0x8000ULL;
const uint64_t RecordsTotal       = 0x10000ULL;
const uint64_t RecordsRemoved     = 0x20000ULL;
const uint64_t RecordsRemovedSize = 0x40000ULL;
const uint64_t WantDefrag         = 0x80000ULL;
const uint64_t BaseSize           = 0x100000ULL;
const uint64_t Config             = 0x200000ULL;
const uint64_t BlobSizeLimit      = 0x400000ULL;
const uint64_t BlobSize           = 0x800000ULL;
const uint64_t BaseStats          = 0x1000000ULL;
const uint64_t BlobFilename       = 0x2000000ULL;
const uint64_t BlobBaseSize       = 0x4000000ULL;
const uint64_t DefragState        = 0x8000000ULL;
const uint64_t State              = 0x10000000ULL;
const uint64_t Group              = 0x20000000ULL;
const uint64_t Timestamp          = 0x40000000ULL;
const uint64_t TvSec              = 0x80000000ULL;
const uint64_t TvUsec             = 0x100000000ULL;
const uint64_t Procfs             = 0x200000000ULL;
const uint64_t Vm                 = 0x400000000ULL;
const uint64_t La                 = 0x800000000ULL;
const uint64_t Net                = 0x1000000000ULL;
const uint64_t NetInterfaces      = 0x2000000000ULL;
const uint64_t NetInterfaceName   = 0x4000000000ULL;
const uint64_t Receive            = 0x8000000000ULL;
const uint64_t Transmit           = 0x10000000000ULL;
const uint64_t Bytes              = 0x20000000000ULL;

std::vector<Parser::FolderVector> backend_folders = {
    {
        { "backends",  0, Backends  },
        { "timestamp", 0, Timestamp },
        { "procfs",    0, Procfs    }
    },
    {
        { MATCH_ANY, Backends,  BackendFolder },
        { "tv_sec",  Timestamp, TvSec         },
        { "tv_usec", Timestamp, TvUsec        },
        { "vm",      Procfs,    Vm            },
        { "net",     Procfs,    Net           }
    },
    {
        { "backend",        Backends|BackendFolder, Backend       },
        { "backend_id",     Backends|BackendFolder, BackendId     },
        { "status",         Backends|BackendFolder, Status        },
        { "la",             Procfs|Vm,              La            },
        { "net_interfaces", Procfs|Net,             NetInterfaces }
    },
    {
        { "dstat",         Backends|BackendFolder|Backend, Dstat            },
        { "vfs",           Backends|BackendFolder|Backend, Vfs              },
        { "summary_stats", Backends|BackendFolder|Backend, SummaryStats     },
        { "config",        Backends|BackendFolder|Backend, Config           },
        { "base_stats",    Backends|BackendFolder|Backend, BaseStats        },
        { "defrag_state",  Backends|BackendFolder|Status,  DefragState      },
        { "state",         Backends|BackendFolder|Status,  State            },
        { NOT_MATCH "lo",  Procfs|Net|NetInterfaces,       NetInterfaceName }
    },
    {
        { "read_ios",             Backends|BackendFolder|Backend|Dstat,        ReadIos            },
        { "write_ios",            Backends|BackendFolder|Backend|Dstat,        WriteIos           },
        { "error",                Backends|BackendFolder|Backend|Dstat,        Error              },
        { "blocks",               Backends|BackendFolder|Backend|Vfs,          Blocks             },
        { "bavail",               Backends|BackendFolder|Backend|Vfs,          Bavail             },
        { "bsize",                Backends|BackendFolder|Backend|Vfs,          Bsize              },
        { "fsid",                 Backends|BackendFolder|Backend|Vfs,          Fsid               },
        { "records_total",        Backends|BackendFolder|Backend|SummaryStats, RecordsTotal       },
        { "records_removed",      Backends|BackendFolder|Backend|SummaryStats, RecordsRemoved     },
        { "records_removed_size", Backends|BackendFolder|Backend|SummaryStats, RecordsRemovedSize },
        { "want_defrag",          Backends|BackendFolder|Backend|SummaryStats, WantDefrag         },
        { "base_size",            Backends|BackendFolder|Backend|SummaryStats, BaseSize           },
        { "blob_size_limit",      Backends|BackendFolder|Backend|Config,       BlobSizeLimit      },
        { "blob_size",            Backends|BackendFolder|Backend|Config,       BlobSize           },
        { "group",                Backends|BackendFolder|Backend|Config,       Group              },
        { MATCH_ANY,              Backends|BackendFolder|Backend|BaseStats,    BlobFilename       },
        { "receive",              Procfs|Net|NetInterfaces|NetInterfaceName,   Receive            },
        { "transmit",             Procfs|Net|NetInterfaces|NetInterfaceName,   Transmit           }
    },
    {
        { "base_size", Backends|BackendFolder|Backend|BaseStats|BlobFilename, BlobBaseSize },
        { "bytes",     Procfs|Net|NetInterfaces|NetInterfaceName|Receive,     Bytes        },
        { "bytes",     Procfs|Net|NetInterfaces|NetInterfaceName|Transmit,    Bytes        }
    }
};

#define BOFF(field) offsetof(StatsParser::Data, backend.field)
#define NOFF(field) offsetof(StatsParser::Data, node.field)

Parser::UIntInfoVector backend_uint_info = {
    { Backends|BackendFolder|BackendId,                                   SET, BOFF(backend_id)           },
    { Backends|BackendFolder|Backend|Dstat|ReadIos,                       SET, BOFF(read_ios)             },
    { Backends|BackendFolder|Backend|Dstat|WriteIos,                      SET, BOFF(write_ios)            },
    { Backends|BackendFolder|Backend|Dstat|Error,                         SET, BOFF(error)                },
    { Backends|BackendFolder|Backend|Vfs|Blocks,                          SET, BOFF(vfs_blocks)           },
    { Backends|BackendFolder|Backend|Vfs|Bavail,                          SET, BOFF(vfs_bavail)           },
    { Backends|BackendFolder|Backend|Vfs|Bsize,                           SET, BOFF(vfs_bsize)            },
    { Backends|BackendFolder|Backend|Vfs|Fsid,                            SET, BOFF(fsid)                 },
    { Backends|BackendFolder|Backend|SummaryStats|RecordsTotal,           SET, BOFF(records_total)        },
    { Backends|BackendFolder|Backend|SummaryStats|RecordsRemoved,         SET, BOFF(records_removed)      },
    { Backends|BackendFolder|Backend|SummaryStats|RecordsRemovedSize,     SET, BOFF(records_removed_size) },
    { Backends|BackendFolder|Backend|SummaryStats|WantDefrag,             SET, BOFF(want_defrag)          },
    { Backends|BackendFolder|Backend|SummaryStats|BaseSize,               SET, BOFF(base_size)            },
    { Backends|BackendFolder|Backend|Config|BlobSizeLimit,                SET, BOFF(blob_size_limit)      },
    { Backends|BackendFolder|Backend|Config|BlobSize,                     SET, BOFF(blob_size)            },
    { Backends|BackendFolder|Backend|Config|Group,                        SET, BOFF(group)                },
    { Backends|BackendFolder|Backend|BaseStats|BlobFilename|BlobBaseSize, MAX, BOFF(max_blob_base_size)   },
    { Backends|BackendFolder|Status|DefragState,                          SET, BOFF(defrag_state)         },
    { Backends|BackendFolder|Status|State,                                SET, BOFF(state)                },
    { Timestamp|TvSec,                                                    SET, NOFF(ts_sec)               },
    { Timestamp|TvUsec,                                                   SET, NOFF(ts_usec)              },
    { Procfs|Vm|La,                                                       SET, NOFF(la1)                  },
    { Procfs|Net|NetInterfaces|NetInterfaceName|Receive|Bytes,            SUM, NOFF(rx_bytes)             },
    { Procfs|Net|NetInterfaces|NetInterfaceName|Transmit|Bytes,           SUM, NOFF(tx_bytes)             }
};

Parser::StringInfoVector backend_string_info;

} // unnamed namespace

StatsParser::StatsParser()
    :
    super(backend_folders, backend_uint_info, backend_string_info, (uint8_t *) &m_data)
{}

bool StatsParser::EndObject(rapidjson::SizeType nr_members)
{
    if (m_keys == (Backends|BackendFolder|1) && m_depth == 3) {
        m_backend_stats.push_back(m_data.backend);
        std::memset(&m_data.backend, 0, sizeof(m_data.backend));
    }

    return super::EndObject(nr_members);
}
