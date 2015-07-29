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

#include "BackendParser.h"

#include <cstddef>

enum BackendKey
{
    Backends           = 2,
    BackendFolder      = 4,
    Backend            = 8,
    BackendId          = 0x10,
    Status             = 0x20,
    Dstat              = 0x40,
    Vfs                = 0x80,
    ReadIos            = 0x100,
    WriteIos           = 0x200,
    Error              = 0x400,
    Blocks             = 0x800,
    Bavail             = 0x1000,
    Bsize              = 0x2000,
    Fsid               = 0x4000,
    SummaryStats       = 0x8000,
    RecordsTotal       = 0x10000,
    RecordsRemoved     = 0x20000,
    RecordsRemovedSize = 0x40000,
    WantDefrag         = 0x80000,
    BaseSize           = 0x100000,
    Config             = 0x200000,
    BlobSizeLimit      = 0x400000,
    BlobSize           = 0x800000,
    BaseStats          = 0x1000000,
    BlobFilename       = 0x2000000,
    BlobBaseSize       = 0x4000000,
    DefragState        = 0x8000000,
    State              = 0x10000000,
    Group              = 0x20000000
};

static const Parser::Folder backend_1[] = {
    { "backends", 0, Backends },

    { NULL, 0, 0 }
};

static const Parser::Folder backend_2[] = {
    { MATCH_ANY, Backends, BackendFolder },

    { NULL, 0, 0 }
};

static const Parser::Folder backend_3[] = {
    { "backend",    Backends|BackendFolder, Backend   },
    { "backend_id", Backends|BackendFolder, BackendId },
    { "status",     Backends|BackendFolder, Status    },

    { NULL, 0, 0 }
};

static const Parser::Folder backend_4[] = {
    { "dstat",         Backends|BackendFolder|Backend, Dstat        },
    { "vfs",           Backends|BackendFolder|Backend, Vfs          },
    { "summary_stats", Backends|BackendFolder|Backend, SummaryStats },
    { "config",        Backends|BackendFolder|Backend, Config       },
    { "base_stats",    Backends|BackendFolder|Backend, BaseStats    },

    { "defrag_state",  Backends|BackendFolder|Status,  DefragState  },
    { "state",         Backends|BackendFolder|Status,  State        },

    { NULL, 0, 0 }
};

static const Parser::Folder backend_5[] = {
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

    { NULL, 0, 0 }
};

static const Parser::Folder backend_6[] = {
    { "base_size", Backends|BackendFolder|Backend|BaseStats|BlobFilename, BlobBaseSize },

    { NULL, 0, 0 }
};

static const Parser::Folder *backend_folders[] = {
    backend_1,
    backend_2,
    backend_3,
    backend_4,
    backend_5,
    backend_6
};

#define OFF(field) offsetof(BackendStat, field)

static const Parser::UIntInfo backend_uint_info[] = {
    { Backends|BackendFolder|BackendId,                                   SET, OFF(backend_id)           },
    { Backends|BackendFolder|Backend|Dstat|ReadIos,                       SET, OFF(read_ios)             },
    { Backends|BackendFolder|Backend|Dstat|WriteIos,                      SET, OFF(write_ios)            },
    { Backends|BackendFolder|Backend|Dstat|Error,                         SET, OFF(error)                },
    { Backends|BackendFolder|Backend|Vfs|Blocks,                          SET, OFF(vfs_blocks)           },
    { Backends|BackendFolder|Backend|Vfs|Bavail,                          SET, OFF(vfs_bavail)           },
    { Backends|BackendFolder|Backend|Vfs|Bsize,                           SET, OFF(vfs_bsize)            },
    { Backends|BackendFolder|Backend|Vfs|Fsid,                            SET, OFF(fsid)                 },
    { Backends|BackendFolder|Backend|SummaryStats|RecordsTotal,           SET, OFF(records_total)        },
    { Backends|BackendFolder|Backend|SummaryStats|RecordsRemoved,         SET, OFF(records_removed)      },
    { Backends|BackendFolder|Backend|SummaryStats|RecordsRemovedSize,     SET, OFF(records_removed_size) },
    { Backends|BackendFolder|Backend|SummaryStats|WantDefrag,             SET, OFF(want_defrag)          },
    { Backends|BackendFolder|Backend|SummaryStats|BaseSize,               SET, OFF(base_size)            },
    { Backends|BackendFolder|Backend|Config|BlobSizeLimit,                SET, OFF(blob_size_limit)      },
    { Backends|BackendFolder|Backend|Config|BlobSize,                     SET, OFF(blob_size)            },
    { Backends|BackendFolder|Backend|Config|Group,                        SET, OFF(group)                },
    { Backends|BackendFolder|Backend|BaseStats|BlobFilename|BlobBaseSize, MAX, OFF(max_blob_base_size)   },
    { Backends|BackendFolder|Status|DefragState,                          SET, OFF(defrag_state)         },
    { Backends|BackendFolder|Status|State,                                SET, OFF(state)                },
    { 0, 0, 0 }
};

BackendParser::BackendParser(uint64_t ts_sec, uint64_t ts_usec, Node & node)
    :
    super(backend_folders, sizeof(backend_folders)/sizeof(backend_folders[0]),
            backend_uint_info, (uint8_t *) &m_stat),
    m_ts_sec(ts_sec),
    m_ts_usec(ts_usec),
    m_node(node)
{}

bool BackendParser::EndObject(rapidjson::SizeType nr_members)
{
    if (m_keys == (Backends|BackendFolder|1) && m_depth == 3) {
        m_stat.ts_sec = m_ts_sec;
        m_stat.ts_usec = m_ts_usec;
        m_node.handle_backend(m_stat);
        std::memset(&m_stat, 0, sizeof(m_stat));
    }

    return super::EndObject(nr_members);
}
