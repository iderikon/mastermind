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

#include "TimestampParser.h"

#include <cstddef>

enum TimestampKey
{
    Timestamp = 2,
    TvSec     = 4,
    TvUsec    = 8
};

static const Parser::Folder timestamp_1[] = {
    { "timestamp", 0, Timestamp },
    { NULL,        0, 0 }
};

static const Parser::Folder timestamp_2[] = {
    { "tv_sec",  Timestamp, TvSec  },
    { "tv_usec", Timestamp, TvUsec },
    { NULL, 0, 0 }
};

static const Parser::Folder *timestamp_folders[] = {
    timestamp_1,
    timestamp_2
};

static const Parser::UIntInfo timestamp_uint_info[] = {
    { Timestamp|TvSec,  SET, offsetof(TimestampParser::TS, sec)  },
    { Timestamp|TvUsec, SET, offsetof(TimestampParser::TS, usec) },
    { 0, 0, 0 }
};

TimestampParser::TimestampParser()
    :
    super(timestamp_folders, sizeof(timestamp_folders)/sizeof(timestamp_folders[0]),
            timestamp_uint_info, (uint8_t *) &m_ts)
{}
