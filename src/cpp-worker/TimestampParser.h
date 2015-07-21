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

#ifndef __221b4370_2518_4a22_8352_9091ad9aa553
#define __221b4370_2518_4a22_8352_9091ad9aa553

#include "Parser.h"

#include <ctime>
#include <string>

class TimestampParser : public Parser
{
    typedef Parser super;

public:
    TimestampParser();

    size_t get_ts_sec() const
    { return m_ts.sec; }

    size_t get_ts_usec() const
    { return m_ts.usec; }

    static std::string ts_user_friendly(time_t sec, int usec);

public:
    struct TS {
        uint64_t sec;
        uint64_t usec;
    };

private:
    TS m_ts;
};

#endif

