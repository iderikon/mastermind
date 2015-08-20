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

#ifndef __b0f837f3_4008_452e_a47a_e7fc060a7974
#define __b0f837f3_4008_452e_a47a_e7fc060a7974

#include "Backend.h"
#include "Parser.h"

#include <functional>

class BackendParser : public Parser
{
    typedef Parser super;

public:
    BackendParser(uint64_t ts_sec, uint64_t ts_usec,
            std::function<void(BackendStat&)> callback);

    const BackendStat & get_stat() const
    { return m_stat; }

public:
    virtual bool EndObject(rapidjson::SizeType nr_members);

private:
    BackendStat m_stat;
    uint64_t m_ts_sec;
    uint64_t m_ts_usec;
    std::function<void(BackendStat&)> m_callback;
};

#endif

