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
#include "Node.h"
#include "Parser.h"

class StatsParser : public Parser
{
    typedef Parser super;

public:
    struct Data {
        BackendStat backend;
        NodeStat node;
    };

    StatsParser();

    std::vector<BackendStat> & get_backend_stats()
    { return m_backend_stats; }

    NodeStat & get_node_stat()
    { return m_data.node; }

public:
    virtual bool EndObject(rapidjson::SizeType nr_members);

private:
    Data m_data;
    std::vector<BackendStat> m_backend_stats;
};

#endif

