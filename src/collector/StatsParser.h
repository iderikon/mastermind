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

#include <map>

class StatsParser : public Parser
{
    typedef Parser super;

public:
    struct Data {
        Data()
            : stat_commit()
        {}

        // backend is a currently processing object.
        // It will be put into m_backend_stats.
        BackendStat backend;
        NodeStat node;
        struct {
            unsigned int backend;
            unsigned int err;
            uint64_t count;
        } stat_commit;
    };

    StatsParser();

    std::vector<BackendStat> & get_backend_stats()
    { return m_backend_stats; }

    NodeStat & get_node_stat()
    { return m_data.node; }

    std::map<unsigned int, uint64_t> & get_rofs_errors()
    { return m_rofs_errors; }

public:
    virtual bool Key(const char* str, rapidjson::SizeType length, bool copy);

    virtual bool EndObject(rapidjson::SizeType nr_members);

    virtual bool Bool(bool b)
    { return UInteger(b); }

private:
    Data m_data;
    std::vector<BackendStat> m_backend_stats;
    std::map<unsigned int, uint64_t> m_rofs_errors;
};

#endif

