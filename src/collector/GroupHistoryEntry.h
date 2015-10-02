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

#ifndef __9aa43a1c_afa6_4bf7_a90c_b16d0ba55bb1
#define __9aa43a1c_afa6_4bf7_a90c_b16d0ba55bb1

#include <mongo/bson/bson.h>

#include <set>
#include <string>

class GroupHistoryEntry
{
public:
    GroupHistoryEntry();

    int init(mongo::BSONObj & obj);

    int get_group_id() const
    { return m_group_id; }

    const std::set<std::string> & get_backends() const
    { return m_backends; }

    double get_timestamp() const
    { return m_timestamp; }

    bool empty() const
    { return (m_timestamp < 1.0); }

    std::string to_string() const;

private:
    class BackendObj;

    int parse_backend(mongo::BSONObj & obj, BackendObj & backend);
    int parse_backend_history_entry(mongo::BSONObj & obj);

private:
    int m_group_id;
    std::set<std::string> m_backends;
    double m_timestamp;
};

#endif

