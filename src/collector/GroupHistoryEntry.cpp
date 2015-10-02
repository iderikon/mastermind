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

#include "WorkerApplication.h"

#include "GroupHistoryEntry.h"

#include <vector>

struct GroupHistoryEntry::BackendObj
{
    BackendObj()
        :
        backend_id(0),
        port(0),
        family(0)
    {}

    bool incomplete() const
    {
        return (!backend_id || hostname.empty() || !port || !family);
    }

    std::string to_string() const
    {
        std::ostringstream ostr;
        ostr << hostname << ':' << port << ':' << family << '/' << backend_id;
        return ostr.str();
    }

    int backend_id;
    std::string hostname;
    int port;
    int family;
};

GroupHistoryEntry::GroupHistoryEntry()
    :
    m_group_id(0),
    m_timestamp(0.0)
{}

int GroupHistoryEntry::parse_backend(mongo::BSONObj & obj, BackendObj & backend)
{
    for (mongo::BSONObj::iterator it = obj.begin(); it.more();) {
        mongo::BSONElement elem = it.next();
        const char *field_name = elem.fieldName();

        if (!std::strcmp(field_name, "backend_id")) {
            backend.backend_id = elem.Number();
        } else if (!std::strcmp(field_name, "hostname")) {
            backend.hostname = elem.String();
        } else if (!std::strcmp(field_name, "port")) {
            backend.port = elem.Number();
        } else if (!std::strcmp(field_name, "family")) {
            backend.family = elem.Number();
        }
    }

    if (backend.incomplete()) {
        BH_LOG(app::logger(), DNET_LOG_ERROR,
                "Malformed group history entry: Incomplete backend:\n%s",
                obj.jsonString(mongo::Strict, 1).c_str());
        return -1;
    }

    return 0;
}

int GroupHistoryEntry::parse_backend_history_entry(mongo::BSONObj & obj)
{
    double cur_ts = 0.0;
    std::string cur_type;
    std::vector<BackendObj> backends;

    for (mongo::BSONObj::iterator it = obj.begin(); it.more();) {
        mongo::BSONElement elem = it.next();
        const char *field_name = elem.fieldName();

        if (!std::strcmp(field_name, "timestamp")) {
            cur_ts = elem.Double();
        } else if (!std::strcmp(field_name, "type")) {
            cur_type = elem.String();
        } else if (!std::strcmp(field_name, "set")) {
            std::vector<mongo::BSONElement> array = elem.Array();
            for (mongo::BSONElement & elem2 : array) {
                mongo::BSONObj obj2 = elem2.Obj();
                BackendObj cur_back;
                if (parse_backend(obj2, cur_back) != 0)
                    return -1;
                backends.push_back(cur_back);
            }
        }
    }

    if (cur_ts < m_timestamp)
        return 0;
    if (cur_type == "automatic")
        return 0;

    m_timestamp = cur_ts;
    m_backends.clear();
    for (const BackendObj & bobj : backends)
        m_backends.insert(bobj.to_string());

    return 1;
}

int GroupHistoryEntry::init(mongo::BSONObj & obj)
{
    for (mongo::BSONObj::iterator it = obj.begin(); it.more();) {
        mongo::BSONElement elem1 = it.next();
        const char *field_name = elem1.fieldName();

        try {
            BH_LOG(app::logger(), DNET_LOG_DEBUG, "field name %s", field_name);
            if (!std::strcmp(field_name, "group_id")) {
                m_group_id = elem1.Number();
            } else if (!std::strcmp(field_name, "nodes")) {
                std::vector<mongo::BSONElement> nodes = elem1.Array();
                for (mongo::BSONElement & elem2 : nodes) {
                    mongo::BSONObj obj2 = elem2.Obj();
                    int rc = parse_backend_history_entry(obj2);
                    if (rc < 0)
                        return -1;
                }
            }
        } catch (const mongo::MsgAssertionException & e) {
            BH_LOG(app::logger(), DNET_LOG_ERROR,
                    "Initializing GroupHistoryEntry from BSON: exception: '%s'", e.what());
            return -1;
        } catch (...) {
            BH_LOG(app::logger(), DNET_LOG_ERROR,
                    "Initializing GroupHistoryEntry from BSON: unknown exception");
            return -1;
        }
    }

    if (!m_group_id) {
        BH_LOG(app::logger(), DNET_LOG_ERROR,
                "Malformed group history entry [group_id %d]\n%s",
                m_group_id, time_t(m_timestamp), obj.jsonString(mongo::Strict, 1).c_str());
        return -1;
    }

    return 0;
}

std::string GroupHistoryEntry::to_string() const
{
    std::ostringstream ostr;

    ostr << "{\n"
            "  timestamp: " << m_timestamp << "\n"
            "  group_id: " << m_group_id << "\n"
            "  backends:\n"
            "  [\n";
    for (const std::string & backend : m_backends)
        ostr << "    " << backend << '\n';
    ostr << "  ]\n}";

    return ostr.str();
}
