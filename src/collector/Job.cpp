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

#include "Job.h"

Job::Job()
    :
    m_type(EMPTY),
    m_status(NEW),
    m_group_id(0),
    m_timestamp(0)
{}

Job::Job(uint64_t timestamp)
    :
    m_type(EMPTY),
    m_status(NEW),
    m_group_id(0),
    m_timestamp(timestamp)
{}

int Job::init(mongo::BSONObj & obj, std::string & error_text)
{
    for (mongo::BSONObj::iterator it = obj.begin(); it.more();) {
        mongo::BSONElement element = it.next();
        const char *field_name = element.fieldName();

        try {
            if (!std::strcmp(field_name, "id")) {
                m_id = element.String();
            } else if (!std::strcmp(field_name, "status")) {
                bool ok = true;
                std::string str = element.String();
                m_status = status_from_db(str, ok);
                if (!ok) {
                    std::ostringstream ostr;
                    ostr << "Invalid status '" << str << '\'';
                    error_text = ostr.str();
                    return -1;
                }
            } else if (!std::strcmp(field_name, "group")) {
                m_group_id = element.Int();
            } else if (!std::strcmp(field_name, "type")) {
                bool ok = true;
                std::string str = element.String();
                m_type = type_from_db(str, ok);
                if (!ok) {
                    std::ostringstream ostr;
                    ostr << "Invalid type '" << str << '\'';
                    error_text = ostr.str();
                    return -1;
                }
            }
        } catch (const mongo::MsgAssertionException & e) {
            error_text = e.what();
            return -1;
        } catch (...) {
            error_text = "Unknown exception thrown";
            return -1;
        }
    }

    if (m_id.empty()) {
        error_text = "No job identifier";
        return -1;
    }

    return 0;
}

bool Job::equals(const Job & other) const
{
    return (m_id == other.m_id && m_type == other.m_type &&
            m_status == other.m_status && m_group_id == other.m_group_id);
}

void Job::update(const Job & other)
{
    if (other.equals(*this))
        return;

    if (m_timestamp < other.m_timestamp) {
        m_id = other.m_id;
        m_type = other.m_type;
        m_status = other.m_status;
        m_group_id = other.m_group_id;
        m_timestamp = other.m_timestamp;
    }
}

void Job::merge(const Job & other, bool & have_newer)
{
    if (m_timestamp > other.m_timestamp) {
        have_newer = true;
        return;
    }

    if (m_timestamp == other.m_timestamp)
        return;

    m_id = other.m_id;
    m_type = other.m_type;
    m_status = other.m_status;
    m_group_id = other.m_group_id;
    m_timestamp = other.m_timestamp;
}

void Job::print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer)
{
    writer.StartObject();
    writer.Key("id");
    writer.String(m_id.c_str());
    writer.Key("type");
    writer.String(type_str(m_type));
    writer.Key("status");
    writer.String(status_str(m_status));
    writer.Key("group");
    writer.Uint64(m_group_id);
    writer.EndObject();
}

const char *Job::type_str(Type type)
{
    switch (type)
    {
    case EMPTY:
        return "EMPTY";
    case MOVE_JOB:
        return "MOVE_JOB";
    case RECOVER_DC_JOB:
        return "RECOVER_DC_JOB";
    case COUPLE_DEFRAG_JOB:
        return "COUPLE_DEFRAG_JOB";
    case RESTORE_GROUP_JOB:
        return "RESTORE_GROUP_JOB";
    }
    return "UNKNOWN";
}

Job::Type Job::type_from_db(const std::string & str, bool & ok)
{
    ok = true;

    switch (str.length())
    {
    case 8:
        if (str == "move_job")
            return MOVE_JOB;
        break;

    case 14:
        if (str == "recover_dc_job")
            return RECOVER_DC_JOB;
        break;

    case 17:
        if (str == "couple_defrag_job")
            return COUPLE_DEFRAG_JOB;
        else if (str == "restore_group_job")
            return RESTORE_GROUP_JOB;
        break;
    }

    ok = false;
    return EMPTY;
}

const char *Job::status_str(Status status)
{
    switch (status) {
    case NEW:
        return "NEW";
    case NOT_APPROVED:
        return "NOT_APPROVED";
    case EXECUTING:
        return "EXECUTING";
    case PENDING:
        return "PENDING";
    case BROKEN:
        return "BROKEN";
    case COMPLETED:
        return "COMPLETED";
    case CANCELLED:
        return "CANCELLED";
    }
    return "UNKNOWN";
}

Job::Status Job::status_from_db(const std::string & str, bool & ok)
{
    ok = true;

    switch (str.length())
    {
    case 3:
        if (str == "new")
            return NEW;
        break;

    case 6:
        if (str == "broken")
            return BROKEN;
        break;

    case 7:
        if (str == "pending")
            return PENDING;
        break;

    case 9:
        if (str == "executing")
            return EXECUTING;
        if (str == "cancelled")
            return CANCELLED;
        if (str == "completed")
            return COMPLETED;
        break;

    case 12:
        if (str == "not_approved")
            return NOT_APPROVED;
        break;
    }

    ok = false;
    return NEW;
}
