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

#include "Backend.h"
#include "Config.h"
#include "Couple.h"
#include "Filter.h"
#include "FS.h"
#include "Group.h"
#include "Metrics.h"
#include "Node.h"
#include "Storage.h"
#include "WorkerApplication.h"

#include <msgpack.hpp>

#include <algorithm>
#include <cstring>
#include <sstream>

namespace {

bool parse_couple(msgpack::object & obj, std::vector<int> & couple)
{
    if (obj.type != msgpack::type::ARRAY)
        return false;

    for (uint32_t i = 0; i < obj.via.array.size; ++i) {
        msgpack::object & gr_obj = obj.via.array.ptr[i];
        if (gr_obj.type != msgpack::type::POSITIVE_INTEGER)
            return false;
        couple.push_back(int(gr_obj.via.u64));
    }

    std::sort(couple.begin(), couple.end());

    return true;
}

} // unnamed namespace

Group::Group(int id)
    :
    m_id(id),
    m_clean(true),
    m_metadata_download_time(0),
    m_metadata_parsed(false),
    m_metadata_parse_duration(0),
    m_couple(nullptr),
    m_namespace(nullptr),
    m_status(INIT)
{
    m_metadata.version = 0;
    m_metadata.frozen = false;
    m_metadata.service.migrating = false;
}

bool Group::full() const
{
    for (const Backend & backend : m_backends) {
        if (!backend.full())
            return false;
    }
    return true;
}

uint64_t Group::get_total_space() const
{
    uint64_t res = 0;

    for (const Backend & backend : m_backends)
        res += backend.get_total_space();
    return res;
}

void Group::add_backend(Backend & backend)
{
    m_backends.insert(backend);
}

void Group::handle_metadata_download_failed(const std::string & why)
{
    if (m_status == INIT) {
        std::ostringstream ostr;
        ostr << "Metadata download failed: " << why;
        m_status_text = ostr.str();
    }
}

void Group::save_metadata(const char *metadata, size_t size)
{
    if (m_clean && !m_metadata_file.empty() && m_metadata_file.size() == size &&
            !std::memcmp(&m_metadata_file[0], metadata, size))
        return;

    clock_get(m_metadata_download_time);
    m_metadata_file.assign(metadata, metadata + size);
    m_clean = false;
}

int Group::parse_metadata()
{
    if (m_clean)
        return 0;

    m_clean = true;
    m_metadata_parsed = false;

    Stopwatch watch(m_metadata_parse_duration);

    m_status_text.clear();

    msgpack::unpacked result;
    msgpack::object obj;

    std::ostringstream ostr;
    try {
        msgpack::unpack(&result, &m_metadata_file[0], m_metadata_file.size());
        obj = result.get();
    } catch (std::exception & e) {
        ostr << "msgpack could not parse group metadata: " << e.what();
    } catch (...) {
        ostr << "msgpack could not parse group metadta with unknown reason";
    }

    if (!ostr.str().empty()) {
        m_status_text = ostr.str();
        m_status = BAD;
        return -1;
    }

    int version = 0;
    std::vector<int> couple;
    std::string ns;
    bool frozen = false;
    bool service_migrating = false;
    std::string service_job_id;

    if (obj.type == msgpack::type::MAP) {
        for (uint32_t i = 0; i < obj.via.map.size; ++i) {
            msgpack::object_kv & kv = obj.via.map.ptr[i];
            if (kv.key.type != msgpack::type::RAW)
                continue;

            uint32_t size = kv.key.via.raw.size;
            const char *ptr = kv.key.via.raw.ptr;

            if (size == 7 && !std::strncmp(ptr, "version", 7)) {
                if (kv.val.type == msgpack::type::POSITIVE_INTEGER) {
                    version = int(kv.val.via.u64);
                } else {
                    ostr << "Invalid 'version' value type " << kv.val.type;
                    break;
                }
            }
            else if (size == 6 && !std::strncmp(ptr, "couple", 6)) {
                if (!parse_couple(kv.val, couple)) {
                    ostr << "Couldn't parse 'couple'" << std::endl;
                    break;
                }
            }
            else if (size == 9 && !std::strncmp(ptr, "namespace", 9)) {
                if (kv.val.type == msgpack::type::RAW) {
                    ns.assign(kv.val.via.raw.ptr, kv.val.via.raw.size);
                } else {
                    ostr << "Invalid 'namespace' value type " << kv.val.type;
                    break;
                }
            }
            else if (size == 6 && !std::strncmp(ptr, "frozen", 6)) {
                if (kv.val.type == msgpack::type::BOOLEAN) {
                    frozen = kv.val.via.boolean;
                } else {
                    ostr << "Invalid 'frozen' value type " << kv.val.type;
                    break;
                }
            }
            else if (size == 7 && !std::strncmp(ptr, "service", 7)) {
                if (kv.val.type == msgpack::type::MAP) {
                    bool ok = true;

                    for (uint32_t j = 0; j < kv.val.via.map.size; ++j) {
                        msgpack::object_kv & srv_kv = kv.val.via.map.ptr[j];
                        if (srv_kv.key.type != msgpack::type::RAW)
                            continue;

                        uint32_t srv_size = srv_kv.key.via.raw.size;
                        const char *srv_ptr = srv_kv.key.via.raw.ptr;

                        if (srv_size == 6 && !std::strncmp(srv_ptr, "status", 6)) {
                            // XXX
                            if (srv_kv.val.type == msgpack::type::RAW) {
                                uint32_t st_size = srv_kv.val.via.raw.size;
                                const char *st_ptr = srv_kv.val.via.raw.ptr;
                                if (st_size == 9 && !std::strncmp(st_ptr, "MIGRATING", 9))
                                    service_migrating = true;
                            }
                        }
                        else if (srv_size == 6 && !std::strncmp(srv_ptr, "job_id", 6)) {
                            if (srv_kv.val.type == msgpack::type::RAW) {
                                service_job_id.assign(srv_kv.val.via.raw.ptr, srv_kv.val.via.raw.size);
                            } else {
                                ostr << "Invalid 'job_id' value type " << srv_kv.val.type;
                                ok = false;
                                break;
                            }
                        }
                    }
                    if (!ok)
                        break;
                } else {
                    ostr << "Invalid 'service' value type " << kv.val.type;
                    break;
                }
            }
        }
    }
    else if (obj.type == msgpack::type::ARRAY) {
        version = 1;
        ns = "default";
        if (!parse_couple(obj, couple))
            ostr << "Couldn't parse couple (format of version 1)";
    }

    if (!ostr.str().empty()) {
        m_status_text = ostr.str();
        m_status = BAD;
        return -1;
    }

    m_metadata.version = version;
    m_metadata.frozen = frozen;
    m_metadata.couple = couple;
    m_metadata.namespace_name = ns;
    m_metadata.service.migrating = service_migrating;
    m_metadata.service.job_id = service_job_id;
    m_metadata_parsed = true;

    return 0;
}

void Group::set_namespace(Namespace & ns)
{
    m_namespace = &ns;
}

void Group::update_status(bool forbidden_dht)
{
    if (m_backends.empty()) {
        m_status = INIT;
        m_status_text = "No node backends";
    } else if (m_backends.size() > 1 && forbidden_dht) {
        m_status = BROKEN;

        std::ostringstream ostr;
        ostr << "DHT groups are forbidden but the group has " << m_backends.size() << " backends";
        m_status_text = ostr.str();
        return;
    } else {
        bool have_bad = false;
        bool have_ro = false;
        bool have_other = false;

        for (Backend & backend : m_backends) {
            Backend::Status b_status = backend.get_status();
            if (b_status == Backend::BAD) {
                have_bad = true;
                break;
            } else if (b_status == Backend::RO) {
                have_ro = true;
            } else if (b_status != Backend::OK) {
                have_other = true;
            }
        }

        if (have_bad) {
            m_status = BROKEN;
            m_status_text = "Some of backends are in state BROKEN";
        } else if (have_ro) {
            if (m_metadata.service.migrating) {
                m_status = MIGRATING;

                std::ostringstream ostr;
                ostr << "Group is migrating, job id is '" << m_metadata.service.job_id << '\'';
                m_status_text = ostr.str();
                // TODO: check whether the job was initiated
            } else {
                m_status = RO;
                m_status_text = "Group is read-only because it has read-only backends";
            }
        } else if (have_other) {
            m_status = BAD;
            m_status_text = "Group is in state BAD because some of "
                "backends are not in state OK";
        } else {
            m_status = COUPLED;
            m_status_text = "Group is OK";
        }
    }
}

int Group::check_couple_equals(const Group & other)
{
    if (m_status == INIT || other.m_status == INIT)
        return 0;

    if (m_metadata.couple != other.m_metadata.couple) {
        std::ostringstream ostr;
        ostr << "Groups " << m_id << " and " << other.m_id << " have inconsistent couple info";
        m_status_text = ostr.str();
        m_status = BAD;
        return -1;
    }

    return 0;
}

int Group::check_metadata_equals(const Group & other)
{
    if (m_status == INIT || other.m_status == INIT)
        return 0;

    if (m_metadata.frozen != other.m_metadata.frozen ||
            m_metadata.couple != other.m_metadata.couple ||
            m_metadata.namespace_name != other.m_metadata.namespace_name) {
        std::ostringstream ostr;
        ostr << "Groups " << m_id << " and " << other.m_id << " have different metadata";
        m_status_text = ostr.str();
        m_status = BAD;
        return -1;
    }

    return 0;
}

void Group::set_couple(Couple & couple)
{
    m_couple = &couple;
}

bool Group::match_couple(const Group & other) const
{
    if (m_couple == nullptr || other.m_couple == nullptr)
        return false;
    return m_couple == other.m_couple;
}

void Group::merge(const Group & other, bool & have_newer)
{
    if (m_metadata_download_time > other.m_metadata_download_time) {
        have_newer = true;
        return;
    }

    if (m_metadata_download_time == other.m_metadata_download_time)
        return;

    m_clean = other.m_clean;
    m_metadata_file = other.m_metadata_file;
    m_metadata_download_time = other.m_metadata_download_time;

    m_metadata.version = other.m_metadata.version;
    m_metadata.frozen = other.m_metadata.frozen;
    m_metadata.couple = other.m_metadata.couple;
    m_metadata.namespace_name = other.m_metadata.namespace_name;
    m_metadata.service.migrating = other.m_metadata.service.migrating;
    m_metadata.service.job_id = other.m_metadata.service.job_id;

    m_metadata_parsed = other.m_metadata_parsed;
    m_metadata_parse_duration = other.m_metadata_parse_duration;

    m_status_text = other.m_status_text;
    m_status = other.m_status;
}

void Group::get_items(std::vector<std::reference_wrapper<Couple>> & couples) const
{
    if (m_couple != nullptr)
        couples.push_back(*m_couple);
}

void Group::get_items(std::vector<std::reference_wrapper<Namespace>> & namespaces) const
{
    if (m_namespace != nullptr)
        namespaces.push_back(*m_namespace);
}

void Group::get_items(std::vector<std::reference_wrapper<Node>> & nodes) const
{
    for (Backend & backend : m_backends)
        backend.get_items(nodes);
}

void Group::get_items(std::vector<std::reference_wrapper<Backend>> & backends) const
{
    backends.insert(backends.end(), m_backends.begin(), m_backends.end());
}

void Group::get_items(std::vector<std::reference_wrapper<FS>> & filesystems) const
{
    for (Backend & backend : m_backends)
        backend.get_items(filesystems);
}

void Group::print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer,
        bool show_internals) const
{
    writer.StartObject();

    writer.Key("id");
    writer.Uint64(m_id);

    if (m_couple != nullptr) {
        writer.Key("couple");
        writer.String(m_couple->get_key().c_str());
    }

    writer.Key("backends");
    writer.StartArray();
    for (Backend & backend : m_backends)
        writer.String(backend.get_key().c_str());
    writer.EndArray();

    writer.Key("status_text");
    writer.String(m_status_text.c_str());
    writer.Key("status");
    writer.String(status_str(m_status));

    if (m_metadata_parsed) {
        writer.Key("frozen");
        writer.Bool(m_metadata.frozen);
        writer.Key("version");
        writer.Uint64(m_metadata.version);
        writer.Key("namespace");
        writer.String(m_metadata.namespace_name.c_str());
        if (m_metadata.service.migrating || !m_metadata.service.job_id.empty()) {
            writer.Key("service");
            writer.StartObject();
            writer.Key("migrating");
            writer.Bool(m_metadata.service.migrating);
            writer.Key("job_id");
            writer.String(m_metadata.service.job_id.c_str());
            writer.EndObject();
        }
    }

    if (show_internals) {
        writer.Key("clean");
        writer.Bool(m_clean);
        writer.Key("metadata_download_time");
        writer.Uint64(m_metadata_download_time);
        writer.Key("metadata_parsed");
        writer.Bool(m_metadata_parsed);
        writer.Key("metadata_parse_duration");
        writer.Uint64(m_metadata_parse_duration);

        writer.Key("metadata_internal");
        writer.StartObject();
            writer.Key("version");
            writer.Uint64(m_metadata.version);
            writer.Key("frozen");
            writer.Bool(m_metadata.frozen);
            writer.Key("couple");
            writer.StartArray();
            for (int id : m_metadata.couple)
                writer.Uint64(id);
            writer.EndArray();
            writer.Key("namespace_name");
            writer.String(m_metadata.namespace_name.c_str());
            writer.Key("service");
            writer.StartObject();
                writer.Key("migrating");
                writer.Bool(m_metadata.service.migrating);
                writer.Key("job_id");
                writer.String(m_metadata.service.job_id.c_str());
            writer.EndObject();
        writer.EndObject();
    }

    writer.EndObject();
}

const char *Group::status_str(Status status)
{
    switch (status)
    {
    case INIT:
        return "INIT";
    case COUPLED:
        return "COUPLED";
    case BAD:
        return "BAD";
    case BROKEN:
        return "BROKEN";
    case RO:
        return "RO";
    case MIGRATING:
        return "MIGRATING";
    }
    return "UNKNOWN";
}
