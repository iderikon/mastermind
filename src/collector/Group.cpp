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

#include "Backend.h"
#include "Config.h"
#include "Couple.h"
#include "Filter.h"
#include "FS.h"
#include "Group.h"
#include "Metrics.h"
#include "Node.h"
#include "Storage.h"

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
    m_update_time(0),
    m_metadata_parsed(false),
    m_metadata_parse_duration(0),
    m_couple(nullptr),
    m_active_job(nullptr),
    m_namespace(nullptr),
    m_type(DATA),
    m_status(INIT),
    m_internal_status(INIT_Init)
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

bool Group::has_active_job() const
{
    return m_active_job != nullptr;
}

const Job & Group::get_active_job() const
{
    return *m_active_job;
}

void Group::add_backend(Backend & backend)
{
    m_backends.insert(backend);
}

void Group::remove_backend(Backend & backend)
{
    m_backends.erase(backend);
}

void Group::handle_metadata_download_failed(const std::string & why)
{
    if (m_internal_status != INIT_MetadataFailed) {
        m_internal_status = INIT_MetadataFailed;

        std::ostringstream ostr;
        ostr << "Metadata download failed: " << why;
        m_status_text = ostr.str();

        m_metadata.version = 0;
        m_clean = true;
    }
}

void Group::save_metadata(const char *metadata, size_t size, uint64_t timestamp)
{
    if (timestamp > m_update_time)
        m_update_time = timestamp;

    if (m_clean && !m_metadata_file.empty() && m_metadata_file.size() == size &&
            !std::memcmp(&m_metadata_file[0], metadata, size))
        return;

    clock_get(m_update_time);
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
        m_internal_status = BAD_ParseFailed;
        m_status = BAD;
        return -1;
    }

    int version = 0;
    std::vector<int> couple;
    std::string ns;
    bool frozen = false;
    std::string type;
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
            else if (size == 4 && !std::strncmp(ptr, "type", 4)) {
                if (kv.val.type == msgpack::type::RAW) {
                    type.assign(kv.val.via.raw.ptr, kv.val.via.raw.size);
                } else {
                    ostr << "Invalid 'type' value type " << kv.val.type;
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
        m_internal_status = BAD_ParseFailed;
        m_status = BAD;
        return -1;
    }

    m_metadata.version = version;
    m_metadata.frozen = frozen;
    m_metadata.couple = couple;
    m_metadata.namespace_name = ns;
    m_metadata.type = type;
    m_metadata.service.migrating = service_migrating;
    m_metadata.service.job_id = service_job_id;
    m_metadata_parsed = true;

    return 0;
}

void Group::calculate_type()
{
    const std::string & cache_group_path_prefix = app::config().cache_group_path_prefix;

    if (!m_metadata.version) {
        if (!cache_group_path_prefix.empty()) {
            for (Backend & backend : m_backends) {
                if (!backend.get_base_path().compare(0,
                            cache_group_path_prefix.length(), cache_group_path_prefix)) {
                    m_type = UNMARKED;
                    return;
                }
            }
        }
    } else if (m_metadata.type == "cache") {
        m_type = CACHE;
        return;
    }
    m_type = DATA;
}

uint64_t Group::get_backend_update_time() const
{
    uint64_t res = 0;
    for (Backend & backend : m_backends) {
        uint64_t cur = backend.get_stat().get_timestamp() * 1000ULL;
        if (cur > res)
            res = cur;
    }
    return res;
}

void Group::set_namespace(Namespace & ns)
{
    m_namespace = &ns;
}

void Group::set_active_job(const Job & job)
{
    m_active_job = &job;
}

void Group::clear_active_job()
{
    m_active_job = nullptr;
}

void Group::update_status()
{
    if (m_backends.empty()) {
        if (m_internal_status != INIT_NoBackends) {
            m_internal_status = INIT_NoBackends;
            m_status = INIT;
            m_status_text = "No node backends";
        }
        return;
    } else if (m_backends.size() > 1 && app::config().forbidden_dht_groups) {
        if (m_internal_status != BROKEN_DHTForbidden) {
            m_internal_status = BROKEN_DHTForbidden;
            m_status = BROKEN;

            std::ostringstream ostr;
            ostr << "DHT groups are forbidden but the group has " << m_backends.size() << " backends";
            m_status_text = ostr.str();

            uint64_t backend_ts = get_backend_update_time();
            if (m_update_time < backend_ts)
                m_update_time = backend_ts;
        }
    } else {
        bool have_ro = false;
        bool have_other = false;
        uint64_t backend_ts = 0;

        for (Backend & backend : m_backends) {
            Backend::Status b_status = backend.get_status();

            uint64_t cur_ts = backend.get_stat().get_timestamp() * 1000ULL;
            if (backend_ts < cur_ts)
                backend_ts = cur_ts;

            if (b_status == Backend::RO) {
                have_ro = true;
            } else if (b_status != Backend::OK) {
                have_other = true;
            }
        }

        if (have_ro) {
            if (m_metadata.service.migrating) {
                if (m_active_job != nullptr && m_active_job->get_id() == m_metadata.service.job_id) {
                    m_internal_status = MIGRATING_ServiceMigrating;
                    m_status = MIGRATING;

                    std::ostringstream ostr;
                    ostr << "Group is migrating, job id is '" << m_metadata.service.job_id << '\'';
                    m_status_text = ostr.str();
                } else {
                    m_internal_status = BAD_NoActiveJob;
                    m_status = BAD;

                    std::ostringstream ostr;
                    ostr << "Group has no active job, but marked as migrating with job id '"
                         << m_metadata.service.job_id << '\'';
                    m_status_text = ostr.str();
                }
            } else {
                if (m_internal_status != RO_HaveROBackends) {
                    if (m_update_time < backend_ts)
                        m_update_time = backend_ts;

                    m_internal_status = RO_HaveROBackends;
                    m_status = RO;
                    m_status_text = "Group is read-only because it has read-only backends";
                }
            }
        } else if (have_other) {
            if (m_internal_status != BAD_HaveOther) {
                if (m_update_time < backend_ts)
                    m_update_time = backend_ts;

                m_internal_status = BAD_HaveOther;
                m_status = BAD;
                m_status_text = "Group is in state BAD because some of backends are not in state OK";
            }
        } else if (m_metadata_parsed) {
            m_status_text = "Group is OK";
            if (!m_metadata.couple.empty()) {
                if (m_status != COUPLED) {
                    m_internal_status = COUPLED_MetadataOK;
                    m_status = COUPLED;
                }
            } else {
                m_internal_status = INIT_Uncoupled;
                m_status = INIT;
            }
        }
    }
}

void Group::set_coupled_status(bool ok, uint64_t timestamp)
{
    if (m_internal_status == BROKEN_DHTForbidden ||
            m_internal_status == BAD_HaveOther ||
            m_internal_status == BAD_ParseFailed ||
            m_internal_status == BAD_InconsistentCouple ||
            m_internal_status == BAD_DifferentMetadata ||
            m_internal_status == MIGRATING_ServiceMigrating ||
            m_internal_status == RO_HaveROBackends)
        return;

    InternalStatus new_internal_status = ok ? COUPLED_Coupled : BAD_CoupleBAD;

    if (m_internal_status != new_internal_status) {
        if (m_update_time < timestamp)
            m_update_time = timestamp;
        m_internal_status = new_internal_status;
        m_status = ok ? COUPLED : BAD;
        m_status_text = (ok ? "Group is OK"
                            : "Group is in state BAD because couple check fails");
    }
}

int Group::check_couple_equals(const Group & other)
{
    if (m_internal_status == INIT_Init ||
            other.m_internal_status == INIT_Init ||
            m_internal_status == INIT_NoBackends ||
            other.m_internal_status == INIT_NoBackends ||
            m_internal_status == INIT_MetadataFailed ||
            other.m_internal_status == INIT_MetadataFailed)
        return 0;

    if (m_metadata.couple != other.m_metadata.couple) {
        if (m_internal_status != BAD_InconsistentCouple) {
            if (m_update_time < other.m_update_time)
                m_update_time = other.m_update_time;

            std::ostringstream ostr;
            ostr << "Groups " << m_id << " and " << other.m_id << " have inconsistent couple info";
            m_status_text = ostr.str();
            m_internal_status = BAD_InconsistentCouple;
            m_status = BAD;
        }
        return -1;
    }

    return 0;
}

int Group::check_metadata_equals(const Group & other)
{
    if (m_internal_status == INIT_Init ||
            other.m_internal_status == INIT_Init ||
            m_internal_status == INIT_NoBackends ||
            other.m_internal_status == INIT_NoBackends ||
            m_internal_status == INIT_MetadataFailed ||
            other.m_internal_status == INIT_MetadataFailed)
        return 0;

    if (m_metadata.frozen != other.m_metadata.frozen ||
            m_metadata.couple != other.m_metadata.couple ||
            m_metadata.namespace_name != other.m_metadata.namespace_name) {
        if (m_internal_status != BAD_DifferentMetadata) {
            if (m_update_time < other.m_update_time)
                m_update_time = other.m_update_time;

            std::ostringstream ostr;
            ostr << "Groups " << m_id << " and " << other.m_id << " have different metadata";
            m_status_text = ostr.str();
            m_internal_status = BAD_DifferentMetadata;
            m_status = BAD;
        }
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
    if (m_update_time > other.m_update_time) {
        have_newer = true;
        return;
    }

    if (m_update_time == other.m_update_time)
        return;

    m_clean = other.m_clean;
    m_metadata_file = other.m_metadata_file;
    m_update_time = other.m_update_time;

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
    m_internal_status = other.m_internal_status;
}

void Group::push_items(std::vector<std::reference_wrapper<Couple>> & couples) const
{
    if (m_couple != nullptr)
        couples.push_back(*m_couple);
}

void Group::push_items(std::vector<std::reference_wrapper<Namespace>> & namespaces) const
{
    if (m_namespace != nullptr)
        namespaces.push_back(*m_namespace);
}

void Group::push_items(std::vector<std::reference_wrapper<Node>> & nodes) const
{
    for (Backend & backend : m_backends)
        backend.push_items(nodes);
}

void Group::push_items(std::vector<std::reference_wrapper<Backend>> & backends) const
{
    backends.insert(backends.end(), m_backends.begin(), m_backends.end());
}

void Group::push_items(std::vector<std::reference_wrapper<FS>> & filesystems) const
{
    for (Backend & backend : m_backends)
        backend.push_items(filesystems);
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
    writer.Key("type");
    writer.String(type_str(m_type));

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

    if (m_active_job != nullptr) {
        writer.Key("active_job");
        writer.StartObject();
        writer.Key("type");
        writer.String(Job::type_str(m_active_job->get_type()));
        writer.Key("status");
        writer.String(Job::status_str(m_active_job->get_status()));
        writer.EndObject();
    }

    if (show_internals) {
        writer.Key("clean");
        writer.Bool(m_clean);
        writer.Key("update_time");
        writer.Uint64(m_update_time);
        writer.Key("metadata_parsed");
        writer.Bool(m_metadata_parsed);
        writer.Key("metadata_parse_duration");
        writer.Uint64(m_metadata_parse_duration);
        writer.Key("internal_status");
        writer.String(internal_status_str(m_internal_status));

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
            if (!m_metadata.type.empty()) {
                writer.Key("type");
                writer.String(m_metadata.type.c_str());
            }
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

    enum InternalStatus {
    };

const char *Group::internal_status_str(InternalStatus status)
{
    switch (status)
    {
    case INIT_Init:
        return "INIT_Init";
    case INIT_NoBackends:
        return "INIT_NoBackends";
    case INIT_MetadataFailed:
        return "INIT_MetadataFailed";
    case INIT_Uncoupled:
        return "INIT_Uncoupled";
    case BROKEN_DHTForbidden:
        return "BROKEN_DHTForbidden";
    case BAD_HaveOther:
        return "BAD_HaveOther";
    case BAD_ParseFailed:
        return "BAD_ParseFailed";
    case BAD_InconsistentCouple:
        return "BAD_InconsistentCouple";
    case BAD_DifferentMetadata:
        return "BAD_DifferentMetadata";
    case BAD_CoupleBAD:
        return "BAD_CoupleBAD";
    case BAD_NoActiveJob:
        return "BAD_NoActiveJob";
    case MIGRATING_ServiceMigrating:
        return "MIGRATING_ServiceMigrating";
    case RO_HaveROBackends:
        return "RO_HaveROBackends";
    case COUPLED_MetadataOK:
        return "COUPLED_MetadataOK";
    case COUPLED_Coupled:
        return "COUPLED_Coupled";
    }
    return "UNKNOWN";
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

const char *Group::type_str(Type type)
{
    switch (type)
    {
    case DATA:
        return "DATA";
    case CACHE:
        return "CACHE";
    case UNMARKED:
        return "UNMARKED";
    }
    return "UNKNOWN";
}
