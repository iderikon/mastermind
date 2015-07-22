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

#include "Backend.h"
#include "Config.h"
#include "Couple.h"
#include "Group.h"
#include "Guard.h"
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

Group::Group(Backend & backend, Storage & storage)
    :
    m_id(backend.get_stat().group),
    m_storage(storage),
    m_couple(NULL),
    m_clean(true),
    m_status(INIT),
    m_frozen(false),
    m_version(0),
    m_namespace(NULL)
{
    m_backends.insert(&backend);
    m_service.migrating = false;
}

Group::Group(int id, Storage & storage)
    :
    m_id(id),
    m_storage(storage),
    m_couple(NULL),
    m_clean(true),
    m_status(INIT),
    m_frozen(false),
    m_version(0),
    m_namespace(NULL)
{
    m_service.migrating = false;
}

void Group::update_backend(Backend & backend)
{
    WriteGuard<RWSpinLock> guard(m_backends_lock);
    m_backends.insert(&backend);
}

void Group::save_metadata(const char *metadata, size_t size)
{
    LockGuard<SpinLock> guard(m_metadata_lock);

    if (!m_metadata.empty() && m_metadata.size() == size &&
            !std::memcmp(&m_metadata[0], metadata, size)) {
        m_clean = true;
        return;
    }

    m_metadata.assign(metadata, metadata + size);
    m_clean = false;
}

void Group::process_metadata()
{
    if (m_clean)
        return;

    std::vector<Backend*> backends;

    {
        ReadGuard<RWSpinLock> guard(m_backends_lock);
        backends.assign(m_backends.begin(), m_backends.end());
    }

    if (m_id == 1)
        sleep(20);

    LockGuard<SpinLock> guard(m_metadata_lock);

    if (m_clean)
        return;

    m_clean = true;

    std::ostringstream ostr;

    m_status_text.clear();

    msgpack::unpacked result;
    msgpack::object obj;

    try {
        msgpack::unpack(&result, &m_metadata[0], m_metadata.size());
        obj = result.get();
    } catch (std::exception & e) {
        ostr << "msgpack could not parse group metadata: " << e.what();
    } catch (...) {
        ostr << "msgpack could not parse group metadta with unknown reason";
    }

    if (!ostr.str().empty()) {
        m_status_text = ostr.str();
        m_status = BAD;
        return;
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
                    ostr << "invalid 'version' value type " << kv.val.type;
                    break;
                }
            }
            else if (size == 6 && !std::strncmp(ptr, "couple", 6)) {
                if (!parse_couple(kv.val, couple)) {
                    ostr << "couldn't parse 'couple'" << std::endl;
                    break;
                }
            }
            else if (size == 9 && !std::strncmp(ptr, "namespace", 9)) {
                if (kv.val.type == msgpack::type::RAW) {
                    ns.assign(kv.val.via.raw.ptr, kv.val.via.raw.size);
                } else {
                    ostr << "invalid 'namespace' value type " << kv.val.type;
                    break;
                }
            }
            else if (size == 6 && !std::strncmp(ptr, "frozen", 6)) {
                if (kv.val.type == msgpack::type::BOOLEAN) {
                    frozen = kv.val.via.boolean;
                } else {
                    ostr << "invalid 'frozen' value type " << kv.val.type;
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
                                service_job_id.assign(
                                        srv_kv.val.via.raw.ptr, srv_kv.val.via.raw.size);
                            } else {
                                ostr << "invalid 'job_id' value type " << srv_kv.val.type;
                                ok = false;
                                break;
                            }
                        }
                    }
                    if (!ok)
                        break;
                } else {
                    ostr << "invalid 'service' value type " << kv.val.type;
                    break;
                }
            }
        }
    }
    else if (obj.type == msgpack::type::ARRAY) {
        version = 1;
        ns = "default";
        if (!parse_couple(obj, couple))
            ostr << "couldn't parse couple (format of version 1)";
    }

    if (!ostr.str().empty()) {
        m_status_text = ostr.str();
        m_status = BAD;
        return;
    }

    m_version = version;
    m_frozen = frozen;
    m_service.migrating = service_migrating;
    m_service.job_id = service_job_id;

    if (m_namespace == NULL) {
        m_namespace = m_storage.get_namespace(ns);
    } else if (m_namespace->get_name() != ns) {
        m_status = BAD;
        ostr << "group moved to another namespace: '"
             << m_namespace->get_name() << "' -> '"
             << ns << '\'';
        m_status_text = ostr.str();
        return;
    }

    if (m_couple != NULL) {
        if (!m_couple->check(couple)) {
            std::vector<int> couple_groups;
            m_couple->get_group_ids(couple_groups);

            ostr << "couple in group metadata [ ";
            for (size_t i = 0; i < couple.size(); ++i)
                ostr << couple[i] << ' ';
            ostr << "] doesn't match to existing one [ ";
            for (size_t i = 0; i < couple_groups.size(); ++i)
                ostr << couple_groups[i] << ' ';
            ostr << ']';

            m_status_text = ostr.str();
            m_status = BAD;

            return;
        }
    } else {
        m_storage.create_couple(couple, this);
    }

    if (backends.empty()) {
        m_status = INIT;
        m_status_text = "no node backends";
    } else if (backends.size() > 1 && m_storage.get_app().get_config().forbidden_dht_groups) {
        m_status = BROKEN;

        ostr << "DHT groups are forbidden but the group has " << backends.size() << " backends";
        m_status_text = ostr.str();
    } else {
        bool have_bad = false;
        bool have_ro = false;
        bool have_other = false;

        for (size_t i = 0; i < backends.size(); ++i) {
            Backend::Status b_status = backends[i]->get_status();
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
            m_status_text = "some of backends are in state BROKEN";
        } else if (have_ro) {
            if (m_service.migrating) {
                m_status = MIGRATING;
                ostr << "group is migrating, job id is '" << m_service.job_id << '\'';
                m_status_text = ostr.str();
                // TODO: check whether the job was initiated
            } else {
                m_status = RO;
                m_status_text = "group is read-only because it has read-only backends";
            }
        } else if (have_other) {
            m_status = BAD;
            m_status_text = "group is in state BAD because some of "
                "backends are not in state OK";
        } else {
            m_status = COUPLED;
            m_status_text = "group is OK";
        }
    }
}

bool Group::metadata_equals(const Group & other) const
{
    return (m_frozen == other.m_frozen &&
            m_couple == other.m_couple &&
            m_namespace == other.m_namespace);
}

void Group::set_status_text(const std::string & status_text)
{
    LockGuard<SpinLock> guard(m_metadata_lock);
    m_status_text = status_text;
}

void Group::get_status_text(std::string & status_text) const
{
    LockGuard<SpinLock> guard(m_metadata_lock);
    status_text = m_status_text;
}

void Group::print_info(std::ostream & ostr) const
{
    ostr << "Group " << m_id << " {\n"
            "  couple:   [ ";

    if (m_couple != NULL) {
        std::vector<int> group_ids;
        m_couple->get_group_ids(group_ids);
        for (size_t i = 0; i < group_ids.size(); ++i)
            ostr << group_ids[i] << ' ';
    }

    ostr << "]\n"
            "  backends: [ ";
    {
        ReadGuard<RWSpinLock> guard(m_backends_lock);
        size_t i = 1;
        for (auto it = m_backends.begin(); it != m_backends.end(); ++it, ++i) {
            if (i != 1)
                ostr << "              ";

            ostr << (*it)->get_node().get_key() << '/' << (*it)->get_stat().backend_id;

            if (i < m_backends.size())
                ostr << '\n';
        }
        ostr << " ]\n";
    }

    ostr << "  clean: " << std::boolalpha << m_clean << "\n"
            "  status_text: " << m_status_text << "\n"
            "  status: " << status_str(m_status) << "\n"
            "  frozen: " << m_frozen << "\n"
            "  version: " << m_version << "\n"
            "  namespace: ";

    if (m_namespace != NULL)
        ostr << m_namespace->get_name() << '\n';
    else
        ostr << "<null>\n";

    ostr << "  service: {\n"
            "    migrating: " << m_service.migrating << "\n"
            "    job_id: '" << m_service.job_id << "'\n"
            "  }\n"
            "}";
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
