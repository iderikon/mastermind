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
#include "Inventory.h"

#include <msgpack.hpp>

struct Inventory::GetDcData
{
    GetDcData(Inventory & s, const std::string & h)
        :
        self(s),
        host(h)
    {}

    Inventory & self;
    const std::string & host;
    std::string result;
};

struct Inventory::CacheDbUpdateData
{
    CacheDbUpdateData(Inventory & s, const HostInfo & i, bool e)
        :
        self(s),
        info(i),
        existing(e)
    {}

    Inventory & self;
    HostInfo info;
    bool existing;
};

struct Inventory::SaveUpdateData
{
    SaveUpdateData(Inventory & s, std::vector<Inventory::HostInfo> h)
        :
        self(s),
        hosts(std::move(h))
    {}

    Inventory & self;
    std::vector<Inventory::HostInfo> hosts;
};

Inventory::Inventory()
    :
    m_last_update_time(0.0)
{
    m_common_queue = dispatch_queue_create("inv_common", DISPATCH_QUEUE_SERIAL);
    m_update_queue = dispatch_queue_create("inv_update", DISPATCH_QUEUE_CONCURRENT);
}

Inventory::~Inventory()
{
    dispatch_release(m_common_queue);
    dispatch_release(m_update_queue);
}

int Inventory::init()
{
    m_manager = cocaine::framework::service_manager_t::create(
            cocaine::framework::service_manager_t::endpoint_t("localhost", 10053));

    if (!m_manager) {
        BH_LOG(app::logger(), DNET_LOG_ERROR, "Failed to create cocaine service manager");
        return -1;
    }

    std::string service_name = app::config().app_name + "-inventory";
    try {
        m_service = m_manager->get_service<cocaine::framework::app_service_t>(service_name);
    } catch (std::exception & e) {
        BH_LOG(app::logger(), DNET_LOG_ERROR, "Failed to connect to service %s: %s",
                service_name, e.what());
        m_service.reset();
        return -1;
    } catch (...) {
        BH_LOG(app::logger(), DNET_LOG_ERROR, "Failed to connect to service %s: "
                "unknown exception thrown", service_name);
        m_service.reset();
        return -1;
    }

    return 0;
}

void Inventory::download_initial()
{
    if (cache_db_connect() == 0) {
        BH_LOG(app::logger(), DNET_LOG_INFO, "Performing initial download");
        time_t download_start = ::time(nullptr);
        std::vector<HostInfo> hosts = load_hosts();
        for (HostInfo & info : hosts) {
            m_host_info[info.host] = info;
            if (info.timestamp >= download_start) {
                // Update cache database in update queue.
                dispatch_async_f(m_update_queue, new CacheDbUpdateData(*this, info, false),
                        &Inventory::execute_cache_db_update);
            }
        }

        dispatch_next_reload();
    }
}

void Inventory::dispatch_next_reload()
{
    BH_LOG(app::logger(), DNET_LOG_INFO, "Inventory: Dispatching next reload");

    // Config option 'infrastructure_dc_cache_update_period' is specified in seconds.
    // dispatch_time() accepts nanoseconds.
    dispatch_after_f(dispatch_time(DISPATCH_TIME_NOW,
                app::config().infrastructure_dc_cache_update_period * 1000000000ULL),
            m_update_queue, this, &Inventory::execute_reload);
}

std::vector<Inventory::HostInfo> Inventory::load_hosts()
{
    std::vector<HostInfo> hosts = load_cache_db();
    time_t now = ::time(nullptr);

    for (HostInfo & info : hosts) {
        if (now > info.timestamp &&
                (now - info.timestamp) > app::config().infrastructure_dc_cache_valid_time)
            fetch_from_cocaine(info);
    }

    return hosts;
}

void Inventory::execute_save_update(void *arg)
{
    // Executed in common queue.

    std::unique_ptr<SaveUpdateData> data(static_cast<SaveUpdateData*>(arg));

    BH_LOG(app::logger(), DNET_LOG_INFO, "Inventory: Saving update (%lu nodes)", data->hosts.size());

    for (HostInfo & info : data->hosts)
        data->self.m_host_info[info.host] = info;
}

void Inventory::execute_reload(void *arg)
{
    // Executed in update queue.

    BH_LOG(app::logger(), DNET_LOG_INFO, "Reloading cache");

    Inventory & self = *(Inventory *) arg;

    time_t reload_start = ::time(nullptr);

    std::vector<HostInfo> hosts = self.load_hosts();

    for (HostInfo & info : hosts) {
        if (info.timestamp >= reload_start)
            self.cache_db_update(info, true);
    }

    dispatch_async_f(self.m_common_queue, new SaveUpdateData(self, std::move(hosts)),
            &Inventory::execute_save_update);

    self.dispatch_next_reload();
}

void Inventory::stop()
{
    m_service.reset();
}

std::string Inventory::get_dc_by_host(const std::string & addr)
{
    GetDcData data(*this, addr);
    dispatch_sync_f(m_common_queue, &data, &Inventory::execute_get_dc_by_host);
    return data.result;
}

void Inventory::execute_get_dc_by_host(void *arg)
{
    // Executed in common queue.

    GetDcData & data = *(GetDcData *) arg;

    auto it = data.self.m_host_info.find(data.host);
    if (it != data.self.m_host_info.end()) {
        BH_LOG(app::logger(), DNET_LOG_DEBUG, "Inventory: Found host '%s' in map, DC is '%s'",
                data.host, it->second.dc);
        data.result = it->second.dc;
        return;
    }

    if (data.self.m_service.get() == nullptr) {
        BH_LOG(app::logger(), DNET_LOG_NOTICE,
                "Have no connection to cocaine inventory, defaulting DC=host='%s'", data.host);
        data.result = data.host;
        return;
    }


    HostInfo info;
    info.host = data.host;
    data.self.fetch_from_cocaine(info);
    data.result = info.dc;

    // Update cache database in update queue.
    dispatch_async_f(data.self.m_update_queue, new CacheDbUpdateData(data.self, info, false),
            &Inventory::execute_cache_db_update);
}

void Inventory::fetch_from_cocaine(HostInfo & info)
{
    msgpack::unpacked result;
    msgpack::object obj;

    try {
        // TODO: add timeout, handle reconnects (service.py:11).
        auto g = m_service->enqueue("get_dc_by_host", info.host);
        std::string data = g.next();

        msgpack::unpack(&result, data.c_str(), data.size());
        obj = result.get();

        if (obj.type != msgpack::type::RAW) {
            throw std::runtime_error("inventory worker returned object of unexpected type " +
                    std::to_string(int(obj.type)));
        }
    } catch (std::exception & e) {
        BH_LOG(app::logger(), DNET_LOG_ERROR,
                "Inventory: Could not resolve DC for host %s: %s", info.host, e.what());
        return;
    } catch (...) {
        BH_LOG(app::logger(), DNET_LOG_ERROR,
                "Inventory: Could not resolve DC for host %s: unknown exception thrown", info.host);
        return;
    }

    info.dc.assign(obj.via.raw.ptr, obj.via.raw.size);
}

Inventory::HostInfo::HostInfo(const mongo::BSONObj & obj)
{
    for (mongo::BSONObj::iterator it = obj.begin(); it.more();) {
        mongo::BSONElement element = it.next();
        const char *field_name = element.fieldName();

        if (!std::strcmp(field_name, "host"))
            this->host = element.String();
        else if (!std::strcmp(field_name, "dc"))
            this->dc = element.String();
        else if (!std::strcmp(field_name, "timestamp"))
            this->timestamp = element.Double();
    }

    if (host.empty() || dc.empty() || !timestamp) {
        std::ostringstream ostr;
        ostr << "Incomplete HostInfo from inventory DB: host='" << host << "' dc='" << dc << "' timestamp=" << timestamp;
        throw std::runtime_error(ostr.str());
    }
}

mongo::BSONObj Inventory::HostInfo::obj() const
{
    mongo::BSONObjBuilder builder;
    builder.append("host", host);
    builder.append("dc", dc);
    builder.append("timestamp", double(timestamp));
    return builder.obj();
}

int Inventory::cache_db_connect()
{
    try {
        const Config & config = app::config();

        if (config.metadata.url.empty() || config.metadata.inventory.db.empty()) {
            BH_LOG(app::logger(), DNET_LOG_WARNING,
                    "Not connecting to inventory database because it was not configured");
            return -1;
        }

        std::string errmsg;
        mongo::ConnectionString cs = mongo::ConnectionString::parse(config.metadata.url, errmsg);
        if (!cs.isValid()) {
            BH_LOG(app::logger(), DNET_LOG_ERROR,
                    "Mongo client ConnectionString error: %s", errmsg);
            return -1;
        }

        m_conn.reset((mongo::DBClientReplicaSet *) cs.connect(
                errmsg, double(config.metadata.options.connectTimeoutMS) / 1000.0));
        if (m_conn == nullptr) {
            BH_LOG(app::logger(), DNET_LOG_ERROR, "Connection failed: %s", errmsg);
            return -1;
        }

        m_collection_name = config.metadata.inventory.db + ".inventory";

        BH_LOG(app::logger(), DNET_LOG_INFO, "Successfully connected to inventory database");
        return 0;

    } catch (const mongo::DBException & e) {
        BH_LOG(app::logger(), DNET_LOG_ERROR,
                "Inventory: MongoDB thrown exception during database connection: %s", e.what());
    } catch (const std::exception & e) {
        BH_LOG(app::logger(), DNET_LOG_ERROR,
                "Exception thrown while connecting to inventory database: %s", e.what());
    } catch (...) {
        BH_LOG(app::logger(), DNET_LOG_ERROR,
                "Unknown exception thrown while connecting to inventory database");
    }

    return -1;
}

std::vector<Inventory::HostInfo> Inventory::load_cache_db()
{
    std::vector<HostInfo> result;

    if (m_conn == nullptr)
        return result;

    BH_LOG(app::logger(), DNET_LOG_INFO, "Inventory: Loading cache database "
            "(last update ts=%ld)", long(m_last_update_time));

    try {
        std::auto_ptr<mongo::DBClientCursor> cursor = m_conn->query(m_collection_name,
                MONGO_QUERY("timestamp" << mongo::GT << m_last_update_time));

        while (cursor->more()) {
            mongo::BSONObj obj = cursor->next();

            try {
                HostInfo info(obj);
                BH_LOG(app::logger(), DNET_LOG_INFO, "Loaded DC '%s' for host '%s' (updated at %lu)",
                        info.dc, info.host, info.timestamp);

                result.emplace_back(info);
            } catch (const mongo::MsgAssertionException & e) {
                BH_LOG(app::logger(), DNET_LOG_ERROR,
                        "Initializing HostInfo from BSON: msg assertion exception: '%s'", e.what());
            } catch (const std::exception & e) {
                BH_LOG(app::logger(), DNET_LOG_ERROR,
                        "Initializing HostInfo from BSON: exception thrown: '%s'", e.what());
            } catch (...) {
                BH_LOG(app::logger(), DNET_LOG_ERROR,
                        "Initializing HostInfo from BSON: unknown exception thrown");
            }
        }

        BH_LOG(app::logger(), DNET_LOG_INFO, "Updated inventory info for %lu hosts", result.size());

    } catch (const mongo::DBException & e) {
        BH_LOG(app::logger(), DNET_LOG_ERROR,
                "Cannot load cache db: Inventory DB thrown exception: %s", e.what());
    } catch (const std::exception & e) {
        BH_LOG(app::logger(), DNET_LOG_ERROR,
                "Exception thrown while loading inventory database: %s", e.what());
    } catch (...) {
        BH_LOG(app::logger(), DNET_LOG_ERROR,
                "Unknown exception thrown while loading inventory database");
    }

    return result;
}

void Inventory::execute_cache_db_update(void *arg)
{
    // Executed in update queue.

    std::unique_ptr<CacheDbUpdateData> data(static_cast<CacheDbUpdateData*>(arg));
    data->self.cache_db_update(data->info, data->existing);
}

void Inventory::cache_db_update(const HostInfo & info, bool existing)
{
    if (m_conn == nullptr)
        return;

    BH_LOG(app::logger(), DNET_LOG_INFO, "Adding host info to inventory database: "
            "host: '%s' DC: '%s' timestamp: %lu\n",
            info.host, info.dc, info.timestamp);

    try {
        mongo::BSONObj obj = info.obj();

        // XXX
        if (!existing)
            m_conn->insert(m_collection_name, obj);
        else
            m_conn->update(m_collection_name, MONGO_QUERY("host" << info.host), obj, 0);

    } catch (const mongo::DBException & e) {
        BH_LOG(app::logger(), DNET_LOG_ERROR,
                "Cannot update cache db: Inventory DB thrown exception: %s", e.what());
    } catch (const std::exception & e) {
        BH_LOG(app::logger(), DNET_LOG_ERROR,
                "Exception thrown while updating inventory database: %s", e.what());
    } catch (...) {
        BH_LOG(app::logger(), DNET_LOG_ERROR,
                "Unknown exception thrown while updating inventory database");
    }
}
