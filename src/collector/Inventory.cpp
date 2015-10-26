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

#include <dlfcn.h>

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
    m_handle(nullptr),
    m_last_update_time(0.0)
{
    m_common_queue = dispatch_queue_create("inv_common", 0);
    m_update_queue = dispatch_queue_create("inv_update", DISPATCH_QUEUE_CONCURRENT);
}

Inventory::~Inventory()
{
    m_driver.reset();
    if (m_handle != nullptr)
        dlclose(m_handle);
    dispatch_release(m_common_queue);
    dispatch_release(m_update_queue);
}

int Inventory::init()
{
    if (!app::config().collector_inventory.empty()) {
        BH_LOG(app::logger(), DNET_LOG_INFO, "Opening inventory driver at %s",
                app::config().collector_inventory.c_str());
        open_driver(app::config().collector_inventory);
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
                // update cache database in update queue
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
            fetch_from_driver(info, true);
    }

    return hosts;
}

void Inventory::execute_save_update(void *arg)
{
    std::unique_ptr<SaveUpdateData> data(static_cast<SaveUpdateData*>(arg));

    BH_LOG(app::logger(), DNET_LOG_INFO, "Inventory: Saving update (%lu nodes)", data->hosts.size());

    for (HostInfo & info : data->hosts)
        data->self.m_host_info[info.host] = info;
}

void Inventory::execute_reload(void *arg)
{
    // executed in update queue

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

void Inventory::open_driver(const std::string & file_name)
{
    if (m_handle != nullptr) {
        BH_LOG(app::logger(), DNET_LOG_ERROR,
                "Internal error: Inventory driver is already opened");
        return;
    }

    m_handle = dlopen(file_name.c_str(), RTLD_LAZY | RTLD_LOCAL);

    if (m_handle == nullptr) {
        const char *err = dlerror();
        BH_LOG(app::logger(), DNET_LOG_ERROR,
                "Inventory: dlopen() failed for '%s': %s",
                file_name.c_str(), (err != nullptr ? err : "unknown error"));
        return;
    }

    InventoryDriver *(*create_inventory)() = (InventoryDriver *(*)()) dlsym(m_handle, "create_inventory");
    if (create_inventory == nullptr) {
        const char *err = dlerror();
        BH_LOG(app::logger(), DNET_LOG_ERROR,
                "Inventory: Cannot find symbol 'create_inventory' in '%s': '%s'",
                file_name.c_str(), (err != nullptr ? err : "unknown error"));
        close();
        return;
    }

    InventoryDriver *driver = create_inventory();
    if (driver == nullptr) {
        BH_LOG(app::logger(), DNET_LOG_ERROR, "Inventory: inventory_create() returned null");
        close();
        return;
    }

    m_driver.reset(driver);

    BH_LOG(app::logger(), DNET_LOG_INFO, "Inventory: Successfully loadded driver from %s", file_name);
    return;
}

void Inventory::close()
{
    if (m_handle != nullptr) {
        m_driver.reset();
        dlclose(m_handle);
        m_handle = nullptr;
    }
}

struct GetDcData
{
    // TODO: self
    GetDcData(Inventory & i, const std::string & a)
        :
        inv(i),
        addr(a)
    {}

    Inventory & inv;
    const std::string & addr;
    std::string result;
};

std::string Inventory::get_dc_by_host(const std::string & addr)
{
    GetDcData data(*this, addr);
    dispatch_sync_f(m_common_queue, &data, &Inventory::execute_get_dc_by_host);
    return data.result;
}

void Inventory::execute_get_dc_by_host(void *arg)
{
    // executed in common queue

    GetDcData & data = *(GetDcData *) arg;

    auto it = data.inv.m_host_info.find(data.addr);
    if (it != data.inv.m_host_info.end()) {
        BH_LOG(app::logger(), DNET_LOG_DEBUG, "Inventory: Found host '%s' in map, DC is '%s'",
                data.addr.c_str(), it->second.dc.c_str());
        data.result = it->second.dc;
        return;
    }

    if (data.inv.m_driver.get() == nullptr) {
        BH_LOG(app::logger(), DNET_LOG_NOTICE,
                "Have no inventory driver, defaulting DC=host='%s'", data.addr.c_str());
        data.result = data.addr;
        return;
    }

    HostInfo info;
    info.host = data.addr;
    data.inv.fetch_from_driver(info, false);
    data.result = info.dc;

    // update cache database in update queue
    dispatch_async_f(data.inv.m_update_queue, new CacheDbUpdateData(data.inv, info, false),
            &Inventory::execute_cache_db_update);
}

void Inventory::fetch_from_driver(HostInfo & info, bool update)
{
    if (m_driver == nullptr)
        return;

    BH_LOG(app::logger(), DNET_LOG_INFO, "Fetching DC of host '%s' from driver", info.host);

    std::string result;
    std::string error_text;
    result = m_driver->get_dc_by_host(info.host, error_text);

    // TODO: c_str
    if (result.empty()) {
        BH_LOG(app::logger(), DNET_LOG_ERROR,
                "Inventory: Cannot resolve DC for host '%s': %s", info.host, error_text);
        return;
    } else {
        BH_LOG(app::logger(), DNET_LOG_DEBUG,
                "Inventory: Resolved DC '%s' for host '%s'", result, info.host);
    }

    info.dc = result;
    info.timestamp = ::time(nullptr);
}

Inventory::HostInfo::HostInfo()
    : timestamp(0)
{}

int Inventory::HostInfo::init(const mongo::BSONObj & obj)
{
    for (mongo::BSONObj::iterator it = obj.begin(); it.more();) {
        mongo::BSONElement element = it.next();
        const char *field_name = element.fieldName();

        try {
            if (!std::strcmp(field_name, "host"))
                this->host = element.String();
            else if (!std::strcmp(field_name, "dc"))
                this->dc = element.String();
            else if (!std::strcmp(field_name, "timestamp"))
                this->timestamp = element.Double();

        } catch (const mongo::MsgAssertionException & e) {
            BH_LOG(app::logger(), DNET_LOG_ERROR,
                    "Initializing HostInfo from BSON: exception: '%s'", e.what());
            return -1;
        } catch (...) {
            BH_LOG(app::logger(), DNET_LOG_ERROR,
                    "Initializing HostInfo from BSON: unknown exception");
            return -1;
        }
    }

    if (host.empty() || dc.empty() || !timestamp) {
        BH_LOG(app::logger(), DNET_LOG_ERROR,
                "Incomplete HostInfo from inventory DB: host='%s' dc='%s' timestamp=%lu",
                host.c_str(), dc.c_str(), timestamp);
        return -1;
    }

    return 0;
}

int Inventory::cache_db_connect()
{
    try {
        const Config & config = app::config();

        if (config.metadata_url.empty() || config.inventory_db.empty()) {
            BH_LOG(app::logger(), DNET_LOG_WARNING,
                    "Not connecting to inventory database because it was not configured");
            return -1;
        }

        std::string errmsg;
        mongo::ConnectionString cs = mongo::ConnectionString::parse(config.metadata_url, errmsg);
        if (!cs.isValid()) {
            BH_LOG(app::logger(), DNET_LOG_ERROR,
                    "Mongo client ConnectionString error: %s", errmsg.c_str());
            return -1;
        }

        m_conn.reset((mongo::DBClientReplicaSet *) cs.connect(
                errmsg, double(config.metadata_connect_timeout_ms) / 1000.0));
        if (m_conn == nullptr) {
            BH_LOG(app::logger(), DNET_LOG_ERROR,
                    "Connection failed: %s", errmsg.c_str());
            return -1;
        }

        std::ostringstream collection_ostr;
        collection_ostr << config.inventory_db << ".inventory";
        m_collection_name = collection_ostr.str();

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

            HostInfo info;
            if (info.init(obj) == 0) {
                BH_LOG(app::logger(), DNET_LOG_INFO, "Loaded DC '%s' for host '%s' (updated at %lu)",
                        info.dc.c_str(), info.host.c_str(), info.timestamp);

                result.emplace_back(info);
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
    std::unique_ptr<CacheDbUpdateData> data(static_cast<CacheDbUpdateData*>(arg));
    data->self.cache_db_update(data->info, data->existing);
}

void Inventory::cache_db_update(const HostInfo & info, bool existing)
{
    // executed in update queue

    if (m_conn == nullptr)
        return;

    BH_LOG(app::logger(), DNET_LOG_INFO, "Adding host info to inventory database: "
            "host: '%s' DC: '%s' timestamp: %lu\n",
            info.host.c_str(), info.dc.c_str(), info.timestamp);

    try {
        mongo::BSONObjBuilder builder;
        builder.append("host", info.host);
        builder.append("dc", info.dc);
        builder.append("timestamp", double(info.timestamp));
        mongo::BSONObj obj = builder.obj();

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
