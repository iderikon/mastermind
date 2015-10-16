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

Inventory::Inventory()
    :
    m_handle(nullptr),
    m_last_update_time(0.0)
{
    m_queue = dispatch_queue_create("inventory", 0);
}

Inventory::~Inventory()
{
    m_driver.reset();
    if (m_handle != nullptr)
        dlclose(m_handle);
    dispatch_release(m_queue);
}

int Inventory::init()
{
    if (!app::config().collector_inventory.empty()) {
        BH_LOG(app::logger(), DNET_LOG_INFO, "Opening inventory driver at %s",
                app::config().collector_inventory.c_str());
        if (open_driver(app::config().collector_inventory) == 0)
            cache_db_connect();
    }
    return 0;
}

void Inventory::download_initial()
{
    dispatch_sync_f(m_queue, this, &Inventory::execute_reload);
}

void Inventory::execute_reload(void *arg)
{
    BH_LOG(app::logger(), DNET_LOG_INFO, "Reloading cache");

    Inventory & self = *(Inventory *) arg;

    std::vector<HostInfo> hosts = self.load_cache_db();
    time_t now = ::time(nullptr);

    for (HostInfo & info : hosts) {
        if (now > info.timestamp && (now - info.timestamp) > app::config().infrastructure_dc_cache_valid_time) {
            std::string addr = info.host;
            self.fetch_from_driver(addr, true);
        } else {
            self.m_host_info[info.host] = info;
        }
    }

    if (self.m_driver == nullptr)
        return;

    dispatch_after_f(dispatch_time(DISPATCH_TIME_NOW,
                app::config().infrastructure_dc_cache_update_period * 1000000000ULL),
            self.m_queue, arg, &Inventory::execute_reload);
}

int Inventory::open_driver(const std::string & file_name)
{
    if (m_handle != nullptr) {
        BH_LOG(app::logger(), DNET_LOG_ERROR,
                "Internal error: Inventory driver is already opened");
        return -1;
    }

    m_handle = dlopen(file_name.c_str(), RTLD_LAZY | RTLD_LOCAL);

    if (m_handle == nullptr) {
        const char *err = dlerror();
        BH_LOG(app::logger(), DNET_LOG_ERROR,
                "Inventory: dlopen() failed for '%s': %s",
                file_name.c_str(), (err != nullptr ? err : "unknown error"));
        return -1;
    }

    InventoryDriver *(*create_inventory)() = (InventoryDriver *(*)()) dlsym(m_handle, "create_inventory");
    if (create_inventory == nullptr) {
        const char *err = dlerror();
        BH_LOG(app::logger(), DNET_LOG_ERROR,
                "Inventory: Cannot find symbol 'create_inventory' in '%s': '%s'",
                file_name.c_str(), (err != nullptr ? err : "unknown error"));
        close();
        return -1;
    }

    InventoryDriver *driver = create_inventory();
    if (driver == nullptr) {
        BH_LOG(app::logger(), DNET_LOG_ERROR, "Inventory: inventory_create() returned null");
        close();
        return -1;
    }

    m_driver.reset(driver);

    return 0;
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
    dispatch_sync_f(m_queue, &data, &Inventory::execute_get_dc_by_host);
    return data.result;
}

void Inventory::execute_get_dc_by_host(void *arg)
{
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

    data.result = data.inv.fetch_from_driver(data.addr, false);
}

std::string Inventory::fetch_from_driver(const std::string & addr, bool update)
{
    if (m_driver == nullptr)
        return std::string();

    BH_LOG(app::logger(), DNET_LOG_INFO,
            "Fetching DC of host '%s' from driver", addr.c_str());

    std::string result;
    std::string error_text;
    result = m_driver->get_dc_by_host(addr, error_text);

    if (result.empty()) {
        BH_LOG(app::logger(), DNET_LOG_ERROR,
                "Inventory: Cannot resolve DC for host '%s': %s",
                addr.c_str(), error_text.c_str());
        return std::string();
    } else {
        BH_LOG(app::logger(), DNET_LOG_DEBUG,
                "Inventory: Resolved DC '%s' for host '%s'",
                result.c_str(), addr.c_str());
    }

    HostInfo & info = m_host_info[addr];
    info.host = addr;
    info.dc = result;
    info.timestamp = ::time(nullptr);

    cache_db_update(info, update);

    return result;
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

        if (config.metadata.url.empty() || config.metadata.inventory.db.empty()) {
            BH_LOG(app::logger(), DNET_LOG_WARNING,
                    "Not connecting to inventory database because it was not configured");
            return -1;
        }

        std::string errmsg;
        mongo::ConnectionString cs = mongo::ConnectionString::parse(config.metadata.url, errmsg);
        if (!cs.isValid()) {
            BH_LOG(app::logger(), DNET_LOG_ERROR,
                    "Mongo client ConnectionString error: %s", errmsg.c_str());
            return -1;
        }

        m_conn.reset((mongo::DBClientReplicaSet *) cs.connect(
                errmsg, double(config.metadata.options.connectTimeoutMS) / 1000.0));
        if (m_conn == nullptr) {
            BH_LOG(app::logger(), DNET_LOG_ERROR,
                    "Connection failed: %s", errmsg.c_str());
            return -1;
        }

        std::ostringstream collection_ostr;
        collection_ostr << config.metadata.inventory.db << ".inventory";
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

void Inventory::cache_db_update(const HostInfo & info, bool existing)
{
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
