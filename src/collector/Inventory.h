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

#ifndef __bbd6c304_2898_4ea0_afae_ee8b16e21893
#define __bbd6c304_2898_4ea0_afae_ee8b16e21893

#include <memory>
#include <string>

#include <cocaine/framework/services/app.hpp>
#include <dispatch/dispatch.h>
#include <mongo/client/dbclient.h>

class Inventory
{
public:
    Inventory();
    ~Inventory();

    int init();

    void download_initial();

    void close();

    std::string get_dc_by_host(const std::string & addr);

private:
    struct HostInfo
    {
        HostInfo()
            : timestamp(0)
        {}

        HostInfo(const mongo::BSONObj & obj);

        mongo::BSONObj obj() const;

        std::string host;
        std::string dc;
        time_t timestamp;
    };

    int cache_db_connect();

    // Update host entry in database.
    // If 'existing' is true, update() is used, and insert() is used otherwise.
    void cache_db_update(const HostInfo & info, bool existing);

    void dispatch_next_reload();

    // Download host information from MongoDB.
    std::vector<HostInfo> load_cache_db();

    // Prepare complete host information.
    std::vector<HostInfo> load_hosts();

    // Functions below are executed in common queue.
    struct GetDcData;
    static void execute_get_dc_by_host(void *arg);
    static void execute_reload(void *arg);
    struct SaveUpdateData;
    static void execute_save_update(void *arg);

    // Functions below are executed in update queue.
    struct CacheDbUpdateData;
    static void execute_cache_db_update(void *arg);

    void fetch_from_cocaine(HostInfo & info);

private:
    std::map<std::string, HostInfo> m_host_info;

    std::shared_ptr<cocaine::framework::service_manager_t> m_manager;
    std::shared_ptr<cocaine::framework::app_service_t> m_service;

    std::unique_ptr<mongo::DBClientReplicaSet> m_conn;
    std::string m_collection_name;
    double m_last_update_time;

    dispatch_queue_t m_common_queue;
    dispatch_queue_t m_update_queue;
};

#endif

