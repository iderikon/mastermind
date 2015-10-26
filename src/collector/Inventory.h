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

#include <dispatch/dispatch.h>
#include <mongo/client/dbclient.h>

class InventoryDriver
{
public:
    // This method is not guaranteed to be thread-safe.
    virtual std::string get_dc_by_host(const std::string & addr, std::string & error_text) = 0;
};

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

    // Open shared object file containing C-linkage function
    // create_inventory(void) returning InventoryDriver*
    void open_driver(const std::string & file_name);

    int cache_db_connect();

    void cache_db_update(const HostInfo & info, bool existing);

    void dispatch_next_reload();

    // Download host information from MongoDB.
    std::vector<HostInfo> load_cache_db();

    // Prepare complete host information.
    std::vector<HostInfo> load_hosts();

    // Functions below are executed in common queue.
    static void execute_get_dc_by_host(void *arg);
    static void execute_reload(void *arg);
    struct SaveUpdateData;
    static void execute_save_update(void *arg);

    // Functions below are executed in update queue.
    struct CacheDbUpdateData;
    static void execute_cache_db_update(void *arg);

    void fetch_from_driver(HostInfo & info, bool update);

private:
    std::map<std::string, HostInfo> m_host_info;

    void *m_handle;
    std::unique_ptr<InventoryDriver> m_driver;

    std::unique_ptr<mongo::DBClientReplicaSet> m_conn;
    std::string m_collection_name;
    double m_last_update_time;

    dispatch_queue_t m_common_queue;
    dispatch_queue_t m_update_queue;
};

#endif

