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

#ifndef __f8057e8e_b6f5_475b_bb31_771928953bf8
#define __f8057e8e_b6f5_475b_bb31_771928953bf8

#include "Couple.h"
#include "Group.h"
#include "Namespace.h"
#include "Node.h"
#include "RWMutex.h"

#include <cstring>
#include <map>
#include <memory>
#include <utility>
#include <vector>

#include <elliptics/logger.hpp>

namespace ioremap {
    namespace elliptics {
        class session;
    }
}

class BackendStat;
class WorkerApplication;
class on_get_snapshot;
class on_refresh;

class Storage
{
public:
    struct Entries
    {
        std::vector<Couple*> couples;
        std::vector<Group*> groups;
        std::vector<Backend*> backends;
        std::vector<Node*> nodes;
        std::vector<FS*> filesystems;

        void sort();
    };

public:
    Storage(WorkerApplication & app);
    ~Storage();

    WorkerApplication & get_app()
    { return m_app; }

    bool add_node(const char *host, int port, int family);
    void get_nodes(std::vector<Node*> & nodes);
    bool get_node(const std::string & key, Node *& node);

    void handle_backend(Backend & backend, bool existed);

    void get_group_ids(std::vector<int> & groups) const;
    void get_groups(std::vector<Group*> & groups);
    bool get_group(int id, Group *& group);

    void get_couples(std::vector<Couple*> & couples);

    Namespace *get_namespace(const std::string & name);
    bool get_namespace(const std::string & name, Namespace *& ns);
    void get_namespaces(std::vector<Namespace*> & namespaces);

    void schedule_update(ioremap::elliptics::session & session);
    void schedule_refresh(const Entries & entries, ioremap::elliptics::session & session,
            std::shared_ptr<on_refresh> handler);

    void update_filesystems();
    void update_groups();
    void update_couples();

    // group_ids must be sorted
    void create_couple(const std::vector<int> & groups_ids, Group *group);

    void arm_timer();

    void select(Filter & filter, Entries & entries);

    void get_snapshot(const Filter & filter, std::shared_ptr<on_get_snapshot> handler);
    void refresh(const Filter & filter, std::shared_ptr<on_refresh> handler);

    void print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer,
            Entries & entries, uint32_t item_types);

public:
    class ScheduleMetadataDownload;
    class UpdateJob;
    class UpdateJobToggle;

private:
    void schedule_metadata_download(ioremap::elliptics::session & session,
            UpdateJobToggle *toggle, Group *group);

public:
    struct CoupleKey
    {
        CoupleKey(const std::vector<int> & ids)
            : group_ids(ids)
        {}

        bool operator < (const CoupleKey & other) const
        {
            size_t my_size = group_ids.size();
            size_t oth_size = other.group_ids.size();

            if (my_size < oth_size)
                return true;

            if (my_size > oth_size)
                return false;

            return std::memcmp(group_ids.data(),
                    other.group_ids.data(), my_size * sizeof(int)) < 0;
        }

        bool operator == (const CoupleKey & other) const
        {
            return group_ids == other.group_ids;
        }

        bool operator != (const CoupleKey & other) const
        {
            return !operator == (other);
        }

        std::vector<int> group_ids;
    };

    struct ClockStat
    {
        uint64_t schedule_update_time;
        uint64_t schedule_update_clk;
        uint64_t metadata_download_total_time;
        uint64_t status_update_time;
    };

    const ClockStat & get_clock_stat() const
    { return m_clock; }

private:
    WorkerApplication & m_app;

    std::map<std::string, Node> m_nodes;
    mutable RWMutex m_nodes_lock;

    std::map<int, Group> m_groups;
    mutable RWMutex m_groups_lock;

    std::map<CoupleKey, Couple> m_couples;
    mutable RWMutex m_couples_lock;

    std::map<std::string, Namespace> m_namespaces;
    mutable RWSpinLock m_namespaces_lock;

    friend class ScheduleMetadataDownload;
    friend class UpdateJob;
    friend class UpdateJobToggle;
    ClockStat m_clock;
};

#endif

