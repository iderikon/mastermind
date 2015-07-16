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
class Couple;
class Group;
class WorkerApplication;

class Storage
{
public:
    Storage(WorkerApplication & app);
    ~Storage();

    WorkerApplication & get_app()
    { return m_app; }

    void get_nodes(std::vector<Node *> & nodes);
    bool add_node(const char *host, int port, int family);

    void handle_backend(BackendStat & backend);

    void get_group_ids(std::vector<int> & groups) const;
    void get_groups(std::vector<Group*> & groups) const;
    void get_couples(std::vector<Couple*> & couples) const;

    void schedule_update_groups_and_couples(ioremap::elliptics::session & session);
    void update_groups();
    void update_couples();

    // group_ids must be sorted
    void create_couple(const std::vector<int> & groups_ids, Group *group);

    void arm_timer();

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

private:
    WorkerApplication & m_app;

    std::map<std::string, Node> m_nodes;
    mutable RWMutex m_nodes_lock;

    std::map<int, Group*> m_groups;
    mutable RWMutex m_groups_lock;

    std::map<CoupleKey, Couple*> m_couples;
    mutable RWMutex m_couples_lock;
};

#endif

