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

#ifndef __f8057e8e_b6f5_475b_bb31_771928953bf8
#define __f8057e8e_b6f5_475b_bb31_771928953bf8

#include "Couple.h"
#include "Group.h"
#include "Namespace.h"
#include "Node.h"

#include <cstring>
#include <map>
#include <memory>
#include <utility>
#include <vector>

class WorkerApplication;

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
    Storage(const Storage & other);
    ~Storage();

    WorkerApplication & get_app()
    { return m_app; }

    bool add_node(const char *host, int port, int family);
    bool get_node(const std::string & key, Node *& node);
    std::map<std::string, Node> & get_nodes()
    { return m_nodes; }

    bool get_group(int id, Group *& group);
    Group & get_group(int id);
    std::map<int, Group> & get_groups()
    { return m_groups; }

    bool get_couple(const std::string & key, Couple *& couple);
    std::map<std::string, Couple> & get_couples()
    { return m_couples; }

    Namespace & get_namespace(const std::string & name);
    bool get_namespace(const std::string & name, Namespace *& ns);
    std::map<std::string, Namespace> & get_namespaces()
    { return m_namespaces; }

    // process newly received backends, i.e. create Group objects
    void update_group_structure();

    // process downloaded metadata, recalculate states, etc.
    void update();
    void update(const Entries & entries);

    // group_ids must be sorted
    void create_couple(const std::vector<int> & groups_ids, Group *group);

    // select entries matching filter
    void select(Filter & filter, Entries & entries);

    void merge(const Storage & other);
    void merge(const Entries & entries);

    void print_json(uint32_t item_types, std::string & str);
    void print_json(Filter & filter, std::string & str);

private:
    void handle_backend(Backend & backend);

    void print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer,
            Entries & entries, uint32_t item_types);

    void print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer,
            uint32_t item_types);

    template<typename SOURCE_ITEM, typename RESULT_ITEM>
    static void filter_items(std::vector<SOURCE_ITEM*> & source_items,
            std::vector<RESULT_ITEM*> & result_items,
            bool & first_pass);

public:
    template<typename T, typename K, typename V>
    static void merge_map(T & self, std::map<K, V> & map, const std::map<K, V> & other_map)
    {
        auto my = map.begin();
        auto other = other_map.begin();

        while (other != other_map.end()) {
            while (my != map.end() && my->first < other->first)
                ++my;
            if (my != map.end() && my->first == other->first) {
                my->second.merge(other->second);
            } else {
                my = map.insert(my, std::make_pair(other->first, V(self)));
                my->second.clone_from(other->second);
            }
            ++other;
        }
    }

    template<typename T, typename K, typename V>
    static void merge_map(T & self, std::map<K, V> & map, const std::vector<V*> & entries)
    {
        for (V *entry : entries) {
            auto my = map.lower_bound(entry->get_key());
            if (my != map.end() && my->first == entry->get_key()) {
                my->second.merge(*entry);
            } else {
                my = map.insert(my, std::make_pair(entry->get_key(), V(self)));
                my->second.clone_from(*entry);
            }
        }
    }

    static bool split_node_num(const std::string & key, std::string & node, uint64_t & id);

private:
    WorkerApplication & m_app;

    std::map<std::string, Node> m_nodes;
    std::map<int, Group> m_groups;
    std::map<std::string, Couple> m_couples;
    std::map<std::string, Namespace> m_namespaces;
};

#endif

