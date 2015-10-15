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

#include "Job.h"

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

class Job;
class WorkerApplication;

class Storage
{
public:
    struct Entries
    {
        std::vector<std::reference_wrapper<Couple>> couples;
        std::vector<std::reference_wrapper<Group>> groups;
        std::vector<std::reference_wrapper<Backend>> backends;
        std::vector<std::reference_wrapper<Node>> nodes;
        std::vector<std::reference_wrapper<FS>> filesystems;
        std::vector<std::reference_wrapper<Namespace>> namespaces;

        void sort();
    };

public:
    Storage(WorkerApplication & app);
    Storage(const Storage & other);
    ~Storage();

    WorkerApplication & get_app()
    { return m_app; }

    bool has_node(const char *host, int port, int family) const;
    Node & add_node(const char *host, int port, int family);

    std::map<std::string, Node> & get_nodes()
    { return m_nodes; }

    std::map<int, Group> & get_groups()
    { return m_groups; }

    std::map<std::string, Couple> & get_couples()
    { return m_couples; }

    std::map<std::string, Namespace> & get_namespaces()
    { return m_namespaces; }

    std::map<int, Job> & get_jobs()
    { return m_jobs; }

    // save jobs received from MongoDB
    void save_new_jobs(std::vector<Job> && new_jobs, uint64_t timestamp);

    // process newly received backends, i.e. create Group objects
    void update_group_structure();

    // process newly received jobs, bind them to groups, update existing jobs,
    // remove completed/cancelled jobs, unbind them from groups
    void process_new_jobs();

    // process downloaded metadata, recalculate states, etc.
    void update();

    void merge(const Storage & other, bool & have_newer);

    // select entries matching filter
    void select(Filter & filter, Entries & entries);

    void print_json(uint32_t item_types, bool show_internals, std::string & str);
    void print_json(Filter & filter, bool show_internals, std::string & str);

private:
    void handle_backend(Backend & backend);

    Namespace & get_namespace(const std::string & name);

    void merge_groups(const Storage & other_storage, bool & have_newer);
    void merge_jobs(const Storage & other_storage, bool & have_newer);
    void merge_couples(const Storage & other_storage, bool & have_newer);

    // find an intersection of sets of ResultItem objects
    // each of which is related to a set of SourceItem:s
    template<typename SourceItem, typename ResultItem>
    static void filter_related_items(
            std::vector<std::reference_wrapper<SourceItem>> & source_items,
            std::vector<std::reference_wrapper<ResultItem>> & current_set,
            bool & first_pass);

    void print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer,
            Entries & entries, uint32_t item_types, bool show_internals);

    void print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer,
            uint32_t item_types, bool show_internals);

public:
    template<typename T, typename K, typename V>
    static void merge_map(T & self, std::map<K, V> & map,
            const std::map<K, V> & other_map, bool & have_newer)
    {
        auto my = map.begin();
        auto other = other_map.begin();

        while (other != other_map.end()) {
            while (my != map.end() && my->first < other->first)
                ++my;
            if (my != map.end() && my->first == other->first) {
                my->second.merge(other->second, have_newer);
            } else {
                my = map.insert(my, std::make_pair(other->first, V(self)));
                my->second.clone_from(other->second);
            }
            ++other;
        }

        if (map.size() > other_map.size())
            have_newer = true;
    }

    static bool split_node_num(const std::string & key, std::string & node, uint64_t & id);

private:
    WorkerApplication & m_app;

    std::map<std::string, Node> m_nodes;
    std::map<int, Group> m_groups;
    std::map<std::string, Couple> m_couples;
    std::map<std::string, Namespace> m_namespaces;

    // array of jobs received from MongoDB but not processed yet
    std::vector<Job> m_new_jobs;
    // map group id -> job
    std::map<int, Job> m_jobs;
    // time database query was completed
    uint64_t m_jobs_timestamp;
};

#endif

