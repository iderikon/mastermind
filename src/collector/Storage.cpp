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

#include "Couple.h"
#include "Filter.h"
#include "FS.h"
#include "Node.h"
#include "WorkerApplication.h"

#include "Group.h"
#include "Storage.h"

#include <sstream>

namespace {

template <typename T>
struct RefLess
{
    bool operator () (const T & a, const T & b) const
    { return &a < &b; }
};

template <typename T>
struct RefEq
{
    bool operator () (const T & a, const T & b) const
    { return &a == &b; }
};

template<typename T>
void remove_duplicates(std::vector<std::reference_wrapper<T>> & v)
{
    std::sort(v.begin(), v.end(), RefLess<T>());
    auto it = std::unique(v.begin(), v.end(), RefEq<T>());
    v.erase(it, v.end());
}

class CoupleKey
{
public:
    CoupleKey(const std::vector<int> & group_ids)
        : m_group_ids(group_ids)
    {}

    operator const std::string()
    {
        if (m_group_ids.empty())
            return std::string();

        std::ostringstream ostr;
        size_t i = 0;
        for (; i < (m_group_ids.size() - 1); ++i)
            ostr << m_group_ids[i] << ':';
        ostr << m_group_ids[i];

        return ostr.str();
    }

private:
    const std::vector<int> & m_group_ids;
};

} // unnamed namespace

void Storage::Entries::sort()
{
    std::sort(couples.begin(), couples.end(), RefLess<Couple>());
    std::sort(groups.begin(), groups.end(), RefLess<Group>());
    std::sort(backends.begin(), backends.end(), RefLess<Backend>());
    std::sort(nodes.begin(), nodes.end(), RefLess<Node>());
    std::sort(filesystems.begin(), filesystems.end(), RefLess<FS>());
}

Storage::Storage(WorkerApplication & app)
    :
    m_app(app),
    m_jobs_timestamp(0)
{}

Storage::Storage(const Storage & other)
    :
    m_app(other.m_app),
    m_jobs_timestamp(0)
{
    bool have_newer;
    merge(other, have_newer);
}

Storage::~Storage()
{}

void Storage::add_node(const char *host, int port, int family)
{
    const std::string key = Node::key(host, port, family);

    auto it = m_nodes.lower_bound(key);
    if (it != m_nodes.end() && it->first == key) {
        BH_LOG(m_app.get_logger(), DNET_LOG_DEBUG, "Node %s already exists", key.c_str());
        return;
    }

    BH_LOG(m_app.get_logger(), DNET_LOG_INFO, "New node %s", key.c_str());
    m_nodes.insert(it, std::make_pair(key, Node(*this, host, port, family)));
}

void Storage::handle_backend(Backend & backend)
{
    auto it = m_groups.lower_bound(backend.get_stat().group);

    if (it != m_groups.end() && it->first == int(backend.get_stat().group)) {
        it->second.add_backend(backend);
    } else {
        it = m_groups.insert(it, std::make_pair(backend.get_stat().group, Group(backend.get_stat().group)));
        it->second.add_backend(backend);
    }

    backend.set_group(it->second);
}

void Storage::save_new_jobs(std::vector<Job> && new_jobs, uint64_t timestamp)
{
    assert(m_jobs_timestamp <= timestamp);

    m_new_jobs = std::move(new_jobs);
    m_jobs_timestamp = timestamp;
}

void Storage::update_group_structure()
{
    BH_LOG(m_app.get_logger(), DNET_LOG_INFO, "Updating group structure");

    for (auto it = m_nodes.begin(); it != m_nodes.end(); ++it) {
        Node & node = it->second;
        std::vector<std::reference_wrapper<Backend>> backends = node.pick_new_backends();
        for (Backend & backend : backends)
            handle_backend(backend);
    }
}

void Storage::process_new_jobs()
{
    std::map<int, Job>::iterator old = m_jobs.begin();
    std::vector<Job>::iterator fresh = m_new_jobs.begin();

    while (1) {
        if (old != m_jobs.end()) {
            if (fresh == m_new_jobs.end() || old->first < fresh->get_group_id()) {
                auto it = m_groups.find(old->first);
                if (it != m_groups.end())
                    it->second.clear_active_job();
                m_jobs.erase(old++);
                continue;
            } else if (old->first == fresh->get_group_id()) {
                old->second.update(*fresh);
                ++old;
                ++fresh;
                continue;
            }
        }
        if (fresh != m_new_jobs.end()) {
            m_jobs[fresh->get_group_id()] = *fresh;
            ++fresh;
        } else {
            break;
        }
    }

    m_new_jobs.clear();
}

Namespace & Storage::get_namespace(const std::string & name)
{
    auto it = m_namespaces.lower_bound(name);
    if (it == m_namespaces.end() || it->first != name)
        it = m_namespaces.insert(it, std::make_pair(name, Namespace(name)));
    return it->second;
}

void Storage::update()
{
    BH_LOG(m_app.get_logger(), DNET_LOG_INFO, "Storage: updating filesystems, groups, and couples");

    // Create/update filesystems depending on backend stats.
    for (auto it = m_nodes.begin(); it != m_nodes.end(); ++it)
        it->second.update_filesystems();

    // Process group metadata and jobs.
    for (auto it = m_groups.begin(); it != m_groups.end(); ++it) {
        Group & group = it->second;
        std::string old_namespace_name = group.get_namespace_name();

        // Bind or clear an active job.
        auto jit = m_jobs.find(group.get_id());
        if (jit == m_jobs.end())
            group.clear_active_job();
        else
            group.set_active_job(jit->second);

        if (group.parse_metadata() != 0)
            continue;

        const std::string & new_namespace_name = group.get_namespace_name();
        if (old_namespace_name != new_namespace_name) {
            if (!old_namespace_name.empty())
                get_namespace(old_namespace_name).remove_group(group);

            Namespace & new_ns = get_namespace(new_namespace_name);
            new_ns.add_group(group);
            group.set_namespace(new_ns);
        }

        group.update_status(m_app.get_config().forbidden_dht_groups);
    }

    // Create/update couples depending on changes in group metadata and structure
    for (auto it = m_groups.begin(); it != m_groups.end(); ++it) {
        Group & group = it->second;
        if (!group.metadata_parsed())
            continue;

        const std::vector<int> & group_ids = group.get_couple_group_ids();
        if (group_ids.empty())
            continue;

        std::vector<std::reference_wrapper<Group>> groups;
        groups.reserve(group_ids.size());
        for (int id : group_ids) {
            if (id == it->first) {
                groups.push_back(group);
            } else {
                auto git = m_groups.lower_bound(id);
                if (git == m_groups.end() || git->first != id) {
                    git = m_groups.insert(git, std::make_pair(id, Group(id)));

                    // result must be status=INIT, status_text="No node backends"
                    git->second.update_status(m_app.get_config().forbidden_dht_groups);
                }
                groups.push_back(git->second);
            }
        }

        // check if groups are associated with the same existing couple
        bool have_same_couples = true;
        for (size_t i = 1; i < groups.size(); ++i) {
            if (!groups[i].get().match_couple(groups[0])) {
                have_same_couples = false;
                break;
            }
        }
        if (have_same_couples)
            continue;

        bool md_ok = true;
        for (size_t i = 1; i < groups.size(); ++i) {
            if (groups[0].get().check_couple_equals(groups[i]) != 0) {
                md_ok = false;
                break;
            }
        }
        if (!md_ok)
            continue;

        std::string key = CoupleKey(group_ids);
        auto cit = m_couples.lower_bound(key);
        if (cit == m_couples.end() || cit->first != key) {
            cit = m_couples.insert(cit, std::make_pair(key, Couple(groups)));
            for (Group & group : groups)
                group.set_couple(cit->second);
        }
    }

    // Complete couple and group updates
    for (auto it = m_couples.begin(); it != m_couples.end(); ++it)
        it->second.update_status(m_app.get_config().forbidden_unmatched_group_total_space);

    BH_LOG(m_app.get_logger(), DNET_LOG_INFO, "Storage update completed");
}

void Storage::merge_groups(const Storage & other_storage, bool & have_newer)
{
    auto my = m_groups.begin();
    auto other = other_storage.m_groups.begin();

    while (other != other_storage.m_groups.end()) {
        while (my != m_groups.end() && my->first < other->first)
            ++my;
        if (my != m_groups.end() && my->first == other->first) {
            my->second.merge(other->second, have_newer);
        } else {
            const Group & other_group = other->second;
            my = m_groups.insert(my, std::make_pair(other->first, Group(other_group.get_id())));
            my->second.merge(other_group, have_newer);
        }
        ++other;
    }

    if (m_groups.size() > other_storage.m_groups.size())
        have_newer = true;
}

void Storage::merge_jobs(const Storage & other_storage, bool & have_newer)
{
    if (m_jobs_timestamp > other_storage.m_jobs_timestamp) {
        if (m_jobs.size() != other_storage.m_jobs.size()) {
            have_newer = true;
            return;
        }

        auto my = m_jobs.begin();
        auto other = other_storage.m_jobs.begin();
        while (my != m_jobs.end() && other != other_storage.m_jobs.end()) {
            if (my->first != other->first || !my->second.equals(other->second)) {
                have_newer = true;
                return;
            }
            ++my;
            ++other;
        }

        assert(my == m_jobs.end() && other == m_jobs.end());

        return;
    }

    if (m_jobs_timestamp == other_storage.m_jobs_timestamp)
        return;

    auto my = m_jobs.begin();
    auto other = other_storage.m_jobs.begin();

    while (1) {
        if (my != m_jobs.end()) {
            if (other == other_storage.m_jobs.end() || my->first < other->first) {
                auto it = m_groups.find(my->first);
                if (it != m_groups.end())
                    it->second.clear_active_job();
                m_jobs.erase(my++);
                continue;
            } else if (my->first == other->first) {
                my->second.merge(other->second, have_newer);
                ++my;
                ++other;
                continue;
            }
        }
        if (other != other_storage.m_jobs.end()) {
            m_jobs[other->first] = other->second;
            ++other;
        } else {
            break;
        }
    }

    m_jobs_timestamp = other_storage.m_jobs_timestamp;
}

void Storage::merge_couples(const Storage & other_storage, bool & have_newer)
{
    auto my = m_couples.begin();
    auto other = other_storage.m_couples.begin();
    while (other != other_storage.m_couples.end()) {
        while (my != m_couples.end() && my->first < other->first)
            ++my;
        if (my != m_couples.end() && my->first == other->first) {
            my->second.merge(other->second, have_newer);
        } else {
            const Couple & other_couple = other->second;
            const auto & other_groups = other_couple.get_groups();

            std::vector<std::reference_wrapper<Group>> my_groups;
            my_groups.reserve(other_groups.size());

            for (size_t i = 0; i < other_groups.size(); ++i) {
                int id = other_groups[i].get().get_id();
                auto gr = m_groups.find(id);

                if (gr != m_groups.end()) {
                    my_groups.push_back(gr->second);
                } else {
                    BH_LOG(m_app.get_logger(), DNET_LOG_ERROR,
                            "Merge storage: internal inconsistency: have no group %d for couple", id);
                }
            }

            my = m_couples.insert(my, std::make_pair(other_couple.get_key(), Couple(my_groups)));
            Couple & my_couple = my->second;

            for (Group & group : my_groups)
                group.set_couple(my_couple);

            my_couple.merge(other_couple, have_newer);
        }
        ++other;
    }
    if (m_couples.size() > other_storage.m_couples.size())
        have_newer = true;
}

void Storage::merge(const Storage & other, bool & have_newer)
{
    have_newer = false;

    merge_map(*this, m_nodes, other.m_nodes, have_newer);
    update_group_structure();
    merge_groups(other, have_newer);
    merge_jobs(other, have_newer);
    merge_couples(other, have_newer);
}

template<typename SourceItem, typename ResultItem>
void Storage::filter_related_items(
        std::vector<std::reference_wrapper<SourceItem>> & source_items,
        std::vector<std::reference_wrapper<ResultItem>> & current_set,
        bool & first_pass)
{
    // Intersection with empty set.
    if (current_set.empty() && !first_pass)
        return;

    // Obtain a set of related objects using overloaded method push_items().
    // For example, if we need to get all backends stored on a set of
    // nodes, SourceItem == Node, and ResultItem == Backend, so the method
    // Node::push_items(std::vector<std::reference_wrapper<Backend>>&)
    // will be called for each node.
    std::vector<std::reference_wrapper<ResultItem>> related_items;
    for (SourceItem & source_item : source_items)
        source_item.push_items(related_items);

    if (first_pass || related_items.empty()) {
        current_set = related_items;
        first_pass = false;
        return;
    }

    // push_items() may return duplicates.
    remove_duplicates(related_items);

    // Intersect the set of newly found items with the result of previous passes.
    struct {
        bool operator () (const std::reference_wrapper<ResultItem> & item1,
                const std::reference_wrapper<ResultItem> & item2) const
        { return &item1.get() < &item2.get(); }
    } comp;
    auto it = std::set_intersection(related_items.begin(), related_items.end(),
            current_set.begin(), current_set.end(),
            current_set.begin(),
            comp);
    current_set.erase(it, current_set.end());
}

void Storage::select(Filter & filter, Entries & entries)
{
    filter.sort();

    // map node -> pair(backend ids, fs ids)
    std::map<std::string, std::pair<std::vector<uint64_t>, std::vector<uint64_t>>> node_item_ids;
    for (const std::string & key : filter.backends) {
        std::string node;
        uint64_t backend_id;
        if (split_node_num(key, node, backend_id))
            node_item_ids[node].first.push_back(backend_id);
    }
    for (const std::string & key : filter.filesystems) {
        std::string node;
        uint64_t fsid;
        if (split_node_num(key, node, fsid))
            node_item_ids[node].second.push_back(fsid);
    }

    std::vector<std::reference_wrapper<Backend>> backends;
    std::vector<std::reference_wrapper<FS>> filesystems;
    for (auto nit = node_item_ids.begin(); nit != node_item_ids.end(); ++nit) {
        auto it = m_nodes.find(nit->first);
        if (it == m_nodes.end())
            continue;
        Node & node = it->second;

        const std::vector<uint64_t> & backend_ids = nit->second.first;
        const std::vector<uint64_t> & fs_ids = nit->second.second;
        for (uint64_t backend_id : backend_ids) {
            auto & node_backends = node.get_backends();
            auto it = node_backends.find(backend_id);
            if (it != node_backends.end())
                backends.push_back(it->second);
        }
        for (uint64_t fsid : fs_ids) {
            auto & node_filesystems = node.get_filesystems();
            auto it = node_filesystems.find(fsid);
            if (it != node_filesystems.end())
                filesystems.push_back(it->second);
        }
    }

    std::vector<std::reference_wrapper<Namespace>> namespaces;
    std::vector<std::reference_wrapper<Couple>> couples;
    std::vector<std::reference_wrapper<Group>> groups;
    std::vector<std::reference_wrapper<Node>> nodes;

    for (const std::string & name : filter.namespaces) {
        auto it = m_namespaces.find(name);
        if (it != m_namespaces.end())
            namespaces.push_back(it->second);
    }
    for (const std::string & key : filter.couples) {
        auto it = m_couples.find(key);
        if (it != m_couples.end())
            couples.push_back(it->second);
    }
    for (int id : filter.groups) {
        auto it = m_groups.find(id);
        if (it != m_groups.end())
            groups.push_back(it->second);
    }
    for (const std::string & key : filter.nodes) {
        auto it = m_nodes.find(key);
        if (it != m_nodes.end())
            nodes.push_back(it->second);
    }

    if (filter.item_types & Filter::Group) {
        if (!filter.groups.empty()) {
            entries.groups = groups;
        } else {
            bool first_pass = true;

            if (!(filter.item_types & Filter::Couple) && !couples.empty())
                filter_related_items(couples, entries.groups, first_pass);

            if (!(filter.item_types & Filter::Backend) && !backends.empty())
                filter_related_items(backends, entries.groups, first_pass);

            if (!(filter.item_types & Filter::FS) && !filesystems.empty())
                filter_related_items(filesystems, entries.groups, first_pass);

            if (!(filter.item_types & Filter::Node) && !nodes.empty())
                filter_related_items(nodes, entries.groups, first_pass);

            if (!(filter.item_types & Filter::Namespace) && !namespaces.empty())
                filter_related_items(namespaces, entries.groups, first_pass);
        }
    }

    if (filter.item_types & Filter::Couple) {
        if (!filter.couples.empty()) {
            entries.couples = couples;
        } else {
            bool first_pass = true;

            if (!(filter.item_types & Filter::Group) && !groups.empty())
                filter_related_items(groups, entries.couples, first_pass);

            if (!(filter.item_types & Filter::Backend) && !backends.empty())
                filter_related_items(backends, entries.couples, first_pass);

            if (!(filter.item_types & Filter::FS) && !filesystems.empty())
                filter_related_items(filesystems, entries.couples, first_pass);

            if (!(filter.item_types & Filter::Node) && !nodes.empty())
                filter_related_items(nodes, entries.couples, first_pass);

            if (!(filter.item_types & Filter::Namespace) && !namespaces.empty())
                filter_related_items(namespaces, entries.couples, first_pass);
        }
    }

    if (filter.item_types & Filter::Node) {
        if (!filter.nodes.empty()) {
            entries.nodes = nodes;
        } else {
            bool first_pass = true;

            if (!(filter.item_types & Filter::Group) && !groups.empty())
                filter_related_items(groups, entries.nodes, first_pass);

            if (!(filter.item_types & Filter::Backend) && !backends.empty())
                filter_related_items(backends, entries.nodes, first_pass);

            if (!(filter.item_types & Filter::FS) && !filesystems.empty())
                filter_related_items(filesystems, entries.nodes, first_pass);

            if (!(filter.item_types & Filter::Couple) && !couples.empty())
                filter_related_items(couples, entries.nodes, first_pass);

            if (!(filter.item_types & Filter::Namespace) && !namespaces.empty())
                filter_related_items(namespaces, entries.nodes, first_pass);
        }
    }

    if (filter.item_types & Filter::Backend) {
        if (!filter.backends.empty()) {
            entries.backends = backends;
        } else {
            bool first_pass = true;

            if (!(filter.item_types & Filter::Group) && !groups.empty())
                filter_related_items(groups, entries.backends, first_pass);

            if (!(filter.item_types & Filter::Node) && !nodes.empty())
                filter_related_items(nodes, entries.backends, first_pass);

            if (!(filter.item_types & Filter::FS) && !filesystems.empty())
                filter_related_items(filesystems, entries.backends, first_pass);

            if (!(filter.item_types & Filter::Couple) && !couples.empty())
                filter_related_items(couples, entries.backends, first_pass);

            if (!(filter.item_types & Filter::Namespace) && !namespaces.empty())
                filter_related_items(namespaces, entries.backends, first_pass);
        }
    }

    if (filter.item_types & Filter::FS) {
        if (!filter.filesystems.empty()) {
            entries.filesystems = filesystems;
        } else {
            bool first_pass = true;

            if (!(filter.item_types & Filter::Group) && !groups.empty())
                filter_related_items(groups, entries.filesystems, first_pass);

            if (!(filter.item_types & Filter::Node) && !nodes.empty())
                filter_related_items(nodes, entries.filesystems, first_pass);

            if (!(filter.item_types & Filter::Backend) && !backends.empty())
                filter_related_items(backends, entries.filesystems, first_pass);

            if (!(filter.item_types & Filter::Couple) && !couples.empty())
                filter_related_items(couples, entries.filesystems, first_pass);

            if (!(filter.item_types & Filter::Namespace) && !namespaces.empty())
                filter_related_items(namespaces, entries.filesystems, first_pass);
        }
    }

    if (filter.item_types & Filter::Namespace) {
        if (!filter.namespaces.empty()) {
            entries.namespaces = namespaces;
        } else {
            bool first_pass = true;

            if (!(filter.item_types & Filter::Group) && !groups.empty())
                filter_related_items(groups, entries.namespaces, first_pass);

            if (!(filter.item_types & Filter::Node) && !nodes.empty())
                filter_related_items(nodes, entries.namespaces, first_pass);

            if (!(filter.item_types & Filter::Backend) && !backends.empty())
                filter_related_items(backends, entries.namespaces, first_pass);

            if (!(filter.item_types & Filter::Couple) && !couples.empty())
                filter_related_items(couples, entries.namespaces, first_pass);

            if (!(filter.item_types & Filter::FS) && !filesystems.empty())
                filter_related_items(filesystems, entries.namespaces, first_pass);
        }
    }
}

void Storage::print_json(uint32_t item_types, bool show_internals, std::string & str)
{
    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);

    writer.StartObject();
    print_json(writer, item_types, show_internals);
    writer.EndObject();

    str = buf.GetString();
}

void Storage::print_json(Filter & filter, bool show_internals, std::string & str)
{
    filter.sort();

    Entries entries;
    select(filter, entries);

    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);

    writer.StartObject();
    print_json(writer, entries, filter.item_types, show_internals);
    writer.EndObject();

    str = buf.GetString();
}

void Storage::print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer,
        Entries & entries, uint32_t item_types, bool show_internals)
{
    entries.sort();

    if (!entries.nodes.empty()) {
        writer.Key("nodes");
        writer.StartArray();
        for (Node & node : entries.nodes)
            node.print_json(writer, entries.backends, entries.filesystems,
                    !!(item_types & Filter::Backend), !!(item_types & Filter::FS), show_internals);
        writer.EndArray();
    }

    if (!entries.groups.empty()) {
        writer.Key("groups");
        writer.StartArray();
        for (Group & group : entries.groups)
            group.print_json(writer, show_internals);
        writer.EndArray();
    }

    if (!entries.couples.empty()) {
        writer.Key("couples");
        writer.StartArray();
        for (Couple & couple : entries.couples)
            couple.print_json(writer, show_internals);
        writer.EndArray();
    }

    if (!entries.namespaces.empty()) {
        writer.Key("namespaces");
        writer.StartArray();
        for (Namespace & ns : entries.namespaces)
            writer.String(ns.get_name().c_str());
        writer.EndArray();
    }
}

void Storage::print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer,
        uint32_t item_types, bool show_internals)
{
    if (!!(item_types & (Filter::Node | Filter::Backend | Filter::FS))) {
        writer.Key("nodes");
        writer.StartArray();
        for (auto it = m_nodes.begin(); it != m_nodes.end(); ++it) {
            it->second.print_json(writer,
                    std::vector<std::reference_wrapper<Backend>>(),
                    std::vector<std::reference_wrapper<FS>>(),
                    !!(item_types & Filter::Backend), !!(item_types & Filter::FS), show_internals);
        }
        writer.EndArray();
    }

    if (!!(item_types & Filter::Group)) {
        writer.Key("groups");
        writer.StartArray();
        for (auto it = m_groups.begin(); it != m_groups.end(); ++it)
            it->second.print_json(writer, show_internals);
        writer.EndArray();
    }

    if (!!(item_types & Filter::Couple)) {
        writer.Key("couples");
        writer.StartArray();
        for (auto it = m_couples.begin(); it != m_couples.end(); ++it)
            it->second.print_json(writer, show_internals);
        writer.EndArray();
    }

    if (!!(item_types & Filter::Namespace)) {
        writer.Key("namespaces");
        writer.StartArray();
        for (auto it = m_namespaces.begin(); it != m_namespaces.end(); ++it)
            writer.String(it->first.c_str());
        writer.EndArray();
    }

    if (!!(item_types & Filter::Job)) {
        writer.Key("jobs");
        writer.StartArray();
        for (auto it = m_jobs.begin(); it != m_jobs.end(); ++it)
            it->second.print_json(writer);
        writer.EndArray();
    }
}

bool Storage::split_node_num(const std::string & key, std::string & node, uint64_t & id)
{
    size_t slash_pos = key.rfind('/');
    if (slash_pos == std::string::npos)
        return false;

    node = key.substr(0, slash_pos);

    std::string id_str = key.substr(slash_pos + 1);
    try {
        id = std::stol(id_str);
    } catch (...) {
        return false;
    }

    return true;
}
