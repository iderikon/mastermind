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
#include "Group.h"
#include "Node.h"
#include "Storage.h"
#include "WorkerApplication.h"

namespace {

template<typename T>
void remove_duplicates(std::vector<T> & v)
{
    std::sort(v.begin(), v.end());
    auto it = std::unique(v.begin(), v.end());
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
    std::sort(couples.begin(), couples.end());
    std::sort(groups.begin(), groups.end());
    std::sort(backends.begin(), backends.end());
    std::sort(nodes.begin(), nodes.end());
    std::sort(filesystems.begin(), filesystems.end());
}

Storage::Storage(WorkerApplication & app)
    : m_app(app)
{}

Storage::Storage(const Storage & other)
    :
    m_app(other.m_app)
{
    bool have_newer;
    merge(other, have_newer);
}

Storage::~Storage()
{}

bool Storage::add_node(const char *host, int port, int family)
{
    std::ostringstream node_id;
    node_id << host << ':' << port << ':' << family;
    const char *node_id_str = node_id.str().c_str();

    auto it = m_nodes.lower_bound(node_id.str());
    if (it != m_nodes.end() && it->first == node_id.str()) {
        BH_LOG(m_app.get_logger(), DNET_LOG_DEBUG, "Node %s already exists", node_id_str);
        return false;
    }

    BH_LOG(m_app.get_logger(), DNET_LOG_INFO, "New node %s", node_id_str);
    m_nodes.insert(it, std::make_pair(node_id.str(), Node(*this, host, port, family)));
    return true;
}

bool Storage::get_node(const std::string & key, Node *& node)
{
    auto it = m_nodes.find(key);
    if (it != m_nodes.end()) {
        node = &it->second;
        return true;
    }
    return false;
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

    backend.set_group(&it->second);
}

bool Storage::get_group(int id, Group *& group)
{
    auto it = m_groups.find(id);
    if (it != m_groups.end()) {
        group = &it->second;
        return true;
    }
    return false;
}

Group & Storage::get_group(int id)
{
    auto it = m_groups.lower_bound(id);
    if (it == m_groups.end() || it->first != id)
        it = m_groups.insert(it, std::make_pair(id, Group(id)));
    return it->second;
}

bool Storage::get_couple(const std::string & key, Couple *& couple)
{
    auto it = m_couples.find(key);
    if (it != m_couples.end()) {
        couple = &it->second;
        return true;
    }
    return false;
}

Namespace & Storage::get_namespace(const std::string & name)
{
    auto it = m_namespaces.lower_bound(name);
    if (it == m_namespaces.end() || it->first != name)
        it = m_namespaces.insert(it, std::make_pair(name, Namespace(name)));
    return it->second;
}

bool Storage::get_namespace(const std::string & name, Namespace *& ns)
{
    auto it = m_namespaces.find(name);
    if (it != m_namespaces.end()) {
        ns = &it->second;
        return true;
    }
    return false;
}

void Storage::update()
{
    BH_LOG(m_app.get_logger(), DNET_LOG_INFO, "Storage: updating filesystems, groups, and couples");

    for (auto it = m_nodes.begin(); it != m_nodes.end(); ++it)
        it->second.update_filesystems();

    for (auto it = m_groups.begin(); it != m_groups.end(); ++it) {
        Group & group = it->second;
        if (group.parse_metadata() != 0)
            continue;

        const std::string & metadata_ns = group.get_namespace_name();
        Namespace *ns = group.get_namespace();
        if (ns == nullptr || ns->get_name() != metadata_ns) {
            ns = &get_namespace(metadata_ns);
            group.set_namespace(ns);
            ns->add_group(group);
        }

        group.update_status(m_app.get_config().forbidden_dht_groups);
    }

    for (auto it = m_groups.begin(); it != m_groups.end(); ++it) {
        Group & group = it->second;
        if (!group.metadata_parsed())
            continue;

        const std::vector<int> & group_ids = group.get_couple_group_ids();
        if (group_ids.empty())
            continue;

        std::vector<Group*> groups(group_ids.size());
        for (size_t i = 0; i < group_ids.size(); ++i) {
            int id = group_ids[i];
            if (id == it->first)
                groups[i] = &group;
            else
                groups[i] = &get_group(id);
        }

        Couple *couple0 = groups[0]->get_couple();
        if (couple0 != nullptr) {
            bool equal = true;
            for (size_t i = 1; i < groups.size(); ++i) {
                if (groups[i]->get_couple() != couple0) {
                    equal = false;
                    break;
                }
            }
            if (equal)
                continue;
        }

        bool md_ok = true;
        for (size_t i = 1; i < groups.size(); ++i) {
            if (groups[0]->check_metadata_equals(*groups[i]) != 0) {
                md_ok = false;
                break;
            }
        }
        if (!md_ok)
            continue;

        std::string key = CoupleKey(group_ids);
        auto it = m_couples.lower_bound(key);
        if (it == m_couples.end() || it->first != key) {
            it = m_couples.insert(it, std::make_pair(key, Couple(groups)));
            for (Group *group : groups)
                group->set_couple(&it->second);
        }
    }

    for (auto it = m_couples.begin(); it != m_couples.end(); ++it)
        it->second.update_status(m_app.get_config().forbidden_unmatched_group_total_space);

    BH_LOG(m_app.get_logger(), DNET_LOG_INFO, "Storage update completed");
}

#if 0
void Storage::update(const Entries & entries)
{
    BH_LOG(m_app.get_logger(), DNET_LOG_INFO, "Storage: updating filesystems, groups, and couples");

    for (Node *node : entries.nodes)
        node->update_filesystems();

    for (Group *group : entries.groups)
        group->process_metadata();

    for (Couple *couple : entries.couples)
        couple->update_status();

    BH_LOG(m_app.get_logger(), DNET_LOG_INFO, "Storage update completed");
}
#endif

template<typename SOURCE_ITEM, typename RESULT_ITEM>
void Storage::filter_items(std::vector<SOURCE_ITEM*> & source_items,
        std::vector<RESULT_ITEM*> & result_items,
        bool & first_pass)
{
    if (result_items.empty() && !first_pass)
        return;

    std::vector<RESULT_ITEM*> all_items;
    for (SOURCE_ITEM *source_item : source_items)
        source_item->get_items(all_items);

    if (first_pass) {
        result_items = all_items;
        first_pass = false;
        return;
    }

    remove_duplicates(all_items);
    std::vector<RESULT_ITEM*> result_tmp(all_items.size());

    auto it = std::set_intersection(result_items.begin(), result_items.end(),
            all_items.begin(), all_items.end(), result_tmp.begin());
    result_tmp.resize(it - result_tmp.begin());

    result_items = result_tmp;
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

    std::vector<Backend*> backends;
    std::vector<FS*> filesystems;
    for (auto nit = node_item_ids.begin(); nit != node_item_ids.end(); ++nit) {
        Node *node = nullptr;
        if (!get_node(nit->first, node) || node == nullptr)
            continue;

        const std::vector<uint64_t> & backend_ids = nit->second.first;
        const std::vector<uint64_t> & fs_ids = nit->second.second;
        for (uint64_t backend_id : backend_ids) {
            Backend *backend = nullptr;
            if (node->get_backend(backend_id, backend) && backend != nullptr)
                backends.push_back(backend);
        }
        for (uint64_t fsid : fs_ids) {
            FS *fs = nullptr;
            if (node->get_fs(fsid, fs) && fs != nullptr)
                filesystems.push_back(fs);
        }
    }

    std::vector<Namespace*> namespaces;
    std::vector<Couple*> couples;
    std::vector<Group*> groups;
    std::vector<Node*> nodes;

    for (const std::string & name : filter.namespaces) {
        Namespace *ns = nullptr;
        if (get_namespace(name, ns) && ns != nullptr)
            namespaces.push_back(ns);
    }
    for (const std::string & key : filter.couples) {
        Couple *couple = nullptr;
        if (get_couple(key, couple) && couple != nullptr)
            couples.push_back(couple);
    }
    for (int id : filter.groups) {
        Group *group = nullptr;
        if (get_group(id, group) && group != nullptr)
            groups.push_back(group);
    }
    for (const std::string & key : filter.nodes) {
        Node *node = nullptr;
        if (get_node(key, node) && node != nullptr)
            nodes.push_back(node);
    }

    if (filter.item_types & Filter::Group) {
        if (!filter.groups.empty()) {
            entries.groups = groups;
        } else {
            bool first_pass = true;

            if (!(filter.item_types & Filter::Couple) && !couples.empty())
                filter_items(couples, entries.groups, first_pass);

            if (!(filter.item_types & Filter::Backend) && !backends.empty())
                filter_items(backends, entries.groups, first_pass);

            if (!(filter.item_types & Filter::FS) && !filesystems.empty())
                filter_items(filesystems, entries.groups, first_pass);

            if (!(filter.item_types & Filter::Node) && !nodes.empty())
                filter_items(nodes, entries.groups, first_pass);

            if (!(filter.item_types & Filter::Namespace) && !namespaces.empty())
                filter_items(namespaces, entries.groups, first_pass);
        }
    }

    if (filter.item_types & Filter::Couple) {
        if (!filter.couples.empty()) {
            entries.couples = couples;
        } else {
            bool first_pass = true;

            if (!(filter.item_types & Filter::Group) && !groups.empty())
                filter_items(groups, entries.couples, first_pass);

            if (!(filter.item_types & Filter::Backend) && !backends.empty())
                filter_items(backends, entries.couples, first_pass);

            if (!(filter.item_types & Filter::FS) && !filesystems.empty())
                filter_items(filesystems, entries.couples, first_pass);

            if (!(filter.item_types & Filter::Node) && !nodes.empty())
                filter_items(nodes, entries.couples, first_pass);

            if (!(filter.item_types & Filter::Namespace) && !namespaces.empty())
                filter_items(namespaces, entries.couples, first_pass);
        }
    }

    if (filter.item_types & Filter::Node) {
        if (!filter.nodes.empty()) {
            entries.nodes = nodes;
        } else {
            bool first_pass = true;

            if (!(filter.item_types & Filter::Group) && !groups.empty())
                filter_items(groups, entries.nodes, first_pass);

            if (!(filter.item_types & Filter::Backend) && !backends.empty())
                filter_items(backends, entries.nodes, first_pass);

            if (!(filter.item_types & Filter::FS) && !filesystems.empty())
                filter_items(filesystems, entries.nodes, first_pass);

            if (!(filter.item_types & Filter::Couple) && !couples.empty())
                filter_items(couples, entries.nodes, first_pass);

            if (!(filter.item_types & Filter::Namespace) && !namespaces.empty())
                filter_items(namespaces, entries.nodes, first_pass);
        }
    }

    if (filter.item_types & Filter::Backend) {
        if (!filter.backends.empty()) {
            entries.backends = backends;
        } else {
            bool first_pass = true;

            if (!(filter.item_types & Filter::Group) && !groups.empty())
                filter_items(groups, entries.backends, first_pass);

            if (!(filter.item_types & Filter::Node) && !nodes.empty())
                filter_items(nodes, entries.backends, first_pass);

            if (!(filter.item_types & Filter::FS) && !filesystems.empty())
                filter_items(filesystems, entries.backends, first_pass);

            if (!(filter.item_types & Filter::Couple) && !couples.empty())
                filter_items(couples, entries.backends, first_pass);

            if (!(filter.item_types & Filter::Namespace) && !namespaces.empty())
                filter_items(namespaces, entries.backends, first_pass);
        }
    }

    if (filter.item_types & Filter::FS) {
        if (!filter.filesystems.empty()) {
            entries.filesystems = filesystems;
        } else {
            bool first_pass = true;

            if (!(filter.item_types & Filter::Group) && !groups.empty())
                filter_items(groups, entries.filesystems, first_pass);

            if (!(filter.item_types & Filter::Node) && !nodes.empty())
                filter_items(nodes, entries.filesystems, first_pass);

            if (!(filter.item_types & Filter::Backend) && !backends.empty())
                filter_items(backends, entries.filesystems, first_pass);

            if (!(filter.item_types & Filter::Couple) && !couples.empty())
                filter_items(couples, entries.filesystems, first_pass);

            if (!(filter.item_types & Filter::Namespace) && !namespaces.empty())
                filter_items(namespaces, entries.filesystems, first_pass);
        }
    }

    if (filter.item_types & Filter::Namespace) {
        if (!filter.namespaces.empty()) {
            entries.namespaces = namespaces;
        } else {
            bool first_pass = true;

            if (!(filter.item_types & Filter::Group) && !groups.empty())
                filter_items(groups, entries.namespaces, first_pass);

            if (!(filter.item_types & Filter::Node) && !nodes.empty())
                filter_items(nodes, entries.namespaces, first_pass);

            if (!(filter.item_types & Filter::Backend) && !backends.empty())
                filter_items(backends, entries.namespaces, first_pass);

            if (!(filter.item_types & Filter::Couple) && !couples.empty())
                filter_items(couples, entries.namespaces, first_pass);

            if (!(filter.item_types & Filter::FS) && !filesystems.empty())
                filter_items(filesystems, entries.namespaces, first_pass);
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
        for (Node *node : entries.nodes)
            node->print_json(writer, entries.backends, entries.filesystems,
                    !!(item_types & Filter::Backend), !!(item_types & Filter::FS), show_internals);
        writer.EndArray();
    }

    if (!entries.groups.empty()) {
        writer.Key("groups");
        writer.StartArray();
        for (Group *group : entries.groups)
            group->print_json(writer, show_internals);
        writer.EndArray();
    }

    if (!entries.couples.empty()) {
        writer.Key("couples");
        writer.StartArray();
        for (Couple *couple : entries.couples)
            couple->print_json(writer, show_internals);
        writer.EndArray();
    }

    if (!entries.namespaces.empty()) {
        writer.Key("namespaces");
        writer.StartArray();
        for (Namespace *ns : entries.namespaces)
            writer.String(ns->get_name().c_str());
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
            it->second.print_json(writer, std::vector<Backend*>(), std::vector<FS*>(),
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
}

void Storage::update_group_structure()
{
    BH_LOG(m_app.get_logger(), DNET_LOG_INFO, "Updating group structure");

    for (auto it = m_nodes.begin(); it != m_nodes.end(); ++it) {
        Node & node = it->second;
        std::vector<Backend*> backends = node.pick_new_backends();
        for (Backend *backend : backends)
            handle_backend(*backend);
    }
}

void Storage::merge_groups(const Storage & other_storage, bool & have_newer)
{
    update_group_structure();

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
            const std::vector<Group*> & other_groups = other_couple.get_groups();

            std::vector<Group*> my_groups;
            my_groups.reserve(other_groups.size());

            for (size_t i = 0; i < other_groups.size(); ++i) {
                int id = other_groups[i]->get_id();
                auto gr = m_groups.find(id);

                if (gr != m_groups.end()) {
                    my_groups.push_back(&gr->second);
                } else {
                    BH_LOG(m_app.get_logger(), DNET_LOG_ERROR,
                            "Merge storage: internal inconsistency: have no group %d for couple", id);
                }
            }

            my = m_couples.insert(my, std::make_pair(other_couple.get_key(), Couple(my_groups)));
            Couple & my_couple = my->second;

            for (Group *group : my_groups)
                group->set_couple(&my_couple);

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
    merge_groups(other, have_newer);
    merge_couples(other, have_newer);
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
