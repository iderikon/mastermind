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
    merge(other);
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
        it = m_groups.insert(it, std::make_pair(backend.get_stat().group, Group(*this, backend.get_stat().group)));
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
        it = m_groups.insert(it, std::make_pair(id, Group(*this, id)));
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
    BH_LOG(m_app.get_logger(), DNET_LOG_INFO, "Storage: updating filesystems, groups and couples");

    for (auto it = m_nodes.begin(); it != m_nodes.end(); ++it)
        it->second.update_filesystems();

    for (auto it = m_groups.begin(); it != m_groups.end(); ++it)
        it->second.process_metadata();

    for (auto it = m_couples.begin(); it != m_couples.end(); ++it)
        it->second.update_status();

    BH_LOG(m_app.get_logger(), DNET_LOG_INFO, "Storage update completed");
}

void Storage::create_couple(const std::vector<int> & group_ids, Group *group)
{
    std::vector<Group*> groups;
    groups.reserve(group_ids.size());

    for (int id : group_ids) {
        if (id == group->get_id()) {
            groups.push_back(group);
        } else {
            auto it = m_groups.lower_bound(id);
            if (it == m_groups.end() || it->first != id)
                it = m_groups.insert(it, std::make_pair(id, Group(*this, id)));
            groups.push_back(&it->second);
        }
    }

    std::string key = CoupleKey(group_ids);

    auto it = m_couples.lower_bound(key);
    if (it == m_couples.end() || it->first != key) {
        it = m_couples.insert(it, std::make_pair(key, Couple(*this, groups)));

        Couple & couple = it->second;
        couple.bind_groups();
        Namespace *ns = group->get_namespace();
        if (ns != nullptr)
            ns->add_couple(couple);
    }
}

void Storage::select(Filter & filter, Entries & entries)
{
    filter.sort();

    bool have_groups = false;
    bool have_couples = false;
    bool have_nodes = false;
    bool have_backends = false;
    bool have_fs = false;

    if (filter.item_types & Filter::Group) {
        if (!filter.groups.empty()) {
            std::vector<Group*> candidate_groups;
            for (int id : filter.groups) {
                auto it = m_groups.find(id);
                if (it != m_groups.end()) {
                    Group & group = it->second;
                    if (group.match(filter, ~Filter::Group))
                        entries.groups.push_back(&group);
                }
            }
            have_groups = true;
        }
    }

    if (filter.item_types & Filter::Couple) {
        if (!filter.couples.empty()) {
            std::vector<int> group_ids;
            group_ids.reserve(4);

            for (const std::string & couple_key_str : filter.couples) {
                if (couple_key_str.empty())
                    continue;

                size_t start = 0;
                size_t pos;
                do {
                    pos = couple_key_str.find(':', start);
                    group_ids.push_back(std::stoi(couple_key_str.substr(start, pos - start)));
                    start = pos + 1;
                } while (pos != std::string::npos);

                auto it = m_couples.find(CoupleKey(group_ids));
                if (it != m_couples.end()) {
                    Couple & couple = it->second;
                    if (couple.match(filter, ~Filter::Couple))
                        entries.couples.push_back(&couple);
                }

                group_ids.clear();
            }
            have_couples = true;
        } else if (have_groups) {
            std::vector<Couple*> candidate_couples;
            for (Group *group : entries.groups) {
                if (group->get_couple() != nullptr)
                    candidate_couples.push_back(group->get_couple());
            }
            remove_duplicates(candidate_couples);
            for (Couple *couple : candidate_couples) {
                if (couple->match(filter, ~Filter::Group))
                    entries.couples.push_back(couple);
            }
            have_couples = true;
        } else if (!filter.namespaces.empty()) {
            for (const std::string & name : filter.namespaces) {
                Namespace *ns;
                if (get_namespace(name, ns)) {
                    const std::set<Couple*> & couples = ns->get_couples();
                    for (Couple *couple : couples) {
                        if (couple->match(filter, ~Filter::Namespace))
                            entries.couples.push_back(couple);
                    }
                }
            }
            have_couples = true;
        } else {
            for (auto it = m_couples.begin(); it != m_couples.end(); ++it) {
                Couple & couple = it->second;
                if (couple.match(filter))
                    entries.couples.push_back(&couple);
            }
        }
    }

    if (have_couples && !have_groups && (filter.item_types & Filter::Group)) {
        // XXX
        std::vector<Group*> candidate_groups;
        for (Couple *couple : entries.couples) {
            couple->get_groups(candidate_groups);
            for (Group *group : candidate_groups) {
                if (group->match(filter, ~Filter::Couple & ~Filter::Group))
                    entries.groups.push_back(group);
            }
            candidate_groups.clear();
        }
        have_groups = true;
    }

    if (filter.item_types & Filter::Node) {
        if (!filter.nodes.empty()) {
            for (const std::string & key : filter.nodes) {
                auto it = m_nodes.find(key);
                if (it != m_nodes.end()) {
                    Node & node = it->second;
                    if (node.match(filter, ~Filter::Node))
                        entries.nodes.push_back(&node);
                }
            }
            have_nodes = true;
        } else if (have_groups) {
            std::vector<Node*> candidate_nodes;
            for (Group *group : entries.groups) {
                std::set<Backend*> & backends = group->get_backends();
                for (Backend *backend : backends)
                    candidate_nodes.push_back(&backend->get_node());
            }
            remove_duplicates(candidate_nodes);
            for (Node *node : candidate_nodes) {
                if (node->match(filter, ~Filter::Group))
                    entries.nodes.push_back(node);
            }
            have_nodes = true;
        } else if (have_couples) {
            std::vector<Node*> candidate_nodes;
            std::vector<Group*> groups;
            for (Couple *couple : entries.couples) {
                couple->get_groups(groups);
                for (Group *group : groups) {
                    std::set<Backend*> & backends = group->get_backends();
                    for (Backend *backend : backends)
                        candidate_nodes.push_back(&backend->get_node());
                }
                groups.clear();
            }
            remove_duplicates(candidate_nodes);
            for (Node *node : candidate_nodes) {
                if (node->match(filter, ~Filter::Couple))
                    entries.nodes.push_back(node);
            }
            have_nodes = true;
        } else {
            for (auto it = m_nodes.begin(); it != m_nodes.end(); ++it) {
                Node & node = it->second;
                if (node.match(filter))
                    entries.nodes.push_back(&node);
            }
            have_nodes = true;
        }
    }

    if (filter.item_types & Filter::Backend) {
        if (!filter.backends.empty()) {
            Node *node = nullptr;
            for (const std::string & backend_key : filter.backends) {
                size_t pos = backend_key.rfind('/');
                if (pos == std::string::npos)
                    continue;
                std::string node_key = backend_key.substr(0, pos);
                if (node == nullptr || node->get_key() != node_key) {
                    if (!get_node(node_key, node))
                        continue;
                }
                Backend *backend;
                if (node->get_backend(std::stoi(backend_key.substr(pos + 1)), backend)) {
                    if (backend->match(filter, ~Filter::Backend))
                        entries.backends.push_back(backend);
                }
            }
            have_backends = true;
        } else if (have_groups) {
            for (Group *group : entries.groups) {
                std::set<Backend*> & backends = group->get_backends();
                for (Backend *backend : backends) {
                    if (backend->match(filter, ~Filter::Group))
                        entries.backends.push_back(backend);
                }
            }
            have_backends = true;
        } else if (have_couples) {
            std::vector<Group*> groups;
            for (Couple *couple : entries.couples) {
                couple->get_groups(groups);
                for (Group *group : groups) {
                    std::set<Backend*> & backends = group->get_backends();
                    for (Backend *backend : backends) {
                        if (backend->match(filter, ~Filter::Couple))
                            entries.backends.push_back(backend);
                    }
                }
                groups.clear();
            }
            have_backends = true;
        } else if (have_nodes) {
            for (Node *node : entries.nodes) {
                std::map<int, Backend> & backends = node->get_backends();
                for (auto it = backends.begin(); it != backends.end(); ++it) {
                    Backend & backend = it->second;
                    if (backend.match(filter, ~Filter::Node))
                        entries.backends.push_back(&backend);
                }
            }
            have_backends = true;
        }
    }

    if (filter.item_types & Filter::FS) {
        if (!filter.filesystems.empty()) {
            Node *node = nullptr;
            for (const std::string & fs_key : filter.filesystems) {
                size_t pos = fs_key.rfind('/');
                if (pos == std::string::npos)
                    continue;
                std::string node_key = fs_key.substr(0, pos);
                if (node == nullptr || node->get_key() != node_key) {
                    if (!get_node(node_key, node))
                        continue;
                }
                FS *fs;
                if (node->get_fs(std::stoull(fs_key.substr(pos + 1)), fs)) {
                    if (fs->match(filter, ~Filter::FS))
                        entries.filesystems.push_back(fs);
                }
            }
            have_fs = true;
        } else if (have_backends) {
            std::vector<FS*> candidate_fs;
            for (Backend *backend : entries.backends) {
                FS *fs = backend->get_fs();
                if (fs == nullptr)
                    continue;
                candidate_fs.push_back(fs);
            }
            remove_duplicates(candidate_fs);
            for (FS *fs : candidate_fs) {
                if (fs->match(filter, ~Filter::Backend))
                    entries.filesystems.push_back(fs);
            }
            have_fs = true;
        } else if (have_nodes) {
            for (Node *node : entries.nodes) {
                std::map<uint64_t, FS> & candidate_fs = node->get_filesystems();
                for (auto it = candidate_fs.begin(); it != candidate_fs.end(); ++it) {
                    FS & fs = it->second;
                    if (fs.match(filter, ~Filter::Node))
                        entries.filesystems.push_back(&fs);
                }
                candidate_fs.clear();
            }
            have_fs = true;
        } else if (have_groups) {
            std::vector<FS*> candidate_fs;
            for (Group *group : entries.groups) {
                std::set<Backend*> & backends = group->get_backends();
                for (Backend *backend : backends) {
                    FS *fs = backend->get_fs();
                    if (fs == nullptr)
                        continue;
                    candidate_fs.push_back(fs);
                }
            }
            remove_duplicates(candidate_fs);
            for (FS *fs : candidate_fs) {
                if (fs->match(filter, ~Filter::Group))
                    entries.filesystems.push_back(fs);
            }
            have_fs = true;
        } else if (have_couples) {
            std::vector<Group*> groups;
            std::vector<FS*> candidate_fs;
            for (Couple *couple : entries.couples) {
                couple->get_groups(groups);
                for (Group *group : groups) {
                    std::set<Backend*> & backends = group->get_backends();
                    for (Backend *backend : backends) {
                        FS *fs = backend->get_fs();
                        if (fs == nullptr)
                            continue;
                        candidate_fs.push_back(fs);
                    }
                }
                groups.clear();
            }
            remove_duplicates(candidate_fs);
            for (FS *fs : candidate_fs) {
                if (fs->match(filter, ~Filter::Couple))
                    entries.filesystems.push_back(fs);
            }
            have_fs = true;
        } else {
            for (auto nit = m_nodes.begin(); nit != m_nodes.end(); ++nit) {
                Node & node = nit->second;
                std::map<int, Backend> & backends = node.get_backends();
                for (auto it = backends.begin(); it != backends.end(); ++it) {
                    Backend & backend = it->second;
                    FS *fs = backend.get_fs();
                    if (fs == nullptr)
                        continue;
                    if (fs->match(filter))
                        entries.filesystems.push_back(fs);
                }
            }
            have_fs = true;
        }
    }

    if (!have_backends && (filter.item_types & Filter::Backend)) {
        if (have_fs) {
            for (FS *fs : entries.filesystems) {
                const std::set<Backend*> & backends = fs->get_backends();
                for (Backend *backend : backends) {
                    if (backend->match(filter, ~Filter::FS))
                        entries.backends.push_back(backend);
                }
            }
            have_backends = true;
        } else {
            std::map<std::string, Node> & nodes = m_nodes;
            for (auto nit = nodes.begin(); nit != nodes.end(); ++nit) {
                Node & node = nit->second;
                std::map<int, Backend> & backends = node.get_backends();
                for (auto bit = backends.begin(); bit != backends.end(); ++bit) {
                    Backend & backend = bit->second;
                    if (backend.match(filter))
                        entries.backends.push_back(&backend);
                }
                backends.clear();
            }
            have_backends = true;
        }
    }

    if (!have_groups && (filter.item_types & Filter::Group)) {
        if (have_backends) {
            for (Backend *backend : entries.backends) {
                Group *group = backend->get_group();
                if (group != nullptr && group->match(filter, ~Filter::Backend))
                    entries.groups.push_back(group);
            }
            remove_duplicates(entries.groups);
            have_groups = true;
        } else {
            for (auto it = m_groups.begin(); it != m_groups.end(); ++it) {
                Group & group = it->second;
                if (group.match(filter))
                    entries.groups.push_back(&group);
            }
            have_groups = true;
        }
    }

    if (have_backends || have_fs) {
        for (Backend *backend : entries.backends)
            entries.nodes.push_back(&backend->get_node());
        for (FS *fs : entries.filesystems)
            entries.nodes.push_back(&fs->get_node());
        remove_duplicates(entries.nodes);
    }
}

void Storage::print_json(uint32_t item_types, std::string & str)
{
    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);

    writer.StartObject();
    print_json(writer, item_types);
    writer.EndObject();

    str = buf.GetString();
}

void Storage::print_json(Filter & filter, std::string & str)
{
    filter.sort();

    Entries entries;
    select(filter, entries);

    rapidjson::StringBuffer buf;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buf);

    writer.StartObject();
    print_json(writer, entries, filter.item_types);
    writer.EndObject();

    str = buf.GetString();
}

void Storage::print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer,
        Entries & entries, uint32_t item_types)
{
    entries.sort();

    writer.Key("nodes");
    writer.StartArray();
    for (Node *node : entries.nodes)
        node->print_json(writer, entries.backends, entries.filesystems,
                !!(item_types & Filter::Backend), !!(item_types & Filter::FS));
    writer.EndArray();

    writer.Key("groups");
    writer.StartArray();
    for (Group *group : entries.groups)
        group->print_json(writer);
    writer.EndArray();

    writer.Key("couples");
    writer.StartArray();
    for (Couple *couple : entries.couples)
        couple->print_json(writer);
    writer.EndArray();
}

void Storage::print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer,
        uint32_t item_types)
{
    if (!!(item_types & (Filter::Node | Filter::Backend | Filter::FS))) {
        writer.Key("nodes");
        writer.StartArray();
        for (auto it = m_nodes.begin(); it != m_nodes.end(); ++it) {
            it->second.print_json(writer, std::vector<Backend*>(), std::vector<FS*>(),
                    !!(item_types & Filter::Backend), !!(item_types & Filter::FS));
        }
        writer.EndArray();
    }

    if (!!(item_types & Filter::Group)) {
        writer.Key("groups");
        writer.StartArray();
        for (auto it = m_groups.begin(); it != m_groups.end(); ++it)
            it->second.print_json(writer);
        writer.EndArray();
    }

    if (!!(item_types & Filter::Couple)) {
        writer.Key("couples");
        writer.StartArray();
        for (auto it = m_couples.begin(); it != m_couples.end(); ++it)
            it->second.print_json(writer);
        writer.EndArray();
    }
}

void Storage::update_group_structure()
{
    BH_LOG(m_app.get_logger(), DNET_LOG_INFO, "Updating group structure");

    for (auto it = m_nodes.begin(); it != m_nodes.end(); ++it) {
        Node & node = it->second;
        std::vector<Backend*> backends;
        node.pick_new_backends(backends);
        for (Backend *backend : backends)
            handle_backend(*backend);
    }
}

void Storage::merge(const Storage & other)
{
    merge_map(*this, m_nodes, other.m_nodes);
    merge_map(*this, m_groups, other.m_groups);
    merge_map(*this, m_couples, other.m_couples);
}
