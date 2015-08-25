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

#ifndef __c065a812_f800_4562_8686_a77b1e60e201
#define __c065a812_f800_4562_8686_a77b1e60e201

#include <cstdint>
#include <rapidjson/writer.h>
#include <set>
#include <string>
#include <vector>

class Backend;
class Filter;
class Node;
class Storage;

struct FSStat
{
    uint64_t ts_sec;
    uint64_t ts_usec;
    uint64_t total_space;
};

class FS
{
public:
    enum Status {
        OK,
        BROKEN
    };

    static const char *status_str(Status status);

public:
    FS(Node & node, uint64_t fsid);
    FS(Node & node);

    void clone_from(const FS & other);

    Node & get_node()
    { return m_node; }

    const Node & get_node() const
    { return m_node; }

    uint64_t get_fsid() const
    { return m_fsid; }

    const std::string & get_key() const
    { return m_key; }

    const FSStat & get_stat() const
    { return m_stat; }

    void add_backend(Backend & backend)
    { m_backends.insert(&backend); }

    void remove_backend(Backend & backend)
    { m_backends.erase(&backend); }

    std::set<Backend*> & get_backends()
    { return m_backends; }

    // NB: get_items() may return duplicates
    void get_items(std::vector<Couple*> & couples) const;
    void get_items(std::vector<Namespace*> & namespaces) const;
    void get_items(std::vector<Backend*> & backends) const;
    void get_items(std::vector<Group*> & groups) const;
    void get_items(std::vector<Node*> & nodes) const;

    void update(const Backend & backend);
    void update_status();

    Status get_status() const
    { return m_status; }

    void set_status(Status status)
    { m_status = status; }

    void merge(const FS & other);

    void print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer,
            bool show_internals) const;

private:
    Node & m_node;
    uint64_t m_fsid;
    std::string m_key;

    std::set<Backend*> m_backends;

    FSStat m_stat;
    Status m_status;
};

#endif

