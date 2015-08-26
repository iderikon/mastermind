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

#ifndef __046c58ff_e6c4_49a6_a920_eee87b111685
#define __046c58ff_e6c4_49a6_a920_eee87b111685

#include <iostream>
#include <rapidjson/writer.h>
#include <set>
#include <string>
#include <vector>

class Backend;
class Couple;
class Filter;
class Namespace;
class Storage;

class Group
{
public:
    enum Status {
        INIT,
        COUPLED,
        BAD,
        BROKEN,
        RO,
        MIGRATING
    };

    static const char *status_str(Status status);

public:
    Group(Storage & storage, int id);
    Group(Storage & storage);

    void clone_from(const Group & other);

    int get_id() const
    { return m_id; }

    int get_key() const
    { return m_id; }

    Status get_status() const
    { return m_status; }

    void set_couple(Couple *couple)
    { m_couple = couple; }

    Couple *get_couple()
    { return m_couple; }

    Namespace *get_namespace()
    { return m_namespace; }

    void set_namespace(Namespace *ns)
    { m_namespace = ns; }

    bool has_backend(Backend & backend) const;
    void add_backend(Backend & backend);
    std::set<Backend*> & get_backends()
    { return m_backends; }

    // NB: get_items() may return duplicates
    void get_items(std::vector<Couple*> & couples) const;
    void get_items(std::vector<Namespace*> & namespaces) const;
    void get_items(std::vector<Node*> & nodes) const;
    void get_items(std::vector<Backend*> & backends) const;
    void get_items(std::vector<FS*> & filesystems) const;

    bool full() const;
    uint64_t get_total_space() const;

    void save_metadata(const char *metadata, size_t size);

    void process_metadata();

    bool check_metadata_equals(const Group & other) const;

    void set_status(Status status)
    { m_status = status; }

    void set_status_text(const std::string & status_text);

    void get_status_text(std::string & status_text) const;

    bool get_frozen() const
    { return m_frozen; }

    int get_version() const
    { return m_version; }

    bool get_service_migrating() const
    { return m_service.migrating; }

    void get_job_id(std::string & job_id) const;

    uint64_t get_metadata_process_time() const
    { return m_metadata_process_time; }

    void merge(const Group & other, bool & have_newer);

    void print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer,
            bool show_internals) const;

private:
    Storage & m_storage;

    int m_id;
    Couple *m_couple;

    std::set<Backend*> m_backends;

    bool m_clean;
    std::vector<char> m_metadata;
    std::string m_status_text;
    Status m_status;

    uint64_t m_metadata_process_start;
    uint64_t m_metadata_process_time;

    bool m_frozen;
    int m_version;
    Namespace *m_namespace;

    struct {
        bool migrating;
        std::string job_id;
    } m_service;
};

#endif

