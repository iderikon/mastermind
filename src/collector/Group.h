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

#include <functional>
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

    struct BackendLess
    {
        bool operator () (const Backend & b1, const Backend & b2) const
        { return &b1 < &b2; }
    };
    typedef std::set<std::reference_wrapper<Backend>, BackendLess> Backends;

public:
    Group(int id);
    Group();

    int get_id() const
    { return m_id; }

    int get_key() const
    { return m_id; }

    Status get_status() const
    { return m_status; }

    bool full() const;

    uint64_t get_total_space() const;

    const std::string & get_namespace_name() const
    { return m_metadata.namespace_name; }

    const std::vector<int> & get_couple_group_ids() const
    { return m_metadata.couple; }

    bool get_frozen() const
    { return m_metadata.frozen; }

    int get_version() const
    { return m_metadata.version; }

    bool get_service_migrating() const
    { return m_metadata.service.migrating; }

    void add_backend(Backend & backend);

    void handle_metadata_download_failed(const std::string & why);
    void save_metadata(const char *metadata, size_t size);
    int parse_metadata();

    bool metadata_parsed() const
    { return m_metadata_parsed; }

    void set_namespace(Namespace & ns);

    void update_status(bool forbidden_dht);

    int check_metadata_equals(const Group & other);

    void set_couple(Couple & couple);

    bool match_couple(const Group & other) const;

    void merge(const Group & other, bool & have_newer);

    // NB: get_items() may return duplicates
    void get_items(std::vector<std::reference_wrapper<Couple>> & couples) const;
    void get_items(std::vector<std::reference_wrapper<Namespace>> & namespaces) const;
    void get_items(std::vector<std::reference_wrapper<Node>> & nodes) const;
    void get_items(std::vector<std::reference_wrapper<Backend>> & backends) const;
    void get_items(std::vector<std::reference_wrapper<FS>> & filesystems) const;

    void print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer,
            bool show_internals) const;

    uint64_t get_metadata_parse_duration() const
    { return m_metadata_parse_duration; }

private:
    int m_id;

    Backends m_backends;

    bool m_clean;
    std::vector<char> m_metadata_file;
    uint64_t m_metadata_download_time;

    // values extracted from file
    struct {
        int version;
        bool frozen;
        std::vector<int> couple;
        std::string namespace_name;
        struct {
            bool migrating;
            std::string job_id;
        } service;
    } m_metadata;

    bool m_metadata_parsed;
    uint64_t m_metadata_parse_duration;

    Couple *m_couple;
    Namespace *m_namespace;

    std::string m_status_text;
    Status m_status;
};

#endif

