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

#ifndef __046c58ff_e6c4_49a6_a920_eee87b111685
#define __046c58ff_e6c4_49a6_a920_eee87b111685

#include "RWSpinLock.h"

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
    Group(Backend & backend, Storage & storage);
    Group(int id, Storage & storage);

    int get_id() const
    { return m_id; }

    Status get_status() const
    { return m_status; }

    void set_couple(Couple *couple)
    { m_couple = couple; }

    Couple *get_couple()
    { return m_couple; }

    Namespace *get_namespace()
    { return m_namespace; }

    bool has_backend(Backend & backend) const;
    void add_backend(Backend & backend);
    void get_backends(std::vector<Backend*> & backends) const;

    void save_metadata(const char *metadata, size_t size);

    void process_metadata();

    bool metadata_equals(const Group & other) const;

    void set_status(Status status)
    { m_status = status; }

    void set_status_text(const std::string & status_text);

    void get_status_text(std::string & status_text) const;

    bool get_frozen() const
    { return m_frozen; }

    bool match(const Filter & filter, uint32_t item_types = 0xFFFFFFFF) const;

    void print_info(std::ostream & ostr) const;
    void print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer) const;

private:
    int m_id;

    Storage & m_storage;
    Couple *m_couple;

    std::set<Backend*> m_backends;
    mutable RWSpinLock m_backends_lock;

    bool m_clean;
    std::vector<char> m_metadata;
    std::string m_status_text;
    Status m_status;
    mutable RWSpinLock m_metadata_lock;

    bool m_frozen;
    int m_version;
    Namespace *m_namespace;

    struct {
        bool migrating;
        std::string job_id;
    } m_service;
};

#endif

