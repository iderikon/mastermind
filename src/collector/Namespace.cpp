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

#include "Backend.h"
#include "Couple.h"
#include "FS.h"
#include "Group.h"
#include "Namespace.h"
#include "Node.h"

Namespace::Namespace(const std::string & id)
    :
    m_id(id),
    m_default_settings(true)
{}

void Namespace::push_items(std::vector<std::reference_wrapper<Group>> & groups) const
{
    for (Couple & couple : m_couples)
        couple.push_items(groups);
}

void Namespace::push_items(std::vector<std::reference_wrapper<Node>> & nodes) const
{
    for (Couple & couple : m_couples)
        couple.push_items(nodes);
}

void Namespace::push_items(std::vector<std::reference_wrapper<Backend>> & backends) const
{
    for (Couple & couple : m_couples)
        couple.push_items(backends);
}

void Namespace::push_items(std::vector<std::reference_wrapper<FS>> & filesystems) const
{
    for (Couple & couple : m_couples)
        couple.push_items(filesystems);
}

void Namespace::push_items(std::vector<std::reference_wrapper<Couple>> & couples) const
{
    couples.insert(couples.end(), m_couples.begin(), m_couples.end());
}

void Namespace::print_json(rapidjson::Writer<rapidjson::StringBuffer> & writer) const
{
    writer.StartObject();

    writer.Key("id");
    writer.String(m_id.c_str());

    writer.Key("couples");
    writer.StartArray();
    for (const Couple & couple : m_couples)
        writer.String(couple.get_key().c_str());
    writer.EndArray();

    writer.EndObject();
}
