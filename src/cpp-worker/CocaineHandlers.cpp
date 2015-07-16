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

#include "CocaineHandlers.h"
#include "Couple.h"
#include "Group.h"
#include "Node.h"
#include "Storage.h"

void on_summary::on_chunk(const char *chunk, size_t size)
{
    Storage & storage = m_app.get_storage();

    std::vector<Node*> nodes;
    storage.get_nodes(nodes);

    size_t nr_backends = 0;
    for (size_t i = 0; i < nodes.size(); ++i)
        nr_backends += nodes[i]->get_backend_count();

    std::vector<Group*> groups;
    storage.get_groups(groups);

    std::vector<Couple*> couples;
    storage.get_couples(couples);

    std::map<Group::Status, int> group_status;
    for (size_t i = 0; i < groups.size(); ++i)
        ++group_status[groups[i]->get_status()];

    std::map<Couple::Status, int> couple_status;
    for (size_t i = 0; i < couples.size(); ++i)
        ++couple_status[couples[i]->get_status()];

    std::ostringstream ostr;

    ostr << "Storage contains:\n"
         << nodes.size() << " nodes\n"
         << nr_backends << " backends\n";

    ostr << groups.size() << " groups\n  ( ";
    for (auto it = group_status.begin(); it != group_status.end(); ++it)
        ostr << it->second << ' ' << Group::status_str(it->first) << ' ';

    ostr << ")\n" << couples.size() << " couples\n  ( ";
    for (auto it = couple_status.begin(); it != couple_status.end(); ++it)
        ostr << it->second << ' ' << Couple::status_str(it->first) << ' ';
    ostr << ")\n";

    std::vector<Namespace*> namespaces;
    storage.get_namespaces(namespaces);

    ostr << namespaces.size() << " namespaces\n";

    response()->write(ostr.str());
}
