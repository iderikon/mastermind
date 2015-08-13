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

#include <Backend.h>
#include <FS.h>
#include <Group.h>
#include <Node.h>
#include <Storage.h>
#include <WorkerApplication.h>

#include "Samples.h"

#include <gtest/gtest.h>

TEST(Storage, Collections)
{
    WorkerApplication app;
    init_logger(app);
    Storage storage(app);

    Node *node = nullptr;
    std::vector<Node*> nodes;
    EXPECT_EQ(1, storage.add_node("::1", 1025, 10));
    storage.get_nodes(nodes);
    ASSERT_EQ(1, nodes.size());
    ASSERT_TRUE(storage.get_node("::1:1025:10", node));
    EXPECT_EQ(node, nodes[0]);
    EXPECT_EQ(std::string("::1:1025:10"), node->get_key());

    TestBackendStat1 bstat1;
    TestBackendStat2 bstat2;
    node->handle_backend(bstat1);
    node->handle_backend(bstat2);

    std::vector<Namespace*> namespaces;
    Namespace *ns = storage.get_namespace("winter");
    Namespace *found_ns = nullptr;
    ASSERT_NE(nullptr, ns);
    ASSERT_EQ(std::string("winter"), ns->get_name());
    ASSERT_TRUE(storage.get_namespace("winter", found_ns));
    ASSERT_NE(nullptr, found_ns);
    EXPECT_EQ(ns, found_ns);
    storage.get_namespaces(namespaces);
    ASSERT_EQ(1, namespaces.size());
    ASSERT_EQ(ns, namespaces[0]);

    Group *group1 = nullptr;
    Group *group2 = nullptr;
    std::vector<int> group_ids;
    std::vector<Group*> groups;
    storage.get_group_ids(group_ids);
    ASSERT_EQ(2, group_ids.size());
    storage.get_groups(groups);
    ASSERT_EQ(2, groups.size());
    ASSERT_TRUE(storage.get_group(bstat1.group, group1));
    ASSERT_TRUE(storage.get_group(bstat2.group, group2));
    ASSERT_NE(nullptr, group1);
    ASSERT_NE(nullptr, group2);
    ASSERT_EQ(group1, groups[0]);
    ASSERT_EQ(group2, groups[1]);
    EXPECT_EQ(bstat1.group, group1->get_id());
    EXPECT_EQ(bstat2.group, group2->get_id());

    group1->set_namespace(ns);
    group2->set_namespace(ns);

    std::vector<Couple*> couples;
    storage.create_couple(group_ids, group1);
    storage.get_couples(couples);
    ASSERT_EQ(1, couples.size());
    ASSERT_NE(nullptr, couples[0]);
    EXPECT_EQ(std::string("1:2"), couples[0]->get_key());
}
