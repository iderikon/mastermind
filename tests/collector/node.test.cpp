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

TEST(Node, Constructor)
{
    WorkerApplication app;
    Storage & storage = app.get_storage();

    Node node(storage, "::1", 1025, 10);
    node.~Node();
    memset(&node, 0xAA, sizeof(node));
    new (&node) Node(storage, "::1", 1025, 10);

    EXPECT_EQ(&storage, &node.get_storage());
    EXPECT_EQ(std::string("::1"), node.get_host());
    EXPECT_EQ(1025, node.get_port());
    EXPECT_EQ(10, node.get_family());
    EXPECT_EQ(std::string("::1:1025:10"), node.get_key());
}

TEST(Node, Update)
{
    WorkerApplication app;
    init_logger(app);
    Storage & storage = app.get_storage();

    Node *node = nullptr;
    storage.add_node("::1", 1025, 10);
    ASSERT_TRUE(storage.get_node("::1:1025:10", node));
    ASSERT_NE(nullptr, node);

    TestNodeStat1 stat;
    node->update(stat);

    stat.ts_sec += 60;
    stat.tx_bytes += 60000000;
    stat.rx_bytes += 30000000;
    node->update(stat);

    const NodeStat & nstat = node->get_stat();
    EXPECT_EQ(stat.ts_sec, nstat.ts_sec);
    EXPECT_EQ(stat.ts_usec, nstat.ts_usec);
    EXPECT_EQ(stat.la1, nstat.la1);
    EXPECT_EQ(stat.tx_bytes, nstat.tx_bytes);
    EXPECT_EQ(stat.rx_bytes, nstat.rx_bytes);
    EXPECT_EQ(double(stat.la1)/100.0, nstat.load_average);
    EXPECT_EQ(1000000.0, nstat.tx_rate);
    EXPECT_EQ(500000.0, nstat.rx_rate);
}

TEST(Node, HandleBackend)
{
    WorkerApplication app;
    init_logger(app);
    Storage & storage = app.get_storage();

    Node *node = nullptr;
    storage.add_node("::1", 1025, 10);
    ASSERT_TRUE(storage.get_node("::1:1025:10", node));
    ASSERT_NE(nullptr, node);

    TestBackendStat1 bstat;

    Group *group = nullptr;
    ASSERT_FALSE(storage.get_group(bstat.group, group));

    node->handle_backend(bstat);

    Backend* backend = nullptr;
    std::vector<Backend*> backends;
    EXPECT_EQ(1, node->get_backend_count());
    ASSERT_TRUE(node->get_backend(bstat.backend_id, backend));
    ASSERT_NE(nullptr, backend);
    node->get_backends(backends);
    ASSERT_EQ(1, backends.size());
    EXPECT_EQ(backend, backends[0]);

    ASSERT_TRUE(storage.get_group(bstat.group, group));
    ASSERT_NE(nullptr, group);
    EXPECT_EQ(bstat.group, group->get_id());
}
