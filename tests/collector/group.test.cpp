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

#include <Filter.h>
#include <FS.h>
#include <Group.h>
#include <Storage.h>
#include <WorkerApplication.h>

#include <algorithm>
#include <cstring>

#include <msgpack.hpp>
#include <gtest/gtest.h>

namespace {

void test_initialized(Group & group, int id)
{
    EXPECT_EQ(id, group.get_id());
    EXPECT_EQ(Group::INIT, group.get_status());
    EXPECT_EQ(nullptr, group.get_couple());
    EXPECT_EQ(nullptr, group.get_namespace());
    EXPECT_EQ(0, group.get_total_space());
    EXPECT_EQ(0, group.get_frozen());
    EXPECT_EQ(0, group.get_version());
    EXPECT_EQ(0, group.get_service_migrating());
    EXPECT_EQ(0, group.get_metadata_process_time());
}

void init_logger(WorkerApplication & app)
{
    app.set_logger(new ioremap::elliptics::file_logger(
                "/dev/null", ioremap::elliptics::log_level(4)));
}

} // unnamed namespace

TEST(Group, Constructor_BackendStorage)
{
    WorkerApplication app;
    Storage & storage = app.get_storage();
    Node node(storage, "::1", 1025, 10);

    BackendStat bstat;
    bstat.backend_id = 1;
    bstat.group = 2;
    Backend backend(node);
    backend.init(bstat);

    Group group(backend, storage);
    test_initialized(group, 2);

    EXPECT_EQ(true, group.has_backend(backend));
}

TEST(Group, Constructor_IntStorage)
{
    WorkerApplication app;
    Storage & storage = app.get_storage();

    Group group(3, storage);
    group.~Group();
    std::memset(&group, 0xAA, sizeof(group));
    new (&group) Group(3, storage);

    test_initialized(group, 3);
}

TEST(Group, Backends)
{
    WorkerApplication app;
    Storage & storage = app.get_storage();
    Node node(storage, "::1", 1025, 10);

    BackendStat bstat;
    bstat.group = 5;

    bstat.backend_id = 7;
    Backend b1(node);
    b1.init(bstat);

    bstat.backend_id = 11;
    Backend b2(node);
    b2.init(bstat);

    bstat.backend_id = 13;
    Backend b3(node);
    b3.init(bstat);

    Group group(13, storage);
    group.add_backend(b1);
    group.add_backend(b2);

    EXPECT_EQ(true, group.has_backend(b1));
    EXPECT_EQ(true, group.has_backend(b2));
    EXPECT_EQ(0, group.has_backend(b3));

    std::vector<Backend*> backends;
    group.get_backends(backends);

    ASSERT_EQ(2, backends.size());
    std::sort(backends.begin(), backends.end());
    EXPECT_EQ(std::vector<Backend*>({ std::min(&b1, &b2), std::max(&b1, &b2) }), backends);
}

namespace {

void check_couple(Group *group)
{
    Couple *couple = group->get_couple();
    ASSERT_NE(nullptr, couple);
    EXPECT_EQ("19:23:29", couple->get_key());

    std::vector<int> group_ids;
    couple->get_group_ids(group_ids);
    ASSERT_EQ(group_ids.size(), 3);
    EXPECT_EQ(std::vector<int>({19, 23, 29}), group_ids);

    std::vector<Group*> couple_groups;
    couple->get_groups(couple_groups);
    ASSERT_EQ(3, couple_groups.size());
    EXPECT_EQ(group, couple_groups[0]);
    EXPECT_EQ(23, couple_groups[1]->get_id());
    EXPECT_EQ(29, couple_groups[2]->get_id());
}

void test_metadata_common(const char *data, size_t size,
        Storage & storage, Group *& group)
{
    storage.add_node("::1", 1025, 10);

    Node *node = nullptr;
    ASSERT_TRUE(storage.get_node("::1:1025:10", node));
    ASSERT_NE(nullptr, node);

    BackendStat bstat;
    bstat.backend_id = 17;
    bstat.group = 19;
    bstat.state = 1;
    node->handle_backend(bstat);

    std::vector<Group*> groups;
    storage.get_groups(groups);
    ASSERT_EQ(1, groups.size());

    group = groups[0];
    ASSERT_NE(nullptr, group);
    ASSERT_EQ(19, group->get_id());

    group->save_metadata(data, size);
    group->process_metadata();

    EXPECT_EQ(Group::COUPLED, group->get_status()); // XXX

    check_couple(group);
}

} // unnamed namespace

TEST(Group, Metadata_V1)
{
    msgpack::sbuffer buffer;
    msgpack::packer<msgpack::sbuffer> packer(buffer);
    packer.pack(std::vector<int>({19, 23, 29}));

    WorkerApplication app;
    init_logger(app);

    Group *group = nullptr;
    test_metadata_common(buffer.data(), buffer.size(), app.get_storage(), group);
    ASSERT_NE(nullptr, group);

    EXPECT_EQ(1, group->get_version());
    EXPECT_EQ(0, group->get_frozen());

    Namespace *ns = group->get_namespace();
    ASSERT_NE(nullptr, ns);
    EXPECT_EQ(std::string("default"), ns->get_name());
    EXPECT_EQ(0, group->get_service_migrating());
}

TEST(Group, Metadata_V2)
{
    msgpack::sbuffer buffer;
    msgpack::packer<msgpack::sbuffer> packer(buffer);
    packer.pack_map(5);
    packer.pack(std::string("version"));
    packer.pack(2);
    packer.pack(std::string("couple"));
    packer.pack(std::vector<int>({19, 23, 29}));
    packer.pack(std::string("namespace"));
    packer.pack(std::string("special"));
    packer.pack(std::string("frozen"));
    packer.pack(true);
    packer.pack(std::string("service"));
    packer.pack_map(2);
    packer.pack(std::string("status"));
    packer.pack(std::string("MIGRATING"));
    packer.pack(std::string("job_id"));
    packer.pack(std::string("abcd"));

    WorkerApplication app;
    init_logger(app);

    Group *group = nullptr;
    test_metadata_common(buffer.data(), buffer.size(), app.get_storage(), group);
    ASSERT_NE(nullptr, group);

    EXPECT_EQ(2, group->get_version());
    EXPECT_EQ(true, group->get_frozen());

    Namespace *ns = group->get_namespace();
    ASSERT_NE(nullptr, ns);
    EXPECT_EQ("special", ns->get_name());
    EXPECT_EQ(true, group->get_service_migrating());

    std::string job_id;
    group->get_job_id(job_id);
    EXPECT_EQ(std::string("abcd"), job_id);

    std::vector<Group*> groups;
    group->get_couple()->get_groups(groups);
    ASSERT_EQ(group, groups[0]);
    EXPECT_EQ(true, group->check_metadata_equals(*group));
    EXPECT_EQ(true, group->check_metadata_equals(*groups[1]));

    groups[1]->save_metadata(buffer.data(), buffer.size());
    groups[1]->process_metadata();
    EXPECT_EQ(true, group->check_metadata_equals(*groups[1]));

    msgpack::sbuffer buffer_v1;
    msgpack::packer<msgpack::sbuffer> packer_v1(buffer_v1);
    packer_v1.pack(std::vector<int>({19, 23, 29}));

    groups[1]->save_metadata(buffer_v1.data(), buffer_v1.size());
    groups[1]->process_metadata();
    EXPECT_EQ(0, group->check_metadata_equals(*groups[1]));
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
