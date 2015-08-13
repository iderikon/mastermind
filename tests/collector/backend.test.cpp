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
#include <Storage.h>
#include <WorkerApplication.h>

#include "Samples.h"

#include <gtest/gtest.h>

TEST(Backend, Constructor)
{
    WorkerApplication app;
    Storage & storage = app.get_storage();
    Node node(storage, "::1", 1025, 10);

    Backend backend(node);
    backend.~Backend();
    memset(&backend, 0xAA, sizeof(backend));
    new (&backend) Backend(node);

    EXPECT_EQ(nullptr, backend.get_fs());

    EXPECT_EQ(0, backend.get_vfs_free_space());
    EXPECT_EQ(0, backend.get_vfs_total_space());
    EXPECT_EQ(0, backend.get_vfs_used_space());
    EXPECT_EQ(0, backend.get_free_space());
    EXPECT_EQ(0, backend.get_total_space());
    EXPECT_EQ(0, backend.get_used_space());
    EXPECT_EQ(0, backend.get_effective_space());
    EXPECT_EQ(0.0, backend.get_fragmentation());
    EXPECT_EQ(Backend::INIT, backend.get_status());
}

TEST(Backend, Recalculate)
{
    WorkerApplication app;
    init_logger(app);
    Storage & storage = app.get_storage();

    Node *node = nullptr;
    storage.add_node("::1", 1025, 10);
    ASSERT_EQ(true, storage.get_node("::1:1025:10", node));
    ASSERT_NE(nullptr, node);

    TestBackendStat1 bstat;
    node->handle_backend(bstat);
    bstat.ts_sec += 60;
    bstat.read_ios += 60000;
    bstat.write_ios += 30000;
    node->handle_backend(bstat);

    std::vector<Backend*> backends;
    node->get_backends(backends);
    ASSERT_EQ(1, backends.size());
    ASSERT_NE(nullptr, backends[0]);

    Backend *backend = backends[0];
    FS *fs = backend->get_fs();
    ASSERT_NE(nullptr, fs);

    std::vector<Backend*> fs_backends;
    EXPECT_EQ(bstat.fsid, fs->get_fsid());
    fs->get_backends(fs_backends);
    EXPECT_EQ(1, fs->get_backend_count());
    ASSERT_EQ(1, fs_backends.size());
    EXPECT_EQ(backend, fs_backends[0]);

    const FSStat & fs_stat = fs->get_stat();

    EXPECT_EQ(bstat.ts_sec, fs_stat.ts_sec);
    EXPECT_EQ(bstat.ts_usec, fs_stat.ts_usec);
    EXPECT_EQ(bstat.vfs_blocks * bstat.vfs_bsize, fs_stat.total_space);

    uint64_t vfs_free_space = bstat.vfs_bavail * bstat.vfs_bsize;
    uint64_t vfs_total_space = bstat.vfs_blocks * bstat.vfs_bsize;
    uint64_t vfs_used_space = vfs_total_space - vfs_free_space;

    EXPECT_EQ(vfs_free_space, backend->get_vfs_free_space());
    EXPECT_EQ(vfs_total_space, backend->get_vfs_total_space());
    EXPECT_EQ(vfs_used_space, backend->get_vfs_used_space());

    uint64_t total_space = std::min(bstat.blob_size_limit, vfs_total_space);
    uint64_t used_space = bstat.base_size;
    uint64_t free_space = std::min(total_space - used_space, vfs_free_space);

    EXPECT_EQ(total_space, backend->get_total_space());
    EXPECT_EQ(used_space, backend->get_used_space());
    EXPECT_EQ(free_space, backend->get_free_space());

    double fragmentation = double(bstat.records_removed) / double(bstat.records_total);

    EXPECT_EQ(fragmentation, backend->get_fragmentation());
    EXPECT_EQ(Backend::OK, backend->get_status());

    EXPECT_EQ(0, backend->full());

    EXPECT_EQ(1000, backend->get_read_rps());
    EXPECT_EQ(500, backend->get_write_rps());

    // TODO: effective

    ++bstat.ts_sec;
    bstat.vfs_bavail = 0;
    EXPECT_EQ(0, backend->full());
    node->handle_backend(bstat);
    EXPECT_EQ(1, backend->full());

    EXPECT_EQ(Backend::OK, backend->get_status());

    bstat.error = 1;
    node->handle_backend(bstat);

    EXPECT_EQ(Backend::STALLED, backend->get_status());

    // TODO: other statuses
}
