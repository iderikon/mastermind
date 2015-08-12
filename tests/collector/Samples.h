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

#ifndef __f41ca941_4fa9_43fb_afcf_7466a303c03a
#define __f41ca941_4fa9_43fb_afcf_7466a303c03a

#include <Backend.h>

struct TestNodeStat1 : public NodeStat
{
    TestNodeStat1()
    {
        ts_sec = 597888000;
        ts_usec = 0;

        la1 = 50;
        tx_bytes = 0x40000000;
        rx_bytes = 0x80000000;
    }
};

struct TestBackendStat1 : public BackendStat
{
    TestBackendStat1()
    {
        ts_sec = 597888000;
        ts_usec = 0;

        backend_id = 1;
        state = 1;

        vfs_blocks = 500000000;
        vfs_bavail = 300000000;
        vfs_bsize = 4096;

        records_total = 100;
        records_removed = 10;
        records_removed_size = 1000;
        base_size = 3000000;

        fsid = 0x504f4c494e41;
        defrag_state = 0;
        want_defrag = 0;

        read_ios = 20000000;
        write_ios = 10000000;
        error = 0;

        blob_size_limit = 100000000000;
        max_blob_base_size = 50000000000;
        blob_size = 40000000000;

        group = 1;
    }
};

struct TestBackendStat2 : public TestBackendStat1
{
    TestBackendStat2()
    {
        group = 2;
    }
};

namespace {

void init_logger(WorkerApplication & app)
{
    app.set_logger(new ioremap::elliptics::file_logger(
                "/dev/null", ioremap::elliptics::log_level(1)));
}

} // unnamed namespace

#endif

