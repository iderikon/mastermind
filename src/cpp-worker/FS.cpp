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

#include "FS.h"
#include "Guard.h"
#include "Node.h"
#include "Storage.h"
#include "WorkerApplication.h"

FS::FS(Storage & storage, const std::string & host, uint64_t fsid)
    :
    m_storage(storage),
    m_host(host),
    m_fsid(fsid),
    m_status(OK)
{
    m_stat.ts_sec = 0;
    m_stat.ts_usec = 0;
    m_stat.total_space = 0;
}

void FS::add_backend(BackendStat *backend)
{
    WriteGuard<RWSpinLock> guard(m_backends_lock);
    m_backends.insert(backend);
}

void FS::remove_backend(BackendStat *backend)
{
    WriteGuard<RWSpinLock> guard(m_backends_lock);
    m_backends.erase(backend);
}

void FS::update(const BackendStat & stat)
{
    m_stat.ts_sec = stat.ts_sec;
    m_stat.ts_usec = stat.ts_usec;
    m_stat.total_space = stat.vfs_total_space;
}

void FS::update_status()
{
    Status prev = m_status;

    uint64_t total_space = 0;
    {
        ReadGuard<RWSpinLock> guard(m_backends_lock);
        for (auto it = m_backends.begin(); it != m_backends.end(); ++it) {
            const BackendStat & bstat = **it;
            if (bstat.status != BackendStat::OK && bstat.status != BackendStat::BROKEN)
                continue;
            total_space += bstat.total_space;
        }
    }

    m_status = (total_space <= m_stat.total_space) ? OK : BROKEN;
    if (m_status != prev)
        BH_LOG(m_storage.get_app().get_logger(), DNET_LOG_INFO,
                "FS %s/%lu status change %d -> %d",
                m_host.c_str(), m_fsid, int(prev), int(m_status));
}

const char *FS::status_str(Status status)
{
    switch (status)
    {
    case OK:
        return "OK";
    case BROKEN:
        return "BROKEN";
    }
    return "UNKNOWN";
}
