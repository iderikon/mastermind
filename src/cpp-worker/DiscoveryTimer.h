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

#ifndef __73daa2c1_32b3_4c59_a2fa_3ff5b0df23ef
#define __73daa2c1_32b3_4c59_a2fa_3ff5b0df23ef

#include <elliptics/logger.hpp>

#include <signal.h>
#include <time.h>
#include <unistd.h>

class WorkerApplication;

class DiscoveryTimer
{
public:
    enum Launch {
        Initial,
        Subsequent
    };

    DiscoveryTimer(WorkerApplication & app, int interval);
    ~DiscoveryTimer();

    int init();

    int start();

    void stop();

    void trigger();

    int arm(Launch launch);
    int disarm();

    static void handler(int sig, siginfo_t *si, void *uc);

private:
    WorkerApplication & m_app;

    timer_t m_timer_id;
    int m_signum;

    int m_interval;
    bool m_started;
};

#endif

