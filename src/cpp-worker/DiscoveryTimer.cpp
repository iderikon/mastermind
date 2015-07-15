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

#include "DiscoveryTimer.h"
#include "ThreadPool.h"

#include <elliptics/logger.hpp>

namespace {

class DiscoveryStart : public ThreadPool::Job
{
public:
    DiscoveryStart(Discovery & discovery)
        : m_discovery(discovery)
    {}

    virtual void execute()
    {
        m_discovery.start();
    }

private:
    Discovery & m_discovery;
};

} // unnamed namespace

DiscoveryTimer::DiscoveryTimer(Discovery & discovery,
        ThreadPool & thread_pool, int interval)
    :
    m_discovery(discovery),
    m_thread_pool(thread_pool),
    m_interval(interval),
    m_started(false)
{
    m_signum = SIGRTMIN;
}

DiscoveryTimer::~DiscoveryTimer()
{
    stop();
}

int DiscoveryTimer::init()
{
    struct sigaction sa;
    sa.sa_flags = SA_SIGINFO;
    sa.sa_sigaction = DiscoveryTimer::handler;
    sigemptyset(&sa.sa_mask);
    if (sigaction(m_signum, &sa, NULL) < 0) {
        int err = errno;
        BH_LOG(*m_discovery.get_logger(), DNET_LOG_ERROR, "sigaction: %s", strerror(err));
        return -1;
    }

    return 0;
}

int DiscoveryTimer::start()
{
    if (m_started)
        return 0;

    struct sigevent sev;
    sev.sigev_notify = SIGEV_SIGNAL;
    sev.sigev_signo = m_signum;
    sev.sigev_value.sival_ptr = (void *) this;
    if (timer_create(CLOCK_REALTIME, &sev, &m_timer_id) < 0) {
        int err = errno;
        BH_LOG(*m_discovery.get_logger(), DNET_LOG_ERROR, "timer_create: %s", strerror(err));
        return -1;
    }

    int rc = arm(Initial);
    if (rc < 0) {
        int err = errno;
        BH_LOG(*m_discovery.get_logger(), DNET_LOG_ERROR, "Arm timer: %s", strerror(err));
        return -1;
    }

    m_started = true;
    return 0;
}

void DiscoveryTimer::stop()
{
    if (!m_started)
        return;

    timer_delete(m_timer_id);
    m_started = false;
}

void DiscoveryTimer::trigger()
{
    m_thread_pool.dispatch(new DiscoveryStart(m_discovery));
}

int DiscoveryTimer::arm(Launch launch)
{
    BH_LOG(*m_discovery.get_logger(), DNET_LOG_DEBUG, "Starting new timer");

    struct itimerspec its;

    if (launch == Subsequent) {
        its.it_value.tv_sec = m_interval;
        its.it_value.tv_nsec = 0;
    } else {
        its.it_value.tv_sec = 0;
        its.it_value.tv_nsec =1000;
    }

    its.it_interval.tv_sec = 0;
    its.it_interval.tv_nsec = 0;

    return timer_settime(m_timer_id, 0, &its, NULL);
}

void DiscoveryTimer::handler(int sig, siginfo_t *si, void *uc)
{
    DiscoveryTimer *self = (DiscoveryTimer *) si->si_value.sival_ptr;
    if (self == NULL)
        return;

    self->trigger();
}
