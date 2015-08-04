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

#ifndef __104fec25_c9ba_4720_bf37_565d262e41e0
#define __104fec25_c9ba_4720_bf37_565d262e41e0

#include "Semaphore.h"
#include "SpinLock.h"

#include <atomic>
#include <set>
#include <thread>
#include <vector>

class ThreadPool
{
    enum { NrThreads = 8 };

public:
    class Job
    {
    public:
        Job()
            :
            m_next(NULL),
            m_nr_deps(0)
        {}

        virtual ~Job() {}
        virtual void execute() = 0;

        void add_next(Job *job);

        bool has_next() const
        { return (m_next != NULL); }

        Job *get_next() const
        { return m_next; }

        bool check_ready();

        virtual void dispose()
        { delete this; }

    protected:
        void clear()
        {
            m_next = NULL;
            m_nr_deps = 0;
        }

        Job *m_next;
        std::atomic<int> m_nr_deps;
    };

private:
    class Aggregator;

public:
    ThreadPool();
    ~ThreadPool();

    int get_nr_threads() const
    { return NrThreads; }

    void start();
    void dispatch(Job *job);
    void dispatch_after(Job *job);
    void dispatch_aggregate(Job *job);
    void dispatch_pending(Job *job);
    void execute_pending(Job *job);
    void stop();
    void flush();

private:
    void thread_func(int thr_id);

    bool have_running() const
    {
        for (int i = 0; i < NrThreads; ++i) {
            if (m_running[i] != NULL)
                return true;
        }
        return false;
    }

private:
    std::thread m_threads[NrThreads];
    Semaphore m_sem;

    std::set<Job*> m_pending;
    std::vector<Job*> m_scheduled;
    std::vector<Job*> m_running;
    Aggregator *m_active_aggregator;
    SpinLock m_jobs_lock;
};

#endif

