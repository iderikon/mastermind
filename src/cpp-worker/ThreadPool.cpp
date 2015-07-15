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

#include "Guard.h"
#include "ThreadPool.h"

#include <assert.h>

namespace {

class FlushJob : public ThreadPool::Job
{
public:
    FlushJob(Semaphore & sem)
        : m_sem(sem)
    {}

    void execute()
    {
        m_sem.post();
    }

private:
    Semaphore & m_sem;
};

} // unnamed namespace

void ThreadPool::Job::add_next(Job *job)
{
    assert(m_next != job);

    if (m_next == NULL) {
        m_next = job;
        job->m_nr_deps++;
    } else {
        m_next->add_next(job);
    }
}

bool ThreadPool::Job::check_ready()
{
    return (--m_nr_deps == 0);
}

ThreadPool::ThreadPool()
    :
    m_running(NrThreads, NULL)
{
}

ThreadPool::~ThreadPool()
{
    stop();
}

void ThreadPool::start()
{
    for (int i = 0; i < NrThreads; ++i)
        m_threads[i] = std::thread( [this, i] { thread_func(i); } );
}

void ThreadPool::dispatch(Job *job)
{
    m_jobs_lock.acquire();
    m_scheduled.push_back(job);
    m_jobs_lock.release();
    m_sem.post();
}

void ThreadPool::dispatch_after(Job *job)
{
    assert(job != NULL);

    LockGuard<SpinLock> guard(m_jobs_lock);

    bool have_running = false;
    for (int i = 0; i < NrThreads; ++i) {
        if (m_running[i] != NULL) {
            have_running = true;
            break;
        }
    }

    bool run_now = false;
    if (!have_running && m_pending.empty() && m_scheduled.empty()) {
        run_now = true;
        m_scheduled.push_back(job);
    } else {
        bool added_top = false;

        for (size_t i = 0; i < m_scheduled.size(); ++i) {
            Job *j = m_scheduled[i];
            if (j) {
                if (j->has_next()) {
                    if (added_top)
                        continue;
                    added_top = true;
                }
                j->add_next(job);
            }
        }

        for (size_t i = 0; i < m_running.size(); ++i) {
            Job *j = m_running[i];
            if (j) {
                if (j->has_next()) {
                    if (added_top)
                        continue;
                    added_top = true;
                }
                j->add_next(job);
            }
        }

        auto it = m_pending.begin();
        for (; it != m_pending.end(); ++it) {
            Job *j = *it;
            if (j->has_next()) {
                if (added_top)
                    continue;
                added_top = true;
            }
            j->add_next(job);
        }
    }

    guard.release();

    if (run_now)
        m_sem.post();
}

void ThreadPool::dispatch_pending(Job *job)
{
    m_jobs_lock.acquire();
    m_pending.insert(job);
    m_jobs_lock.release();
}

void ThreadPool::execute_pending(Job *job)
{
    m_jobs_lock.acquire();

    m_pending.erase(job);
    m_scheduled.push_back(job);

    m_jobs_lock.release();
    m_sem.post();
}

void ThreadPool::stop()
{
    for (int i = 0; i < NrThreads; ++i)
        dispatch(NULL);

    for (int i = 0; i < NrThreads; ++i)
        if (m_threads[i].joinable())
            m_threads[i].join();
}

void ThreadPool::flush()
{
    Semaphore sem;
    dispatch_after(new FlushJob(sem));
    sem.wait();
}

void ThreadPool::thread_func(int thr_id)
{
    while (1) {
        m_sem.wait();

        Job *job = NULL;

        {
            LockGuard<SpinLock> guard(m_jobs_lock);
            if (!m_scheduled.empty()) {
                job = m_scheduled.back();
                m_scheduled.pop_back();
            }

            assert(m_running[thr_id] == NULL);
            m_running[thr_id] = job;
        }

        if (job) {
            job->execute();

            Job *next = NULL;
            {
                LockGuard<SpinLock> guard(m_jobs_lock);

                m_running[thr_id] = NULL;

                next = job->get_next();
                if (next != NULL) {
                    if (!next->check_ready())
                        next = NULL;
                }
            }

            if (next != NULL)
                dispatch(next);

            delete job;
        } else {
            break;
        }
    }
}
