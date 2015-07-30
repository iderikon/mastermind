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

#ifndef __85f47038_5a9f_43b5_97fd_8c7f69012993
#define __85f47038_5a9f_43b5_97fd_8c7f69012993

#include <errno.h>
#include <semaphore.h>

class Semaphore
{
public:
    Semaphore()
        : m_errno(0)
    {
        sem_init(&m_sem, 0, 0);
    }

    ~Semaphore()
    {
        sem_destroy(&m_sem);
    }

    int post()
    {
        return sem_post(&m_sem);
    }

    int wait()
    {
        int rc = sem_wait(&m_sem);
        if (rc)
            m_errno = errno;
        return rc;
    }

    int get_value()
    {
        int ret = 0;
        sem_getvalue(&m_sem, &ret);
        return ret;
    }

    bool interrupted() const
    { return m_errno == EINTR; }

private:
    Semaphore(const Semaphore & other);
    Semaphore & operator = (const Semaphore & other);

private:
    sem_t m_sem;
    int m_errno;
};

#endif

