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

#ifndef __49081ec6_28e4_49d3_aeb6_70cb63ae2cc6
#define __49081ec6_28e4_49d3_aeb6_70cb63ae2cc6

#include <pthread.h>

class RWMutex
{
public:
    RWMutex()
    {
        pthread_rwlock_init(&m_lock, NULL);
    }

    ~RWMutex()
    {
        pthread_rwlock_destroy(&m_lock);
    }

    void acquire_read()
    {
        pthread_rwlock_rdlock(&m_lock);
    }

    void acquire_write()
    {
        pthread_rwlock_wrlock(&m_lock);
    }

    void release()
    {
        pthread_rwlock_unlock(&m_lock);
    }

    void release_read()
    {
        release();
    }

    void release_write()
    {
        release();
    }

private:
    pthread_rwlock_t m_lock;
};

#endif

