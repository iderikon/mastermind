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

#ifndef __fe3f3177_eb96_4b90_b6a8_7891a62318dc
#define __fe3f3177_eb96_4b90_b6a8_7891a62318dc

template <typename LOCK>
class LockGuard
{
public:
    LockGuard(LOCK & lock)
        :
        m_lock(lock),
        m_released(false)
    {
        lock.acquire();
    }

    ~LockGuard()
    {
        release();
    }

    void release()
    {
        if (!m_released) {
            m_lock.release();
            m_released = true;
        }
    }

private:
    LOCK & m_lock;
    bool m_released;
};

template <typename LOCK>
class ReadGuard
{
public:
    ReadGuard(LOCK & lock)
        :
        m_lock(lock),
        m_released(false)
    {
        lock.acquire_read();
    }

    ~ReadGuard()
    {
        release();
    }

    void release()
    {
        if (!m_released) {
            m_lock.release_read();
            m_released = true;
        }
    }

private:
    LOCK & m_lock;
    bool m_released;
};

template <typename LOCK>
class WriteGuard
{
public:
    WriteGuard(LOCK & lock)
        :
        m_lock(lock),
        m_released(false)
    {
        lock.acquire_write();
    }

    ~WriteGuard()
    {
        release();
    }

    void release()
    {
        if (!m_released) {
            m_lock.release_write();
            m_released = true;
        }
    }

private:
    LOCK & m_lock;
    bool m_released;
};

#endif

