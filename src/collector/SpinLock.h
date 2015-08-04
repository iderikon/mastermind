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

#ifndef __c0089610_1d55_467c_b5e3_cde7f51a402a
#define __c0089610_1d55_467c_b5e3_cde7f51a402a

class SpinLock
{
public:
    SpinLock()
        : m_flag(0)
    {}

    void acquire()
    {
        while (__sync_lock_test_and_set(&m_flag, 1))
#if defined(__x86_64__) || defined(__i386__)
            asm ("pause")
#endif
                ;
    }

    void release()
    {
        __sync_lock_release(&m_flag);
    }

private:
    int m_flag;
};

#endif

