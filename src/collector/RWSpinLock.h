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

#ifndef __9fdca1e5_682f_4626_a279_1d65241e80b6
#define __9fdca1e5_682f_4626_a279_1d65241e80b6

class RWSpinLock
{
public:
    RWSpinLock()
        :
        m_writer(0),
        m_readers(0)
    {}

    void acquire_write()
    {
        while (__sync_lock_test_and_set(&m_writer, 1))
#if defined(__x86_64__) || defined(__i386__)
            asm ("pause")
#endif
                ;

        while (m_readers != 0)
#if defined(__x86_64__) || defined(__i386__)
            asm ("pause")
#endif
                ;
    }

    void release_write()
    {
        __sync_lock_release(&m_writer);
    }

    void acquire_read()
    {
        while (1) {
            while (m_writer != 0)
#if defined(__x86_64__) || defined(__i386__)
                asm ("pause")
#endif
                    ;

            __sync_fetch_and_add(&m_readers, 1);

            if (!m_writer)
                break;

            __sync_fetch_and_sub(&m_readers, 1);
        }
    }

    void release_read()
    {
        __sync_fetch_and_sub(&m_readers, 1);
    }

private:
    int m_writer;
    int m_readers;
};

#endif

