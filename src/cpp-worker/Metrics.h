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

#ifndef __79e93520_3d35_496a_83b9_fd0f7764b0ff
#define __79e93520_3d35_496a_83b9_fd0f7764b0ff

#include <cstdint>

#include <time.h>

inline void clock_start(uint64_t & value)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    value = ts.tv_sec * 1000000000 + ts.tv_nsec;
}

inline void clock_stop(uint64_t & value)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    value = ts.tv_sec * 1000000000 + ts.tv_nsec - value;
}

class Stopwatch
{
public:
    Stopwatch(uint64_t & value)
        :
        m_value(value),
        m_stopped(false)
    {
        clock_start(value);
    }

    ~Stopwatch()
    {
        if (!m_stopped)
            clock_stop(m_value);
    }

    void stop()
    {
        clock_stop(m_value);
        m_stopped = true;
    }

private:
    uint64_t & m_value;
    bool m_stopped;
};

#endif

