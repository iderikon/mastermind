/*
   Copyright (c) YANDEX LLC, 2015. All rights reserved.
   This file is part of Mastermind.

   Mastermind is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 3.0 of the License, or (at your option) any later version.

   Mastermind is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public
   License along with Mastermind.
*/

#include "Metrics.h"

std::string timeval_user_friendly(time_t sec, int usec)
{
    struct tm tm_buf;
    struct tm *res = localtime_r(&sec, &tm_buf);
    if (res == NULL)
        return std::string();

    // res->tm_year is the number of years since 1900
    // res->tm_mon is the number of months since January (0-11)
    // res->tm_mday is in the range 1-31
    char buf[64];
    sprintf(buf, "%04d-%02d-%02d %02d:%02d:%02d.%06d",
            res->tm_year + 1900, res->tm_mon + 1, res->tm_mday,
            res->tm_hour, res->tm_min, res->tm_sec, usec);
    return std::string(buf);
}
