/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.spi.type;

import com.fasterxml.jackson.annotation.JsonValue;

import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.TimeZone;

import static com.facebook.presto.spi.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.spi.type.DateTimeEncoding.unpackZoneKey;

/**
 * This type represent instant of time in some day and timezone, where timezone
 * is known and day is not.
 */
public final class SqlTimeWithTimeZone
{
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS VV");

    /**
     * As we dont have negative timestamps and max value is midnight in timezone that is ~(UTC -12:00),
     * just not to care about precision we do use ~(UTC -24:00).
     *
     * It's all about protecting type from beeing used with full millisUTC.
     */
    private static final long MAX_MILLIS_TIME = 2 * 24 * 60 * 60 * 1000;

    /**
     * Milliseconds relative to midnight UTC of the given time (millisUtc < MAX_MILLIS_TIME).
     */
    private final long millisUtc;
    private final TimeZoneKey timeZoneKey;

    /**
     * FIXME This is used only to determine last of TZ shifts for TIME
     * This is workaround to avoid chaning semantic of millisUTC being part of SPI
     * This is best information about current TZ we can get at the point of query
     *
     * eg. Asia/Katmandu was +5:30 at some point in a history and then was changed
     *     to +5:45, without usage of this timestamp, we would use TZ shift valid
     *     on 1970-01-01, which is invalid today.
     */
    private final long timezoneOffsetReferenceTimestampUtc = System.currentTimeMillis();

    public SqlTimeWithTimeZone(long timeWithTimeZone)
    {
        millisUtc = unpackMillisUtc(timeWithTimeZone);
        if (millisUtc > MAX_MILLIS_TIME) {
            throw new RuntimeException("millisUtc must be time in millisecond that past since midnight UTC.");
        }
        timeZoneKey = unpackZoneKey(timeWithTimeZone);
    }

    public SqlTimeWithTimeZone(long millisUtc, TimeZoneKey timeZoneKey)
    {
        this.millisUtc = millisUtc;
        if (millisUtc > MAX_MILLIS_TIME) {
            throw new RuntimeException("millisUtc must be time in millisecond that past since midnight UTC.");
        }
        this.timeZoneKey = timeZoneKey;
    }

    public SqlTimeWithTimeZone(long millisUtc, TimeZone timeZone)
    {
        this.millisUtc = millisUtc;
        this.timeZoneKey = TimeZoneKey.getTimeZoneKey(timeZone.getID());
    }

    public long getMillisUtc()
    {
        return millisUtc;
    }

    public TimeZoneKey getTimeZoneKey()
    {
        return timeZoneKey;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(millisUtc, timeZoneKey);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SqlTimeWithTimeZone other = (SqlTimeWithTimeZone) obj;
        return Objects.equals(this.millisUtc, other.millisUtc) &&
                Objects.equals(this.timeZoneKey, other.timeZoneKey);
    }

    @JsonValue
    @Override
    public String toString()
    {
        // FIXME This is hack that we need to use as the timezone interpretation depends on date (not only on time)
        // Additionally we cannot just grab current TZ offset from timezoneOffsetReferenceTimestampUtc and use it
        // as our formatting library will fail to display expected TZ symbol.
        long currentMillisOfDay =
                LocalTime.from(
                        Instant.ofEpochMilli(timezoneOffsetReferenceTimestampUtc)
                               .atZone(ZoneId.of(TimeZoneKey.UTC_KEY.getId())))
                        .toNanoOfDay() / 1_000_000L; // nanos to millis
        long timeMillisUtcInCurrentDay = timezoneOffsetReferenceTimestampUtc - currentMillisOfDay + millisUtc;

        Instant utcInstantInCurrentDay = Instant.ofEpochMilli(timeMillisUtcInCurrentDay);
        ZonedDateTime zonedDateTime = utcInstantInCurrentDay.atZone(ZoneId.of(timeZoneKey.getId()));
        return zonedDateTime.format(formatter);
    }
}
