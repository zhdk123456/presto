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
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.Objects;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.spi.type.DateTimeEncoding.unpackZoneKey;

/**
 * This type represents instant of time in some day and timezone, where timezone
 * is known and day is not.
 * It corresponds to HOUR, MINUTE and SECONDS(3) fields from SQL standard + TIMEZONE
 * that can be mapped to HOUR TZ, MINUTE TZ.
 */
public final class SqlTimeWithTimeZone
{
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS VV");

    private static final long MAX_LOCAL_MILLIS = TimeUnit.DAYS.toMillis(1);
    private static final long MIN_LOCAL_MILLIS = 0;

    /**
     * Milliseconds relative to midnight UTC of the given time
     */
    private final long millisUtc;
    private final TimeZoneKey timeZoneKey;

    /**
     * FIXME This is used only to determine current TZ offset for TIME
     * This is workaround to avoid changing semantic of millisUTC being part of SPI
     * This is best information about current TZ we can get at the point of query lifetime
     *
     * eg. Asia/Katmandu was +5:30 in 1970 and then was changed to +5:45,
     *     without usage of this timestamp, we would use TZ shift valid
     *     on 1970-01-01, which is invalid today.
     */
    private final long timezoneOffsetReferenceTimestampUtc = System.currentTimeMillis();

    public SqlTimeWithTimeZone(long timeWithTimeZone)
    {
        millisUtc = unpackMillisUtc(timeWithTimeZone);
        timeZoneKey = unpackZoneKey(timeWithTimeZone);

        validateMillisRange();
    }

    public SqlTimeWithTimeZone(long millisUtc, TimeZoneKey timeZoneKey)
    {
        this.millisUtc = millisUtc;
        this.timeZoneKey = timeZoneKey;

        validateMillisRange();
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
        Instant utcInstantInCurrentDay = getTimeInstantInCurrentDay();
        ZonedDateTime zonedDateTime = utcInstantInCurrentDay.atZone(ZoneId.of(timeZoneKey.getId()));
        return zonedDateTime.format(formatter);
    }

    private Instant getTimeInstantInCurrentDay()
    {
        // FIXME This is hack that we need to use as the timezone interpretation depends on date (not only on time)
        // Additionally we cannot just grab current TZ offset from timezoneOffsetReferenceTimestampUtc and use it
        // as our formatting library will fail to display expected TZ symbol.
        long currentMillisOfDay = ChronoField.MILLI_OF_DAY.getFrom(
                Instant.ofEpochMilli(timezoneOffsetReferenceTimestampUtc).atZone(ZoneOffset.UTC));
        long timeMillisUtcInCurrentDay = timezoneOffsetReferenceTimestampUtc - currentMillisOfDay + millisUtc;
        return Instant.ofEpochMilli(timeMillisUtcInCurrentDay);
    }

    private void validateMillisRange()
    {
        if (getLocalMillis() < MIN_LOCAL_MILLIS || getLocalMillis() >= MAX_LOCAL_MILLIS) {
            throw new RuntimeException("millisUtc + TZ offset must be between 0:00-23:59:59.999 UTC (" + this.millisUtc + " in time zone " + this.timeZoneKey + "is to high)");
        }
    }

    private long getLocalMillis()
    {
        ZoneId id = ZoneId.of(timeZoneKey.getId());
        return ChronoField.MILLI_OF_DAY.getFrom(getTimeInstantInCurrentDay().atZone(id));
    }
}
