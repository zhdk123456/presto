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
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * This type represents HOUR, MINUTE and SECONDS(3) fields from SQL standard.
 * Due to legacy mode it also contains TZ ID mappable to HOUR TZ and MINUTE TZ
 * but this should be dropped in the future.
 * For legacy behavior reference to SqlTimeWithTimeZone.
 */
public final class SqlTime
{
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private static final long MAX_LOCAL_MILLIS = TimeUnit.DAYS.toMillis(1);
    private static final long MIN_LOCAL_MILLIS = 0;

    private final long millisUtc;
    private final Optional<TimeZoneKey> sessionTimeZoneKey;

    /**
     * FIXME This is used only to determine current TZ offset for TIME
     * This is workaround to avoid changing semantic of millisUTC being part of SPI
     * This is best information about current TZ we can get at the point of query lifetime
     *
     * eg. Asia/Katmandu was +5:30 in 1970 and then was changed to +5:45,
     *     without usage of this timestamp, we would use TZ shift valid
     *     on 1970-01-01, which is invalid today.
     *
     *  @Note This is only required for legacy mode.
     */
    @Deprecated
    private final long timezoneOffsetReferenceTimestampUtc = System.currentTimeMillis();

    public SqlTime(long millisUtc)
    {
        this.millisUtc = millisUtc;
        this.sessionTimeZoneKey = Optional.empty();
        validateMillisRange();
    }

    @Deprecated
    public SqlTime(long millisUtc, TimeZoneKey sessionTimeZoneKey)
    {
        this.millisUtc = millisUtc;
        this.sessionTimeZoneKey = Optional.of(sessionTimeZoneKey);
        validateMillisRange();
    }

    public long getMillisUtc()
    {
        return millisUtc;
    }

    @Deprecated
    public Optional<TimeZoneKey> getSessionTimeZoneKey()
    {
        return sessionTimeZoneKey;
    }

    @Deprecated
    private boolean isDeprecatedTimestamp()
    {
        return sessionTimeZoneKey.isPresent();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(millisUtc, sessionTimeZoneKey);
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
        SqlTime other = (SqlTime) obj;
        return Objects.equals(this.millisUtc, other.millisUtc) &&
                Objects.equals(this.sessionTimeZoneKey, other.sessionTimeZoneKey);
    }

    @JsonValue
    @Override
    public String toString()
    {
        if (isDeprecatedTimestamp()) {
            Instant utcInstantInCurrentDay = getTimeInstantInCurrentDay();
            ZonedDateTime zonedDateTime = utcInstantInCurrentDay.atZone(ZoneId.of(sessionTimeZoneKey.get().getId()));
            return zonedDateTime.format(formatter);
        }
        else {
            // For testing purposes this should be exact equivalent of TIMESTAMP to VARCHAR cast
            return Instant.ofEpochMilli(millisUtc).atZone(ZoneOffset.UTC).format(formatter);
        }
    }

    private void validateMillisRange()
    {
        if (getLocalMillis() < MIN_LOCAL_MILLIS || getLocalMillis() >= MAX_LOCAL_MILLIS) {
            throw new RuntimeException("millisUtc + TZ offset must be between 0:00-23:59:59.999 UTC (" + millisUtc + " in time zone " + sessionTimeZoneKey + "is to high)");
        }
    }

    @Deprecated
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

    @Deprecated
    private long getLocalMillis()
    {
        if (!sessionTimeZoneKey.isPresent()) {
            return millisUtc;
        }
        TimeZoneKey timeZoneKey = sessionTimeZoneKey.get();
        ZoneId id = ZoneId.of(timeZoneKey.getId());
        ZoneOffset offset = id.getRules().getOffset(getTimeInstantInCurrentDay());
        return millisUtc + TimeUnit.SECONDS.toMillis(offset.getTotalSeconds());
    }
}
