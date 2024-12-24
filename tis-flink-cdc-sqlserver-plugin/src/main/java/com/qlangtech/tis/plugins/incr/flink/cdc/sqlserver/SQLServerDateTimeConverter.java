package com.qlangtech.tis.plugins.incr.flink.cdc.sqlserver;

import com.qlangtech.plugins.incr.flink.cdc.valconvert.DateTimeConverter;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-11-29 10:11
 **/
public class SQLServerDateTimeConverter extends DateTimeConverter {

    @Override
    protected String convertDate(Object input) {
        if (input instanceof LocalDate) {
            return dateFormatter.format((LocalDate) input);
        }
        if (input instanceof Integer) {
            LocalDate date = LocalDate.ofEpochDay((Integer) input);
            return dateFormatter.format(date);
        }
        return null;
    }

    @Override
    protected String convertTime(Object input) {
        if (input instanceof Duration) {
            Duration duration = (Duration) input;
            long seconds = duration.getSeconds();
            int nano = duration.getNano();
            LocalTime time = LocalTime.ofSecondOfDay(seconds).withNano(nano);
            return timeFormatter.format(time);
        }
        return null;
    }

    @Override
    protected String convertDateTime(Object input) {
        if (input instanceof LocalDateTime) {
            return datetimeFormatter.format((LocalDateTime) input);
        }
        return null;
    }

    @Override
    protected String convertTimestamp(Object input) {
        if (input instanceof ZonedDateTime) {
            // mysql的timestamp会转成UTC存储，这里的zonedDatetime都是UTC时间
            ZonedDateTime zonedDateTime = (ZonedDateTime) input;
            LocalDateTime localDateTime = zonedDateTime.withZoneSameInstant(timestampZoneId).toLocalDateTime();
            return timestampFormatter.format(localDateTime);
        }
        return null;
    }

}
