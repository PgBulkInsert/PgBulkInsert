package de.bytefish.pgbulkinsert.pgsql.model.interval;

import java.util.concurrent.TimeUnit;

public class Interval {

    private final int months;
    private final int days;
    private final long timeOfDay;

    public Interval(int months, int days, long timeOfDay) {
        this.months = months;
        this.days = days;
        this.timeOfDay = timeOfDay;
    }

    public Interval(int months, int days, int hours, int minutes, int wholeSeconds, long microSeconds) {
        this.months = months;
        this.days = days;
        this.timeOfDay = calcTimeOfDay(hours, minutes, wholeSeconds, microSeconds);
    }

    public int getMonths() {
        return months;
    }

    public int getDays() {
        return days;
    }

    public long getTimeOfDay() {
        return timeOfDay;
    }

    private long calcTimeOfDay(int hours, int minutes, int wholeSeconds, long microSeconds) {
        return TimeUnit.HOURS.toMicros(hours)
                + TimeUnit.MINUTES.toMicros(minutes)
                + TimeUnit.SECONDS.toMicros(wholeSeconds)
                + microSeconds;
    }
}
