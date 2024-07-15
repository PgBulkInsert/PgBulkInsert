package de.bytefish.pgbulkinsert.pgsql.model.interval;

public class Interval {

    private final int months;
    private final int days;
    private final long time;


    public Interval(int months, int days, long time) {
        this.months = months;
        this.days = days;
        this.time = time;
    }

    public int getMonths() {
        return months;
    }

    public int getDays() {
        return days;
    }

    public long getTime() {
        return time;
    }
}
