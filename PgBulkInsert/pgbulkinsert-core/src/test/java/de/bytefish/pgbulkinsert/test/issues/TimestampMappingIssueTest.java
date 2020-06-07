package de.bytefish.pgbulkinsert.test.issues;

import de.bytefish.pgbulkinsert.PgBulkInsert;
import de.bytefish.pgbulkinsert.mapping.AbstractMapping;
import de.bytefish.pgbulkinsert.test.mapping.PersonMapping;
import de.bytefish.pgbulkinsert.test.model.Person;
import de.bytefish.pgbulkinsert.test.utils.TransactionalTestBase;
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@SuppressWarnings("NullAway")
public class TimestampMappingIssueTest extends TransactionalTestBase {

    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTable();
    }

    private enum TicketStatusEnum {
        NONE,
        NEW
    }

    private static class Ticket {
        public String jobId;
        public String entityId;
        public long stepNo;
        public String opType;
        public TicketStatusEnum status;
        public String createdBy;
        public String resetBy;
        public Timestamp scheduledStartTime;
        public Timestamp scheduledEndTime;
        public Timestamp startTime;
        public Timestamp endTime;
        public boolean retryable;
        public int retryCount;
        public int cleanupStep;
        public String assignedTo;
        public String externalRefId;
        public String ticketDetail;
        public Timestamp createdTime;
        public String updatedBy;
        public boolean dryRun;
        public boolean cancelRequested;
        public String abortCode;
        public String errorCode;
    }

    private static class TicketMapping extends AbstractMapping<Ticket> {
        public TicketMapping(String schema) {
            super(schema, "unit_test");

            mapText("job_id", x -> x.jobId);
            mapText("entity_id", x -> x.entityId);
            mapNumeric("step_no", x -> x.stepNo);
            mapText("status", x -> x.status != null ? x.status.name() : null);
            mapText("op_type", x -> x.opType);
            mapText("created_by", x -> x.createdBy);
            mapText("reset_by", x -> x.resetBy);
            mapTimeStamp("scheduled_end_time", x -> x.scheduledEndTime != null ? x.scheduledEndTime.toLocalDateTime() : null);
            mapTimeStamp("scheduled_start_time", x -> x.scheduledStartTime != null ? x.scheduledStartTime.toLocalDateTime() : null);
            mapTimeStamp("start_time", x -> x.startTime != null ? x.startTime.toLocalDateTime() : null);
            mapTimeStamp("end_time", x -> x.endTime != null ? x.endTime.toLocalDateTime() : null);
            mapBoolean("retryable", x -> x != null ? x.retryable : null);
            mapNumeric("retry_count", x -> x.retryCount);
            mapNumeric("cleanup_step", x -> x.cleanupStep);
            mapText("assigned_to", x -> x.assignedTo);
            mapText("external_ref_id", x -> x.externalRefId);
            mapText("ticket_detail", x -> x.ticketDetail);
            mapTimeStamp("created_time",  x -> x.createdTime != null ? x.createdTime.toLocalDateTime() : null);
            mapText("updated_by", x -> x.updatedBy);
            mapBoolean("dry_run", x -> x != null ? x.dryRun : null);
            mapBoolean("cancel_requested", x -> x != null ? x.cancelRequested : null);
            mapText("abort_code", x -> x.abortCode);
            mapText("error_code", x -> x.errorCode);
        }


    }

    @Test
    public void bulkInsertPersonDataTest() throws SQLException {
        List<Ticket> ticketList = getTicketList(100000);
        // Create the BulkInserter:
        PgBulkInsert<Ticket> bulkInsert = new PgBulkInsert<>(new TicketMapping(schema));
        // Now save all entities of a given stream:
        bulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), ticketList.stream());
        // And assert all have been written to the database:
        Assert.assertEquals(100000, getRowCount());
    }

    private List<Ticket> getTicketList(int num) {

        List<Ticket> ticketList = new ArrayList<>();

        for (int pos = 0; pos < num; pos++) {
            Ticket t = new Ticket();

            t.errorCode = "ERROR";
            t.abortCode = "ABORT";
            t.cancelRequested = true;
            t.dryRun = true;
            t.updatedBy = "Philipp";
            t.createdTime = new Timestamp(new Date().getTime());
            t.ticketDetail = "Ticket Detail";
            t.externalRefId = "18431";
            t.assignedTo = "Philipp";
            t.cleanupStep = 4;
            t.retryCount = 8;
            t.retryable = true;
            t.endTime = new Timestamp(new Date().getTime());
            t.startTime = new Timestamp(new Date().getTime());
            t.scheduledStartTime = new Timestamp(new Date().getTime());
            t.scheduledEndTime = new Timestamp(new Date().getTime());
            t.resetBy = "Philipp";
            t.createdBy = "Philipp";
            t.status = TicketStatusEnum.NONE;
            t.opType = "OP_START";
            t.stepNo = 3;
            t.entityId = "7814111247";
            t.jobId = "7";

            ticketList.add(t);
        }

        return ticketList;
    }

    private boolean createTable() throws SQLException {

        String sqlStatement = String.format("CREATE TABLE %s.unit_test\n", schema) +
                "            (\n" +
                "                ticket_id text\n" +
                "                , abort_code text\n" +
                "                , assigned_to text\n" +
                "                , cancel_requested boolean\n" +
                "                , cleanup_step numeric\n" +
                "                , created_by text\n" +
                "                , created_time timestamp\n" +
                "                , dry_run boolean\n" +
                "                , end_time timestamp\n" +
                "                , entity_id text\n" +
                "                , error_code text\n" +
                "                , external_ref_id text\n" +
                "                , job_id text\n" +
                "                , op_type text\n" +
                "                , reset_by text\n" +
                "                , retry_count numeric\n" +
                "                , retryable boolean\n" +
                "                , scheduled_end_time timestamp\n" +
                "                , scheduled_start_time timestamp\n" +
                "                , start_time timestamp\n" +
                "                , status text\n" +
                "                , step_no numeric\n" +
                "                , ticket_detail text\n" +
                "                , updated_by text\n" +
                "            );";

        Statement statement = connection.createStatement();

        return statement.execute(sqlStatement);
    }

    private int getRowCount() throws SQLException {

        Statement s = connection.createStatement();

        ResultSet r = s.executeQuery(String.format("SELECT COUNT(*) AS rowcount FROM %s.unit_test", schema));
        r.next();
        int count = r.getInt("rowcount");
        r.close();

        return count;
    }

}

