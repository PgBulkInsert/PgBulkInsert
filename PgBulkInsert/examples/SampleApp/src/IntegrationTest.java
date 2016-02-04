// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

import de.bytefish.jtinycsvparser.CsvParser;
import de.bytefish.jtinycsvparser.CsvParserOptions;
import de.bytefish.jtinycsvparser.builder.IObjectCreator;
import de.bytefish.jtinycsvparser.mapping.CsvMapping;
import de.bytefish.jtinycsvparser.mapping.CsvMappingResult;
import de.bytefish.jtinycsvparser.typeconverter.LocalDateConverter;
import de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.PgBulkInsert;
import de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.stream.Stream;

public class IntegrationTest extends TransactionalTestBase {

    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTable();
    }

    private boolean createTable() throws SQLException {

        String sqlStatement = "CREATE TABLE sample.unit_test\n" +
                "            (\n" +
                "                wban text,\n" +
                "                sky_condition text,\n" +
                "                date timestamp\n" +
                "            );";

        Statement statement = connection.createStatement();

        return statement.execute(sqlStatement);
    }

    private int getRowCount() throws SQLException {

        Statement s = connection.createStatement();

        ResultSet r = s.executeQuery("SELECT COUNT(*) AS rowcount FROM sample.unit_test");
        r.next();
        int count = r.getInt("rowcount");
        r.close();

        return count;
    }

    private class LocalWeatherData
    {
        private String WBAN;

        private LocalDate Date;

        private String SkyCondition;

        public String getWBAN() {
            return WBAN;
        }

        public void setWBAN(String WBAN) {
            this.WBAN = WBAN;
        }

        public LocalDate getDate() {
            return Date;
        }

        public void setDate(LocalDate date) {
            Date = date;
        }

        public String getSkyCondition() {
            return SkyCondition;
        }

        public void setSkyCondition(String skyCondition) {
            SkyCondition = skyCondition;
        }
    }

    public class LocalWeatherDataBulkInserter extends PgBulkInsert<LocalWeatherData>
    {
        public LocalWeatherDataBulkInserter() {
            super("sample", "unit_test");

            MapString("wban", LocalWeatherData::getWBAN);
            MapString("sky_condition", LocalWeatherData::getSkyCondition);
            MapDate("date", LocalWeatherData::getDate);
        }
    }

    public class LocalWeatherDataMapper extends CsvMapping<LocalWeatherData>
    {
        public LocalWeatherDataMapper(IObjectCreator creator)
        {
            super(creator);

            MapProperty(0, String.class, LocalWeatherData::setWBAN);
            MapProperty(1, LocalDate.class, LocalWeatherData::setDate, new LocalDateConverter(DateTimeFormatter.ofPattern("yyyyMMdd")));
            MapProperty(4, String.class, LocalWeatherData::setSkyCondition);
        }
    }

    @Test
    public void bulkInsertWeatherDataTest() throws SQLException {
        // Do not process the CSV file in parallel (Java 1.8 bug!):
        CsvParserOptions options = new CsvParserOptions(true, ",", false);
        // The Mapping to employ:
        LocalWeatherDataMapper mapping = new LocalWeatherDataMapper(() -> new LocalWeatherData());
        // Construct the parser:
        CsvParser<LocalWeatherData> parser = new CsvParser<>(options, mapping);
        // Create the BulkInserter used for Bulk Inserts into the Database:
        LocalWeatherDataBulkInserter bulkInserter = new LocalWeatherDataBulkInserter();
        // Read the file. Make sure to wrap it in a try with resources block, so the file handle gets disposed properly:
        try(Stream<CsvMappingResult<LocalWeatherData>> stream = parser.readFromFile(FileSystems.getDefault().getPath("C:\\Users\\philipp\\Downloads\\csv", "201503hourly.txt"), StandardCharsets.UTF_8)) {
            // Filter the Stream of Mapping results, so only valid entries are processed:
            Stream<LocalWeatherData> localWeatherDataStream =  stream.filter(e -> e.isValid()).map(e -> e.getResult());
            // Now bulk insert valid entries into the PostgreSQL database:
            bulkInserter.saveAll(PostgreSqlUtils.getPGConnection(connection), localWeatherDataStream);
        }
        // Check if we have the correct amount of rows in the DB:
        Assert.assertEquals(4496262, getRowCount());
    }
}