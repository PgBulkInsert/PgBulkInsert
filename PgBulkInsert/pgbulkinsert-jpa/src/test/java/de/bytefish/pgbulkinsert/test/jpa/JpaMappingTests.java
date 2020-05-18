// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.test.jpa;

import de.bytefish.pgbulkinsert.PgBulkInsert;
import de.bytefish.pgbulkinsert.jpa.JpaMapping;
import de.bytefish.pgbulkinsert.pgsql.constants.DataType;
import de.bytefish.pgbulkinsert.test.utils.TransactionalTestBase;
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Assert;
import org.junit.Test;
import org.postgresql.PGConnection;

import javax.persistence.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class JpaMappingTests extends TransactionalTestBase {

    public enum SampleEntityTypeEnum {
        STRING,
        INTEGER
    }

    @Entity
    @Table(name = "unit_test", schema = "public")
    public class SampleEntity {

        @Nullable
        @Id
        @Column(name = "id")
        private Long id;

        @Nullable
        @Column(name = "int_field")
        private Integer intField;

        @Nullable
        @Column(name = "text_field")
        private String textField;

        @Nullable
        @Column(name = "read_only_text_field")
        private String readOnlyTextField;

        @Nullable
        @Enumerated(value = EnumType.STRING)
        @Column(name = "enum_string_field")
        private SampleEntityTypeEnum typeStringField;

        @Nullable
        @Enumerated(value = EnumType.ORDINAL)
        @Column(name = "enum_smallint_field")
        private SampleEntityTypeEnum typeOrdinalField;

        @Nullable
        @Enumerated(value = EnumType.ORDINAL)
        @Column(name = "enum_smallint_field_as_integer")
        private SampleEntityTypeEnum typeOrdinalFieldAsInteger;

        @Nullable
        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        @Nullable
        public Integer getIntField() {
            return intField;
        }

        public void setIntField(Integer intField) {
            this.intField = intField;
        }

        @Nullable
        public String getTextField() {
            return textField;
        }

        public void setTextField(String textField) {
            this.textField = textField;
        }

        @Nullable
        public String getReadOnlyTextField() {
            return readOnlyTextField;
        }

        @Nullable
        public SampleEntityTypeEnum getTypeStringField() {
            return typeStringField;
        }

        public void setTypeStringField(SampleEntityTypeEnum typeStringField) {
            this.typeStringField = typeStringField;
        }

        @Nullable
        public SampleEntityTypeEnum getTypeOrdinalField() {
            return typeOrdinalField;
        }

        public void setTypeOrdinalField(SampleEntityTypeEnum typeOrdinalField) {
            this.typeOrdinalField = typeOrdinalField;
        }

        @Nullable
        public SampleEntityTypeEnum getTypeOrdinalFieldAsInteger() {
            return typeOrdinalFieldAsInteger;
        }

        public void setTypeOrdinalFieldAsInteger(SampleEntityTypeEnum typeOrdinalFieldAsInteger) {
            this.typeOrdinalFieldAsInteger = typeOrdinalFieldAsInteger;
        }
    }

    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTable();
    }

    @Test
    public void writeEnumeratedValueTest() throws SQLException {

        SampleEntity s = new SampleEntity();

        s.setTypeStringField(SampleEntityTypeEnum.STRING);
        s.setTypeOrdinalField(SampleEntityTypeEnum.INTEGER);

        // Create the JpaMapping:
        JpaMapping<SampleEntity> mapping = new JpaMapping<>(SampleEntity.class);
        // Create the Bulk Inserter:
        PgBulkInsert<SampleEntity> bulkInsert = new PgBulkInsert<>(mapping);

        PGConnection conn = PostgreSqlUtils.getPGConnection(connection);

        bulkInsert.saveAll(conn, Arrays.asList(s));

        ResultSet rs = getAll();

        while (rs.next()) {
            String v0 = rs.getString("enum_string_field");
            Short v1 = rs.getShort("enum_smallint_field");

            Assert.assertEquals("STRING", v0);
            Assert.assertEquals(1, v1.intValue());
        }
    }

    @Test
    public void writeFailsWithWrongTypeMappingTest() {

        SampleEntity s = new SampleEntity();

        s.setTypeOrdinalField(SampleEntityTypeEnum.INTEGER);

        Map<String, DataType> postgresColumnMapping = new HashMap<>();

        postgresColumnMapping.put("enum_smallint_field", DataType.Text);

        // Create the JpaMapping:
        JpaMapping<SampleEntity> mapping = new JpaMapping<>(SampleEntity.class, postgresColumnMapping);

        // Create the Bulk Inserter:
        PgBulkInsert<SampleEntity> bulkInsert = new PgBulkInsert<>(mapping);

        PGConnection conn = PostgreSqlUtils.getPGConnection(connection);

        boolean didThrow = false;
        try {
            bulkInsert.saveAll(conn, Arrays.asList(s));
        } catch(Exception e) {
            didThrow = true;
        }

        Assert.assertEquals(true, didThrow);
    }


    @Test
    public void customEnumTypeMappingTest() throws SQLException {

        SampleEntity s = new SampleEntity();

        s.setTypeOrdinalFieldAsInteger(SampleEntityTypeEnum.INTEGER);

        Map<String, DataType> postgresColumnMapping = new HashMap<>();

        postgresColumnMapping.put("enum_smallint_field_as_integer", DataType.Int4);

        // Create the JpaMapping:
        JpaMapping<SampleEntity> mapping = new JpaMapping<>(SampleEntity.class, postgresColumnMapping);

        // Create the Bulk Inserter:
        PgBulkInsert<SampleEntity> bulkInsert = new PgBulkInsert<>(mapping);

        PGConnection conn = PostgreSqlUtils.getPGConnection(connection);

        bulkInsert.saveAll(conn, Arrays.asList(s));

        ResultSet rs = getAll();

        while (rs.next()) {
            Integer v1 = rs.getInt("enum_smallint_field_as_integer");

            Assert.assertEquals(1, v1.intValue());
        }
    }

    @Test
    public void bulkImportSampleEntities() throws SQLException {
        // Create a large list of People:
        List<SampleEntity> personList = getSampleEntityList(100000);
        // Create the JpaMapping:
        JpaMapping<SampleEntity> mapping = new JpaMapping<>(SampleEntity.class);
        // Create the Bulk Inserter:
        PgBulkInsert<SampleEntity> bulkInsert = new PgBulkInsert<>(mapping);
        // Now save all entities of a given stream:
        bulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), personList.stream());
        // And assert all have been written to the database:
        Assert.assertEquals(100000, getRowCount());
    }

    private List<SampleEntity> getSampleEntityList(int num) {
        List<SampleEntity> results = new ArrayList<>();

        for (int pos = 0; pos < num; pos++) {
            SampleEntity p = new SampleEntity();

            p.setId(pos + 1L);
            p.setIntField(pos);
            p.setTextField(Integer.toString(pos));
            p.setTypeOrdinalField(SampleEntityTypeEnum.INTEGER);
            p.setTypeStringField(SampleEntityTypeEnum.STRING);

            results.add(p);
        }

        return results;
    }

    private boolean createTable() throws SQLException {

        String sqlStatement = "CREATE TABLE public.unit_test" +
                "            (\n" +
                "                id int8,\n" +
                "                int_field int4,\n" +
                "                text_field text,\n" +
                "                read_only_text_field text,\n" +
                "                enum_string_field text,\n" +
                "                enum_smallint_field smallint,\n" +
                "                enum_smallint_field_as_integer int4\n" +
                "            );";

        Statement statement = connection.createStatement();

        return statement.execute(sqlStatement);
    }

    private ResultSet getAll() throws SQLException {
        String sqlStatement = "SELECT * FROM public.unit_test";

        Statement statement = connection.createStatement();

        return statement.executeQuery(sqlStatement);
    }

    private int getRowCount() throws SQLException {

        Statement s = connection.createStatement();

        ResultSet r = s.executeQuery(String.format("SELECT COUNT(*) AS rowcount FROM public.unit_test"));
        r.next();
        int count = r.getInt("rowcount");
        r.close();

        return count;
    }
}
