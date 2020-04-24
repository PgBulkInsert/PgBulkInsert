// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.test.jpa;

import de.bytefish.pgbulkinsert.PgBulkInsert;
import de.bytefish.pgbulkinsert.jpa.JpaMapping;
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import de.bytefish.pgbulkinsert.utils.TransactionalTestBase;
import org.junit.Assert;
import org.junit.Test;

import javax.persistence.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class JpaMappingTests extends TransactionalTestBase {

    public enum SampleEntityTypeEnum {
        STRING,
        INTEGER
    }

    @Entity
    @Table(name = "unit_test", schema = "sample")
    public class SampleEntity {

        @Id
        @Column(name = "id")
        private Long id;

        @Column(name = "int_field")
        private Integer intField;

        @Column(name = "text_field")
        private String textField;

        @Enumerated(value = EnumType.STRING)
        @Column(name = "enum_string_field")
        private SampleEntityTypeEnum typeStringField;

        @Enumerated(value = EnumType.ORDINAL)
        @Column(name = "enum_integer_field")
        private SampleEntityTypeEnum typeOrdinalField;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public Integer getIntField() {
            return intField;
        }

        public void setIntField(Integer intField) {
            this.intField = intField;
        }

        public String getTextField() {
            return textField;
        }

        public void setTextField(String textField) {
            this.textField = textField;
        }

        public SampleEntityTypeEnum getTypeStringField() {
            return typeStringField;
        }

        public void setTypeStringField(SampleEntityTypeEnum typeStringField) {
            this.typeStringField = typeStringField;
        }

        public SampleEntityTypeEnum getTypeOrdinalField() {
            return typeOrdinalField;
        }

        public void setTypeOrdinalField(SampleEntityTypeEnum typeOrdinalField) {
            this.typeOrdinalField = typeOrdinalField;
        }
    }


    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTable();
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

        String sqlStatement = String.format("CREATE TABLE %s.unit_test\n", schema) +
                "            (\n" +
                "                id int8,\n" +
                "                int_field int4,\n" +
                "                text_field text,\n" +
                "                enum_string_field text,\n" +
                "                enum_integer_field int4\n" +
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
