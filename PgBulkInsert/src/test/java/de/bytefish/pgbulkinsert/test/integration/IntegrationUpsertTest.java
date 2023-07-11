// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.test.integration;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import de.bytefish.pgbulkinsert.PgBulkUpsert;
import de.bytefish.pgbulkinsert.test.mapping.PersonWithIdUpsertMapping;
import de.bytefish.pgbulkinsert.test.model.PersonWithId;
import de.bytefish.pgbulkinsert.test.utils.TransactionalTestBase;
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;

public class IntegrationUpsertTest extends TransactionalTestBase {

	@Override
	protected void onSetUpInTransaction() throws Exception {
		createTable();
	}

	@Test
	public void bulkInsertPersonDataTest() throws SQLException {
		// Create a large list of People:
		List<PersonWithId> personList = getPersonList(100000, "Philipp");
		List<PersonWithId> personListUpdate = getPersonList(50000, "Johann");
		// Create the BulkInserter:
		PgBulkUpsert<PersonWithId> bulkupsert = new PgBulkUpsert<PersonWithId>(new PersonWithIdUpsertMapping(schema));
		// Now save all entities of a given stream:
		bulkupsert.saveAll(PostgreSqlUtils.getPGConnection(connection), personList.stream());
		bulkupsert.saveAll(PostgreSqlUtils.getPGConnection(connection), personListUpdate.stream());
		Assert.assertEquals(50000, getRowCountByName("Philipp"));
		Assert.assertEquals(50000, getRowCountByName("Johann"));
	}

	private List<PersonWithId> getPersonList(int num, String firstName) {
		List<PersonWithId> personList = new ArrayList<>();

		for (long pos = 0; pos < num; pos++) {
			PersonWithId p = new PersonWithId();

			p.setId(pos);
			p.setFirstName(firstName);
			p.setBirthDate(LocalDate.of(1989, 4, 12));

			personList.add(p);
		}

		return personList;
	}

	private boolean createTable() throws SQLException {

		String sqlStatement = String.format("CREATE TABLE %s.unit_test\n", schema) //
				+ "            (\n" //
				+ "                id bigint,\n" //
				+ "                first_name text,\n" //
				+ "                last_name text,\n" //
				+ "                birth_date date,\n" //
				+ "                constraint pk_person primary key (id)\n" //
				+ "            );";

		Statement statement = connection.createStatement();

		return statement.execute(sqlStatement);
	}

	private int getRowCountByName(String name) throws SQLException {

		Statement s = connection.createStatement();

		ResultSet r = s.executeQuery(
				String.format("SELECT COUNT(*) AS rowcount FROM %s.unit_test where first_name='%s'", schema, name));
		r.next();
		int count = r.getInt("rowcount");
		r.close();

		return count;
	}

}
