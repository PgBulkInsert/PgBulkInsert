// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.test.mapping;

import de.bytefish.pgbulkinsert.mapping.AbstractUpsertMapping;
import de.bytefish.pgbulkinsert.test.model.PersonWithId;

public class PersonWithIdUpsertMapping extends AbstractUpsertMapping<PersonWithId> {
	public PersonWithIdUpsertMapping(String schema) {
		super(schema, "unit_test", "id");

		mapLongPrimitive("id", PersonWithId::getId);
		mapText("first_name", PersonWithId::getFirstName);
		mapText("last_name", PersonWithId::getLastName);
		mapDate("birth_date", PersonWithId::getBirthDate);
	}
}
