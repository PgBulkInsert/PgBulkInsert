// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.mapping;

import de.bytefish.pgbulkinsert.PgBulkInsert;
import de.bytefish.pgbulkinsert.model.Person;

public class PersonBulkInserter extends PgBulkInsert<Person>
{
    public PersonBulkInserter() {
        super("sample", "unit_test");

        mapString("first_name", Person::getFirstName);
        mapString("last_name", Person::getLastName);
        mapDate("birth_date", Person::getBirthDate);
    }
}
