// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.mapping;

import de.bytefish.pgbulkinsert.model.Person;

public class PersonMapping extends AbstractMapping<Person>
{
    public PersonMapping(String schema) {
        super(schema, "unit_test");

        mapText("first_name", Person::getFirstName);
        mapText("last_name", Person::getLastName);
        mapDate("birth_date", Person::getBirthDate);
    }
}
