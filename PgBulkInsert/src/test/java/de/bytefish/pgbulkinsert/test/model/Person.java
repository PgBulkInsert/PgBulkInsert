// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.test.model;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.time.LocalDate;

public class Person {

    @Nullable
    private String firstName;

    @Nullable
    private String lastName;

    @Nullable
    private LocalDate birthDate;

    public Person() {}

    @Nullable
    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    @Nullable
    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    @Nullable
    public LocalDate getBirthDate() {
        return birthDate;
    }

    public void setBirthDate(LocalDate birthDate) {
        this.birthDate = birthDate;
    }

}