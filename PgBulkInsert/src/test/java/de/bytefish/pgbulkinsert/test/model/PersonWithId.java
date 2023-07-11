// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.test.model;

import java.time.LocalDate;

import org.checkerframework.checker.nullness.qual.Nullable;

public class PersonWithId {

	@Nullable
	private long id;

	@Nullable
	private String firstName;

	@Nullable
	private String lastName;

	@Nullable
	private LocalDate birthDate;

	public PersonWithId() {
	}

	@Nullable
	public long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

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