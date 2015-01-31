package org.ccjmne.faomaintenance.api.resources;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class Employee {

	private final String registrationNumber;
	private final String surname;
	private final String firstName;
	private final Date dateOfBirth;
	private final boolean permanent;

	public Employee(
	                final String registrationNumber,
	                final String surname,
	                final String firstName,
	                final Date dateOfBirth,
	                final boolean permanent,
	                final Map<Training, Boolean> trainings) {
		super();
		this.registrationNumber = registrationNumber;
		this.surname = surname;
		this.firstName = firstName;
		this.dateOfBirth = dateOfBirth;
		this.permanent = permanent;
		this.trainings = trainings;
	}

	@JsonIgnore
	public Map<Training, Boolean> getTrainings() {
		return this.trainings;
	}

	public void setTrainings(final Map<Training, Boolean> trainings) {
		this.trainings = trainings;
	}

	public String getRegistrationNumber() {
		return this.registrationNumber;
	}

	public String getSurname() {
		return this.surname;
	}

	public String getFirstName() {
		return this.firstName;
	}

	public Date getDateOfBirth() {
		return this.dateOfBirth;
	}

	public boolean isPermanent() {
		return this.permanent;
	}

	@JsonIgnore
	public Map<Training, Boolean> trainings = new HashMap<>();

	public Map<String, EmployeeTrainingsStatistics> getStatistics() {
		final Map<String, EmployeeTrainingsStatistics> res = new HashMap<>();
		this.trainings.forEach((t, isValid) -> res.compute(
		                                                   t.getType().getCertificate().getName(),
		                                                   (c, s) -> (s == null) ? new EmployeeTrainingsStatistics(t.getRenewDate(), isValid.booleanValue())
		                                                   : s.renew(t.getRenewDate(), isValid.booleanValue())));
		return res;
	}

	public class EmployeeTrainingsStatistics {

		public Date nextRenew = null;

		public EmployeeTrainingsStatistics(final Date nextRenew, final boolean isValid) {
			if (isValid) {
				this.nextRenew = nextRenew;
			}
		}

		public EmployeeTrainingsStatistics renew(final Date renew, final boolean isValid) {
			if (isValid) {
				if ((this.nextRenew == null) || renew.after(this.nextRenew)) {
					this.nextRenew = renew;
				}
			}

			return this;
		}

		public boolean isCurrentlyValid() {
			return (this.nextRenew != null) && this.nextRenew.after(new Date());
		}
	}
}
