package org.ccjmne.faomaintenance.api.rest.requests;

import java.util.Date;
import java.util.List;

import javax.validation.constraints.NotNull;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class EmployeeRequest {

	@NotNull
	public Identity identity;
	@NotNull
	public Other other;

	public FAOStuff faoStuff;
	public List<Training> trainings;

	public static class Identity {

		@NotNull
		public String code;
		@NotNull
		public String surname;
		@NotNull
		public String firstName;
		@NotNull
		public String aurore;

		public Date dateOfBirth;
		public String jobTitle;
	}

	public static class FAOStuff {

		public String faoTrainer;
		public Date initialFao;
	}

	public static class Training {

		public Training() {
		}

		public Date renewal;
	}

	public static class Other {

		public String status;
		public String contract;
		public String type;
		public float etpContractuel;
		public float etpPayeFinDeMois;
	}
}
