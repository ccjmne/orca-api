package org.ccjmne.faomaintenance.api.resources;

import java.util.Calendar;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Training {

	private final int id;
	private final Date date;
	private final Type type;

	private String outcome;

	public Training(final int id, final Type type, final Date date) {
		this.id = id;
		this.type = type;
		this.outcome = "SCHEDULED";
		this.date = date;
		final Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.add(Calendar.MONTH, getType().getValidityDuration());
		this.renewDate = calendar.getTime();
	}

	@JsonIgnore
	private final Date renewDate;

	@JsonIgnore
	public Date getRenewDate() {
		return this.renewDate;
	}

	public Date getDate() {
		return this.date;
	}

	@JsonIgnore
	public Type getType() {
		return this.type;
	}

	@JsonProperty("type")
	public int getTypeId() {
		return this.type.getId();
	}

	public String getOutcome() {
		return this.outcome;
	}

	public void setOutcome(final String outcome) {
		this.outcome = outcome;
	}

	public int getId() {
		return this.id;
	}

	public static class Type {

		private final int id;
		private final String name;
		private final Certificate certificate;
		// in months
		private final int validityDuration;

		public Type(final int id, final int validityDuration, final Certificate certificate, final String name) {
			this.id = id;
			this.validityDuration = validityDuration;
			this.certificate = certificate;
			this.name = name;
		}

		public int getValidityDuration() {
			return this.validityDuration;
		}

		public Certificate getCertificate() {
			return this.certificate;
		}

		public int getId() {
			return this.id;
		}

		public String getName() {
			return this.name;
		}
	}
}
