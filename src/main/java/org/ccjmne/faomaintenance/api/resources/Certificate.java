package org.ccjmne.faomaintenance.api.resources;

public class Certificate {

	private final int target;
	private final boolean permanentOnly;
	private final String name;

	public Certificate(final int target, final boolean permanentOnly, final String name) {
		this.target = target;
		this.permanentOnly = permanentOnly;
		this.name = name;
	}

	public int getMinimumRate() {
		return this.target;
	}

	public boolean isPermanentOnly() {
		return this.permanentOnly;
	}

	public String getName() {
		return this.name;
	}
}
