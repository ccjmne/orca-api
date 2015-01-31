package org.ccjmne.faomaintenance.api.rest;

public abstract class ResponseEntity {

	public String type;
	public String status;
	public Object entity;

	protected ResponseEntity(final String type, final String status, final Object entity) {
		super();
		this.type = type;
		this.status = status;
		this.entity = entity;
	}

	public static ResponseEntity badReference(final Object entity) {
		return new ResponseFail("BAD_REFERENCE", entity);
	}

	public static ResponseEntity created(final Object entity) {
		return new ResponseSuccess("CREATED", entity);
	}

	public static ResponseEntity deleted(final Object entity) {
		return new ResponseSuccess("DELETED", entity);
	}

	public static ResponseEntity error(final String type, final Object entity) {
		return new ResponseFail(type, entity);
	}

	public static ResponseEntity success(final String type, final Object entity) {
		return new ResponseSuccess(type, entity);
	}

	public static ResponseSuccess updated(final Object entity) {
		return new ResponseSuccess("UPDATED", entity);
	}

	private static class ResponseFail extends ResponseEntity {

		public ResponseFail(final String type, final Object entity) {
			super(type, "FAILED", entity);
		}
	}

	private static class ResponseSuccess extends ResponseEntity {

		public ResponseSuccess(final String type, final Object entity) {
			super(type, "SUCCEEDED", entity);
		}
	}
}
