package org.ccjmne.faomaintenance.api.rest;

import static org.ccjmne.faomaintenance.jooq.classes.Tables.CLIENT;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import javax.activation.FileTypeMap;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;

import org.ccjmne.faomaintenance.api.modules.Restrictions;
import org.ccjmne.faomaintenance.jooq.classes.tables.records.ClientRecord;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.jooq.DSLContext;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.google.common.io.ByteStreams;

@Path("client")
public class ClientEndpoint {

	private static final String ORCA_RESOURCES_BUCKET = "orca-resources";

	private final DSLContext ctx;
	private final AmazonS3Client client;
	private final Restrictions restrictions;

	@Inject
	public ClientEndpoint(final DSLContext ctx, final AmazonS3Client client, final Restrictions restrictions) {
		this.ctx = ctx;
		this.client = client;
		this.restrictions = restrictions;
	}

	@GET
	public ClientRecord getClientInfo() {
		return this.ctx.fetchOne(CLIENT);
	}

	@PUT
	@Consumes(MediaType.APPLICATION_JSON)
	public void updateClientInfo(final Map<String, String> info) {
		if (!this.restrictions.canManageClient()) {
			throw new ForbiddenException();
		}

		this.ctx.update(CLIENT).set(CLIENT.CLNT_NAME, info.get(CLIENT.CLNT_NAME.getName())).set(CLIENT.CLNT_MAILTO, info.get(CLIENT.CLNT_MAILTO.getName()))
				.execute();
	}

	@POST
	public void updateLogo(
							@FormDataParam("file") final InputStream fileStream,
							@FormDataParam("file") final FormDataContentDisposition fileDetail) throws IOException {
		if (!this.restrictions.canManageClient()) {
			throw new ForbiddenException();
		}

		final String contentType = FileTypeMap.getDefaultFileTypeMap().getContentType(fileDetail.getFileName());
		if (!contentType.startsWith("image/")) {
			throw new IllegalArgumentException("Expected an image. Got " + contentType);
		}

		final File file = getFile(fileStream);
		final String objectKey = String.format(
												"%s-logo.%s",
												this.ctx.selectFrom(CLIENT).fetchOne(CLIENT.CLNT_ID),
												contentType.substring(contentType.indexOf("/") + 1));
		this.client.putObject(new PutObjectRequest(ClientEndpoint.ORCA_RESOURCES_BUCKET, objectKey, file).withCannedAcl(CannedAccessControlList.PublicRead));
		this.ctx.update(CLIENT).set(CLIENT.CLNT_LOGO, this.client.getResourceUrl(ClientEndpoint.ORCA_RESOURCES_BUCKET, objectKey)).execute();
		file.delete();
	}

	private static File getFile(final InputStream input) throws IOException {
		final File file = File.createTempFile("client-upload-", ".tmp");
		try (final OutputStream output = new FileOutputStream(file)) {
			ByteStreams.copy(input, output);
		}

		return file;
	}
}
