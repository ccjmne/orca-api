package org.ccjmne.orca.api.rest.pub;

import static org.ccjmne.orca.jooq.codegen.Tables.CLIENT;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.ccjmne.orca.api.inject.business.Restrictions;
import org.ccjmne.orca.jooq.codegen.tables.records.ClientRecord;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.jooq.DSLContext;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.json.Jackson;
import com.google.common.io.ByteStreams;

@Path("client")
public class ClientEndpoint {

  public static final String ORCA_RESOURCES_BUCKET = "orca-resources";

  private final DSLContext     ctx;
  private final AmazonS3Client client;
  private final Restrictions   restrictions;

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

  /**
   * @param fileDetail
   *          Should be used to handle SVG files someday
   */
  @POST
  @Path("logo")
  public void updateLogo(
                         @FormDataParam("file") final InputStream fileStream,
                         @FormDataParam("file") final FormDataContentDisposition fileDetail)
      throws IOException {
    if (!this.restrictions.canManageClient()) {
      throw new ForbiddenException();
    }

    final File file = ClientEndpoint.getFile(fileStream);
    try (BufferedInputStream is = new BufferedInputStream(new FileInputStream(file))) {
      final String mimeType = URLConnection.guessContentTypeFromStream(is);
      // TODO: handle SVG files (it's complicated)
      if (!mimeType.startsWith("image/")) {
        throw new IllegalArgumentException("Expected an image. Got " + mimeType);
      }

      final String objectKey = String.format(
                                             "%s-logo.%s",
                                             this.ctx.selectFrom(CLIENT).fetchOne(CLIENT.CLNT_ID),
                                             mimeType.substring(mimeType.indexOf("/") + 1));
      this.client
          .putObject(new PutObjectRequest(ClientEndpoint.ORCA_RESOURCES_BUCKET, objectKey, file).withCannedAcl(CannedAccessControlList.PublicRead));
      this.ctx.update(CLIENT).set(CLIENT.CLNT_LOGO, this.client.getResourceUrl(ClientEndpoint.ORCA_RESOURCES_BUCKET, objectKey)).execute();
    }

    file.delete();
  }

  @GET
  @Path("welcome")
  @SuppressWarnings("null")
  @Produces(MediaType.APPLICATION_JSON)
  public Map<String, Object> getWelcomeMessage() throws IOException {
    final String objectKey = String.format("%s-welcome.json", this.ctx.selectFrom(CLIENT).fetchOne(CLIENT.CLNT_ID));
    if (this.client.doesObjectExist(ORCA_RESOURCES_BUCKET, objectKey)) {
      try (final S3Object object = this.client
          .getObject(new GetObjectRequest(ORCA_RESOURCES_BUCKET, objectKey))) {
        return Jackson.getObjectMapper().readValue(object.getObjectContent(), HashMap.class);
      }
    }

    return null; // 204 No Content
  }

  @POST
  @Path("welcome")
  @Consumes(MediaType.APPLICATION_JSON)
  public void postWelcomeMessage(final Map<String, Object> contents) throws IOException {
    if (!this.restrictions.canManageClient()) {
      throw new ForbiddenException();
    }

    final byte[] byteArray = Jackson.getObjectMapper().writeValueAsString(contents).getBytes();
    final ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentLength(byteArray.length);
    this.client.putObject(new PutObjectRequest(
                                               ClientEndpoint.ORCA_RESOURCES_BUCKET,
                                               String.format("%s-welcome.json", this.ctx.selectFrom(CLIENT).fetchOne(CLIENT.CLNT_ID)),
                                               new ByteArrayInputStream(byteArray),
                                               metadata)
                                                   .withCannedAcl(CannedAccessControlList.BucketOwnerRead));
  }

  @DELETE
  @Path("welcome")
  public void deleteWelcomeMessage() {
    final String objectKey = String.format("%s-welcome.json", this.ctx.selectFrom(CLIENT).fetchOne(CLIENT.CLNT_ID));
    if (this.client.doesObjectExist(ORCA_RESOURCES_BUCKET, objectKey)) {
      this.client.deleteObject(new DeleteObjectRequest(ClientEndpoint.ORCA_RESOURCES_BUCKET, objectKey));
    }
  }

  private static File getFile(final InputStream input) throws IOException {
    final File file = File.createTempFile("client-upload-", ".tmp");
    try (final OutputStream output = new FileOutputStream(file)) {
      ByteStreams.copy(input, output);
    }

    return file;
  }
}
