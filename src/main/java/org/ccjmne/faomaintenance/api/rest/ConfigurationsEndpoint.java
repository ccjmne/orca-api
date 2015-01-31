package org.ccjmne.faomaintenance.api.rest;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.Files;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

@Path("configurations")
public class ConfigurationsEndpoint {

	private static final String CONFIGURATION_DIRECTORY_PATH = "./configs";
	private static Charset UTF8 = Charset.forName("UTF-8");

	private final File configDir;

	public ConfigurationsEndpoint() {
		this.configDir = new File(CONFIGURATION_DIRECTORY_PATH);
		this.configDir.mkdirs();
	}

	private File forName(final String configName) {
		return new File(this.configDir, configName);
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public String[] list() {
		return this.configDir.list();
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("{configName}")
	public String get(@PathParam("configName") final String configName) throws IOException {
		final StringBuilder builder = new StringBuilder();
		for (final String line : Files.readAllLines(forName(configName).toPath(), UTF8)) {
			builder.append(line);
		}

		return builder.toString();
	}

	@PUT
	@Consumes(MediaType.APPLICATION_JSON)
	@Path("{configName}")
	public Response post(@Context final UriInfo uriInfo, @PathParam("configName") final String configName, final String configurationString) throws IOException {
		final File file = forName(configName);
		if (file.exists()) {
			file.delete();
		}

		file.createNewFile();
		try (final Writer w = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), UTF8))) {
			w.write(configurationString);
		}

		return Response.created(uriInfo.getAbsolutePath()).build();
	}

	@DELETE
	@Produces(MediaType.APPLICATION_JSON)
	@Path("{configName}")
	public Response delete(@PathParam("configName") final String configName) {
		if (forName(configName).delete()) {
			return Response.status(Status.OK).build();
		}

		return Response.status(Status.NO_CONTENT).build();
	}
}
