package org.ccjmne.orca.api.utils;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Region;

public class S3Client extends AmazonS3Client {

	private static final Regions REGION = Regions.EU_WEST_1;

	/**
	 * Creates an instance of {@link AmazonS3Client} for a specific
	 * {@link Region}.<br />
	 * Expects the system properties <code>aws.accessKeyId</code> and
	 * <code>aws.secretKey</code> to be set appropriately.
	 */
	public S3Client() {
		super();
		this.withRegion(S3Client.REGION);
	}
}
