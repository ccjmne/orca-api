package org.ccjmne.orca.api.utils;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Region;

public class S3Client extends AmazonS3Client {

  private static final Regions REGION = Regions.EU_WEST_1;

  /**
   * Creates an instance of {@link AmazonS3Client} for a specific
   * {@link Region}.<br />
   * Expects either of these two sets of properties to be set appropriately:
   * <ul>
   * <li>Environment Variables - <code>AWS_ACCESS_KEY_ID</code> and
   * <code>AWS_SECRET_KEY</code></li>
   * <li>Java System Properties - <code>aws.accessKeyId</code> and
   * <code>aws.secretKey</code></li>
   * </ul>
   */
  public S3Client() {
    super();
    this.withRegion(S3Client.REGION);
  }
}
