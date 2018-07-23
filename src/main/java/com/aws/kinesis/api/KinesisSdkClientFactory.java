package com.aws.kinesis.api;

import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClientBuilder;
import com.aws.credentials.CredentialsFactory;
import com.utils.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * AWS SDK Kinesis Client Creator
 *
 */
public class KinesisSdkClientFactory {
  private static Logger logger = LoggerFactory.getLogger(KinesisSdkClientFactory.class);

  private static final String awsProfile = AppConfig.getAwsProfile();
  private static final String awsRegion = AppConfig.getAwsRegion();

  private static ConcurrentHashMap<String, AmazonKinesisAsync> kinesisAsyncClientList = new ConcurrentHashMap<>();

  private KinesisSdkClientFactory() {}

  public static KinesisSdkClientFactory getInstance() {
    return LazyHolder.INSTANCE;
  }

  private static class LazyHolder {
    private static final KinesisSdkClientFactory INSTANCE = new KinesisSdkClientFactory();
  }

  private AmazonKinesisAsync create(final String awsProfileName, final String awsRegionName) {
    logger.debug("get kinesis async client. profile: " + awsProfileName + ", region: " + awsRegionName);

    return AmazonKinesisAsyncClientBuilder
      .standard()
      .withCredentials(CredentialsFactory.getInstance().getCredentialsProvider(awsProfileName))
      .withRegion(awsRegionName)
      .build();
  }

  private AmazonKinesisAsync create() {
    return this.create(awsProfile, awsRegion);
  }

  public AmazonKinesisAsync get(final String awsProfileName, final String awsRegionName) {
    logger.debug("get kinesis async client. profile: " + awsProfileName + ", region: " + awsRegionName);

    return kinesisAsyncClientList
      .computeIfAbsent(awsProfileName + "::" + awsRegionName, k -> create(awsProfileName, awsRegionName));
  }

  public AmazonKinesisAsync get() {
    return this.get(awsProfile, awsRegion);
  }


}
