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
public class APIClientCreator {
  private static Logger logger = LoggerFactory.getLogger(APIClientCreator.class);

  private static final String DEFAULT_PROFILE = AppConfig.getAwsProfile();
  private static final String DEFAULT_REGION = AppConfig.getAwsRegion();

  private static ConcurrentHashMap<String, AmazonKinesisAsync> kinesisAsyncClientList = new ConcurrentHashMap<>();

  private APIClientCreator() {}

  public static APIClientCreator getInstance() {
    return LazyHolder.INSTANCE;
  }

  private static class LazyHolder {
    private static final APIClientCreator INSTANCE = new APIClientCreator();
  }

  private AmazonKinesisAsync createAPIClient(final String awsProfileName, final String awsRegionName) {
    logger.debug("get kinesis async client. profile: " + awsProfileName + ", region: " + awsRegionName);

    return AmazonKinesisAsyncClientBuilder
      .standard()
      .withCredentials(CredentialsFactory.getInstance().getCredentialsProvider(awsProfileName))
      .withRegion(awsRegionName)
      .build();
  }

  private AmazonKinesisAsync createAPIClient() {
    return this.createAPIClient(DEFAULT_PROFILE, DEFAULT_REGION);
  }

  public AmazonKinesisAsync getAPIClient(final String awsProfileName, final String awsRegionName) {
    logger.debug("get kinesis async client. profile: " + awsProfileName + ", region: " + awsRegionName);

    return kinesisAsyncClientList
      .computeIfAbsent(awsProfileName + "::" + awsRegionName, k -> createAPIClient(awsProfileName, awsRegionName));
  }

  public AmazonKinesisAsync getAPIClient() {
    return this.getAPIClient(DEFAULT_PROFILE, DEFAULT_REGION);
  }


}
