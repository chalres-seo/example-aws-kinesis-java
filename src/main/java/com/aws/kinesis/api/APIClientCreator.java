package com.aws.kinesis.api;

import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClientBuilder;
import com.aws.credentials.CredentialsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * AWS SDK Kinesis Client Creator
 *
 */
public class APIClientCreator {
  private static Logger logger = LoggerFactory.getLogger(APIClientCreator.class);

  private static ConcurrentHashMap<String, AmazonKinesisAsync> kinesisAsyncClientList = new ConcurrentHashMap<>();

  private APIClientCreator() {}

  public static APIClientCreator getInstance() {
    return LazyHolder.INSTANCE;
  }

  private static class LazyHolder {
    private static final APIClientCreator INSTANCE = new APIClientCreator();
  }

  private AmazonKinesisAsync createAPIClient(final String awsRegionName) {
    logger.debug("create kinesis async client.");
    logger.debug("aws profile name : default" );
    logger.debug("aws resion name : " + awsRegionName);

    return AmazonKinesisAsyncClientBuilder
      .standard()
      .withCredentials(CredentialsFactory.getInstance().getCredentialsProvider())
      .withRegion(awsRegionName)
      .build();
  }

  private AmazonKinesisAsync createAPIClient(final String awsProfileName, final String awsRegionName) {
    logger.debug("create kinesis async client.");
    logger.debug("aws profile name : " + awsProfileName);
    logger.debug("aws resion name : " + awsRegionName);

    return AmazonKinesisAsyncClientBuilder
      .standard()
      .withCredentials(CredentialsFactory.getInstance().getCredentialsProvider(awsProfileName))
      .withRegion(awsRegionName)
      .build();
  }

  public AmazonKinesisAsync getAPIClient(final String awsRegionName) {
    logger.debug("[profile::default] get kinesis async client.");

    return kinesisAsyncClientList
      .computeIfAbsent("default" + "::" + awsRegionName,
        k -> createAPIClient(awsRegionName));
  }

  public AmazonKinesisAsync getAPIClient(final String awsProfileName, final String awsRegionName) {
    logger.debug("[profile::" + awsProfileName + "] get kinesis async client.");
    return kinesisAsyncClientList
      .computeIfAbsent(awsProfileName + "::" + awsRegionName,
        k -> createAPIClient(awsProfileName, awsRegionName));
  }

}
