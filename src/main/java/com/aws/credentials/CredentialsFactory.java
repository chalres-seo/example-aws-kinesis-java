package com.aws.credentials;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.aws.kinesis.api.APIClientCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class CredentialsFactory {
  private static String defaultUserProfile = "default";
  private static String defaultRegionName = "ap-northeast-2";

  private static Logger logger = LoggerFactory.getLogger(APIClientCreator.class);

  private static ConcurrentHashMap<String, AWSCredentialsProvider> credentialsProviderList = new ConcurrentHashMap<>();

  private CredentialsFactory() {}

  private static class LazyHolder {
    private static final CredentialsFactory INSTANCE = new CredentialsFactory();
  }

  public static CredentialsFactory getInstance() {
    return CredentialsFactory.LazyHolder.INSTANCE;
  }

  public AWSCredentialsProvider getCredentialsProvider() {
    logger.debug("get aws default credentials provider.");

    return credentialsProviderList
      .computeIfAbsent("default", k -> this.createCredentialsProvider());
  }

  public AWSCredentialsProvider getCredentialsProvider(final String awsProfile) {
    logger.debug("get aws profile credentials provider.");

    return credentialsProviderList
      .computeIfAbsent(awsProfile, k -> this.createCredentialsProvider(awsProfile));
  }

  private DefaultAWSCredentialsProviderChain createCredentialsProvider() {
    logger.debug("create default credentials provider.");

    return DefaultAWSCredentialsProviderChain.getInstance();
  }

  private ProfileCredentialsProvider createCredentialsProvider(final String awsProfileName) {
    logger.debug("create credentials provider.");
    logger.debug("aws profile name : " + awsProfileName);

    return new ProfileCredentialsProvider(awsProfileName);
  }

  public void refreshAllCredentialsProvider() {
    credentialsProviderList.forEach((profileName, provider) -> provider.refresh());
  }

  public String getDefaultUserProfile() {
    return defaultUserProfile;
  }

  public String getDefaultRegionName() {
    return defaultRegionName;
  }
}
