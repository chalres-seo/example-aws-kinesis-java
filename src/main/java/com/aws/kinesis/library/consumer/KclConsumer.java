package com.aws.kinesis.library.consumer;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.aws.credentials.CredentialsFactory;
import com.utils.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public final class KclConsumer {
  private static final Logger logger = LoggerFactory.getLogger(KclConsumer.class);

  private static final InitialPositionInStream initialStreamPosition = InitialPositionInStream.LATEST;

  private final String regionName;
  private final String streamName;
  private final String appName;
  private final IRecordProcessorFactory recordProcessorFactory;
  private final Worker worker;
  private final String awsProfileName;
  private final AWSCredentialsProvider credentialsProvider;
  private final KinesisClientLibConfiguration kinesisClientLibConfiguration;
  private final String workerId;

  /**
   * Constructor
   *
   * @param regionName aws region name.
   * @param streamName aws kinesis stream name.
   * @param appName aws kinesis client library app name. (= dynamodb table name)
   * @param recordProcessorFactory consume record factory.
   * @param awsProfileName aws profile name.
   *
   * @throws UnknownHostException failed create an kinesis client library app-Id.
   */
  private KclConsumer(String regionName,
                     String streamName,
                     String appName,
                     IRecordProcessorFactory recordProcessorFactory,
                     String awsProfileName) throws UnknownHostException {
    java.security.Security.setProperty("networkaddress.cache.ttl", "60");

    this.regionName = regionName;
    this.streamName = streamName;
    this.appName = appName;
    this.recordProcessorFactory = recordProcessorFactory;
    this.workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
    this.awsProfileName = awsProfileName;
    this.credentialsProvider = CredentialsFactory.getInstance().getCredentialsProvider(this.awsProfileName);

    this.kinesisClientLibConfiguration =
      new KinesisClientLibConfiguration(this.appName, this.streamName, this.credentialsProvider,workerId)
        .withInitialPositionInStream(initialStreamPosition)
        .withRegionName(this.regionName);

    this.worker = new Worker.Builder()
      .recordProcessorFactory(this.recordProcessorFactory)
      .config(this.kinesisClientLibConfiguration)
      .build();
  }

  public KclConsumer(String regionName,
                     String streamName,
                     String appName,
                     IRecordProcessorFactory recordProcessorFactory) throws UnknownHostException {
    this(regionName, streamName, appName, recordProcessorFactory, AppConfig.getAwsProfile());
  }

  public KclConsumer(String streamName,
                     String appName,
                     IRecordProcessorFactory recordProcessorFactory) throws UnknownHostException {
    this(AppConfig.getAwsRegion(), streamName, appName, recordProcessorFactory, AppConfig.getAwsProfile());
  }

  public CompletableFuture<Void> consume() {
    return CompletableFuture.runAsync(this::consumeRecords);
  }

  private void consumeRecords() {
    logger.debug("consumer start. appName: " + appName + ", stream: " + streamName + ", worker: " + workerId);

    int exitCode = 0;

    try {
      worker.run();
    } catch (Throwable t) {
      logger.error("Caught throwable while processing data.");
      logger.error(t.getMessage(), t);
      exitCode = 1;
    }

    logger.debug("shut down consumer workers. exit code: " + exitCode);

    //System.exit(exitCode);
  }

  public boolean hasGracefulShutdownStarted() {
    return worker.hasGracefulShutdownStarted();
  }

  public Future<Boolean> startGracefulShutdown() {
    return worker.startGracefulShutdown();
  }

  public void deleteResources() {
    AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.standard()
      .withCredentials(credentialsProvider)
      .withRegion(regionName)
      .build();

    logger.debug("Deleting the Amazon DynamoDB table used by the Amazon Kinesis Client Library. Table Name: " + appName);
    try {
      dynamoDB.deleteTable(appName);
    } catch (com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException ex) {
      logger.error("DynamoDB Table is not exist.");
      // The table doesn't exist.
    }
  }
}
