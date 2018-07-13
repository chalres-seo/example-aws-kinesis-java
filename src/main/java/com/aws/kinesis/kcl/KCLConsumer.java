package com.aws.kinesis.kcl;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.aws.credentials.CredentialsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public final class KCLConsumer {
  private static final Logger logger = LoggerFactory.getLogger(KCLConsumer.class);

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


  public KCLConsumer(String regionName,
                     String streamName,
                     String appName,
                     IRecordProcessorFactory recordProcessorFactory) throws UnknownHostException {
    this(regionName, streamName, appName, recordProcessorFactory, "default");
  }

  public KCLConsumer(String regionName,
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

  public CompletableFuture<Void> consume() {
    return CompletableFuture.runAsync(this::consumeRecords);
  }

  public boolean hasGracefulShutdownStarted() {
    return worker.hasGracefulShutdownStarted();
  }

  public Future<Boolean> startGracefulShutdown() {
    return worker.startGracefulShutdown();
  }

  private void consumeRecords() {
    logger.info("Running {} to process stream {} as worker {}...\n", appName, streamName, workerId);

    int exitCode = 0;

    try {
      worker.run();
    } catch (Throwable t) {
      System.err.println("Caught throwable while processing data.");
      t.printStackTrace();
      exitCode = 1;
    }

    System.exit(exitCode);
  }

  public void deleteResources() {
    AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.standard()
      .withCredentials(credentialsProvider)
      .withRegion(regionName)
      .build();

    logger.info("Deleting the Amazon DynamoDB table used by the Amazon Kinesis Client Library. Table Name = %s.\n", appName);
    try {
      dynamoDB.deleteTable(appName);
    } catch (com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException ex) {
      logger.error("DynamoDB Table is not exist.");
      // The table doesn't exist.
    }
  }
}
