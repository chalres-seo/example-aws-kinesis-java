package com.aws.kinesis.api;

import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.model.*;
import com.aws.credentials.CredentialsFactory;
import com.aws.kinesis.record.ProduceRecord;
import com.aws.kinesis.record.StringProduceRecord;
import jdk.nashorn.internal.objects.annotations.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * AWS SDK Http Kinesis Client
 *
 */
public class APIClient {
  private static long BACKOFF_TIME_IN_MILLIS = 1000L;
  private static int RETRY_COUNT = 3;

  private static Logger logger = LoggerFactory.getLogger(APIClient.class);

  private final String awsProfileName;
  private final String awsRegionName;
  private final AmazonKinesisAsync kinesisClient;

  /**
   * Constructor
   *
   * @param awsProfileName aws account profile name, or use default profile name from CredentialsFactory
   * @param awsRegionName aws region name. of use default region name from CredentialsFactory
   */
  public APIClient(String awsProfileName, String awsRegionName) {
    this.awsProfileName = awsProfileName;
    this.awsRegionName = awsRegionName;
    this.kinesisClient = APIClientCreator.getInstance().getAPIClient(awsProfileName, awsRegionName);
  }

  public APIClient() {
    this(CredentialsFactory.getInstance().getDefaultUserProfile(),
      CredentialsFactory.getInstance().getDefaultRegionName());
  }

  public String getAwsProfileName() {
    return this.awsProfileName;
  }

  public String getAwsRegionName() {
    return this.awsRegionName;
  }

  /**
   * Getter
   */
  public AmazonKinesisAsync getAmazonKinesisClient() {
    return this.kinesisClient;
  }

  public Future<CreateStreamResult> createStream(String streamName, int shardCount) {
    return kinesisClient.createStreamAsync(streamName, shardCount);
  }

  public Future<DeleteStreamResult> deleteStream(String streamName) {
    return kinesisClient.deleteStreamAsync(streamName);
  }

  /**
   * Get stream name list.
   *
   * @return optional stream name list future
   *
   * @throws LimitExceededException
   */
  public CompletableFuture<Optional<List<String>>> getStreamList() throws LimitExceededException {
    logger.debug("get stream list");

    return CompletableFuture
      .supplyAsync(() -> {
        ListStreamsResult listStreamsResult = kinesisClient.listStreams();
        final List<String> streamNames = listStreamsResult.getStreamNames();

        while (listStreamsResult.getHasMoreStreams()) {
          logger.debug("has more streams");

          if (streamNames.size() > 0) {
            listStreamsResult = kinesisClient.listStreams(streamNames.get(streamNames.size() - 1));
          } else {
            listStreamsResult = kinesisClient.listStreams();
          }

          streamNames.addAll(listStreamsResult.getStreamNames());
        }

        if(streamNames.size() > 0 )
          return Optional.of(streamNames);
        else
          return Optional.empty();
      });
  }

  /**
   * Get shard list.
   *
   * @param streamName
   *
   * @return optional shard description list future
   *
   * @throws ResourceNotFoundException
   * @throws LimitExceededException
   */
  public CompletableFuture<Optional<List<Shard>>> getShardList(String streamName) throws ResourceNotFoundException, LimitExceededException {
    logger.debug(this.debugStreamPrefix(streamName) + "get shard list.");

    return CompletableFuture
      .supplyAsync(() -> {
        final DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest().withStreamName(streamName);
        final List<Shard> shards = new ArrayList<>();

        DescribeStreamResult describeStreamResult = null;
        String exclusiveStartShardId = null;

        do {
          describeStreamRequest.setExclusiveStartShardId(exclusiveStartShardId);
          describeStreamResult = kinesisClient.describeStream(describeStreamRequest);

          shards.addAll(describeStreamResult.getStreamDescription().getShards());

          if (describeStreamResult.getStreamDescription().getHasMoreShards() && shards.size() > 0) {
            logger.debug(this.debugStreamPrefix(streamName) + "has more shards");
            exclusiveStartShardId = shards.get(shards.size() - 1).getShardId();
          } else {
            exclusiveStartShardId = null;
          }
        } while ( exclusiveStartShardId != null );

        if (shards.size() > 0) {
          return Optional.of(shards);
        } else {
          return Optional.empty();
        }
      });
  }

  /**
   * Get shard iterator list.
   *
   * @param streamName
   * @param shardIteratorType "TRIM_HORIZON",
   *                          "LATEST"
   *                          ("AT_SEQUENCE_NUMBER", "AFTER_SEQUENCE_NUMBER", "AT_TIMESTAMP", not support)
   * @return ShardIterator stream
   *
   * @throws ResourceNotFoundException
   * @throws LimitExceededException
   */
  public CompletableFuture<Optional<List<String>>> getShardIterator(String streamName, ShardIteratorType shardIteratorType) throws ResourceNotFoundException, LimitExceededException {
    logger.debug(this.debugStreamPrefix(streamName) + "get shard iterator list.");

    return this.getShardList(streamName)
      .thenApply(shardList -> {
        final List<String> shardIteratorList;

        if (shardList.isPresent()) {
          shardIteratorList = shardList.get().stream()
            .map(shard ->
              kinesisClient.getShardIteratorAsync(streamName, shard.getShardId(), shardIteratorType.toString()))
            .map(future -> {
                try {
                  return future.get().getShardIterator();
                } catch (InterruptedException | ExecutionException e) {
                  //e.printStackTrace();
                  logger.error(this.debugStreamPrefix(streamName) + "failed get shard iterator.");
                  logger.error(this.debugStreamPrefix(streamName) + e.getMessage());
                  logger.error(this.debugStreamPrefix(streamName) + e.getCause());
                  return null;
                }
              }
            ).filter(Objects::nonNull).collect(Collectors.toList());
        } else {
          logger.error(this.debugStreamPrefix(streamName) + "failed get shard list.");
          shardIteratorList = null;
        }

        if (shardIteratorList == null) {
          return Optional.empty();
        } else {
          return Optional.of(shardIteratorList);
        }
      });
  }

  /**
   * Get Stream Status.
   *
   * @param streamName
   * @return stream status "CREATING", "DELETING", "ACTIVE", "UPDATING"
   *
   * @throws ResourceNotFoundException
   * @throws LimitExceededException
   */
  public CompletableFuture<String> getStreamStatus(String streamName) throws ResourceNotFoundException, LimitExceededException {
    logger.debug(this.debugStreamPrefix(streamName) + "get stream status");

    return CompletableFuture
      .supplyAsync(() -> {
        final String streamStatus = kinesisClient.describeStream(streamName)
          .getStreamDescription()
          .getStreamStatus();
        logger.debug(this.debugStreamPrefix(streamName) + "status : " + streamStatus);

        return streamStatus;
      });
  }

  public boolean isStreamExist(String streamName) throws LimitExceededException {
    logger.debug(this.debugStreamPrefix(streamName) + "check stream is exist.");
    try {
      this.getStreamStatus(streamName).get();
    } catch (ResourceNotFoundException e) {
      logger.error(this.debugStreamPrefix(streamName) + "stream is not exist.");
      logger.error(this.debugStreamPrefix(streamName) + e.getMessage());
      return false;
    } catch (InterruptedException | ExecutionException e) {
      logger.error(this.debugStreamPrefix(streamName) + "get stream status exception");
      logger.error(this.debugStreamPrefix(streamName) + e.getMessage());
      return false;
    }
    return true;
  }

  public boolean isNotStreamExist(String streamName) throws LimitExceededException {
    return !this.isStreamExist(streamName);
  }

  public boolean isStreamReady(String streamName) throws ResourceNotFoundException, LimitExceededException {
    logger.debug(this.debugStreamPrefix(streamName) + "check stream is ready.");

    try {
      return this.getStreamStatus(streamName).get().equals("ACTIVE");
    } catch (InterruptedException | ExecutionException e) {
      logger.error(this.debugStreamPrefix(streamName) + "get stream status exception");
      logger.error(this.debugStreamPrefix(streamName) + e.getMessage());
      return false;
    }
  }


  public boolean isNotStreamReady(String streamName) throws ResourceNotFoundException, LimitExceededException {
    return !this.isStreamReady(streamName);
  }

  /**
   * create example json format string record for test.
   *
   * @param recordCount create example record count.
   * @return json format string record list.
   */
  public List<ProduceRecord> createExampleStringRecords(int recordCount) {
    logger.debug("create example records : " + recordCount);

    final List<ProduceRecord> produceReceords = new ArrayList<>();

    for(int i = 0; i < recordCount; i++) {
      produceReceords.add(new StringProduceRecord(String.format("{\"key\" : \"key_%d\", " +
        "\"string_value\" : \"string_value_%d\", " +
        "\"numeric_value\" : %d}", i, i, i)));
    }

    return produceReceords;
  }


  public boolean backoff(int tryCount) {
    try {
      logger.debug("backoff " + BACKOFF_TIME_IN_MILLIS + " millis");
      Thread.sleep(BACKOFF_TIME_IN_MILLIS);
    } catch (InterruptedException e) {
      logger.debug("Interrupted sleep", e);
    }

    return tryCount > RETRY_COUNT;
  }
  /**
   * check stream ready with custom exception.
   *
   * @throws StreamIsNotReadyException
   */
  public void checkStreamReadyIfNotThrow(String streamName) throws StreamIsNotReadyException, ResourceNotFoundException, LimitExceededException {
    if (this.isNotStreamReady(streamName)) {
      logger.debug(this.debugStreamPrefix(streamName) + "stream is not ready.");
      throw new StreamIsNotReadyException(this.debugStreamPrefix(streamName) + "stream is not ready.");
    }
  }

  /**
   * custom exception
   */
  public class StreamIsNotReadyException extends com.amazonaws.services.kinesis.model.AmazonKinesisException {
    private static final long serialVersionUID = 1L;

    /**
     * Constructs a new StreamIsNotReadyException with the specified error message.
     *
     * @param message Describes the error encountered.
     */
    StreamIsNotReadyException(String message) {
      super(message);
    }
  }

  public String debugStreamPrefix(String streamName) {
    return "[stream::" + streamName + "] ";
  }
}
