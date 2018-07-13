package com.aws.kinesis.api;

import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.model.*;
import com.aws.credentials.CredentialsFactory;
import com.aws.kinesis.record.ProduceRecord;
import com.aws.kinesis.record.StringProduceRecord;
import com.utils.AppConfig;
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


/**
 * AWS SDK Http Kinesis Client
 *
 */
public class APIClient {
  private static long BACKOFF_TIME_IN_MILLIS = AppConfig.getRetryBackoffTimeInMillis();
  private static int RETRY_COUNT = AppConfig.getRetryAttemptCount();

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
  public APIClient(final String awsProfileName, final String awsRegionName) {
    this.awsProfileName = awsProfileName;
    this.awsRegionName = awsRegionName;
    this.kinesisClient = APIClientCreator.getInstance().getAPIClient(awsProfileName, awsRegionName);
  }

  public APIClient() {
    this(CredentialsFactory.getInstance().getDefaultUserProfile(),
      CredentialsFactory.getInstance().getDefaultRegionName());
  }


  /**
   * Getter
   */
  public String getAwsProfileName() {
    return this.awsProfileName;
  }
  public String getAwsRegionName() {
    return this.awsRegionName;
  }
  public AmazonKinesisAsync getAmazonKinesisClient() {
    return this.kinesisClient;
  }

  public Future<CreateStreamResult> createStream(final String streamName, final int shardCount) {
    logger.debug("create stream. name: " + streamName + ", shard count: " + shardCount);

    return kinesisClient.createStreamAsync(streamName, shardCount);
  }

  public Future<DeleteStreamResult> deleteStream(final String streamName) {
    logger.debug("delete stream. name: " + streamName);

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
  public CompletableFuture<Optional<List<Shard>>> getShardList(final String streamName)
    throws ResourceNotFoundException, LimitExceededException {
    logger.debug("get shard list. stream: " + streamName);

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
            logger.debug("stream has more shards.");

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
  public CompletableFuture<Optional<List<String>>> getShardIterator(final String streamName,
                                                                    final ShardIteratorType shardIteratorType)
    throws ResourceNotFoundException, LimitExceededException {
    logger.debug("get shardIterator. stream: " + streamName + ", type: " + shardIteratorType.toString());

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
                  logger.error("failed get shard iterator.");
                  logger.error(e.getMessage());
                  return null;
                }
              }
            ).filter(Objects::nonNull).collect(Collectors.toList());
        } else {
          logger.error("failed get shard list.");
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
  public CompletableFuture<String> getStreamStatus(final String streamName)
    throws ResourceNotFoundException, LimitExceededException {
    logger.debug("get stream status. stream: " + streamName);

    return CompletableFuture
      .supplyAsync(() -> {
        final String streamStatus = kinesisClient.describeStream(streamName)
          .getStreamDescription()
          .getStreamStatus();
        logger.debug("stream status : " + streamStatus);

        return streamStatus;
      });
  }

  public boolean isStreamExist(final String streamName) throws LimitExceededException {
    logger.debug("check stream is exist. stream: " + streamName);
    try {
      this.getStreamStatus(streamName).get();
    } catch (ResourceNotFoundException e) {
      logger.error("stream is not exist.");
      return false;
    } catch (InterruptedException | ExecutionException e) {
      logger.error("failed get stream status.");
      logger.error(e.getMessage());
      return false;
    }
    return true;
  }

  public boolean isNotStreamExist(final String streamName) throws LimitExceededException {
    return !this.isStreamExist(streamName);
  }

  public boolean isStreamReady(final String streamName) throws ResourceNotFoundException, LimitExceededException {
    logger.debug("check stream is ready. stream: " + streamName);

    try {
      return this.getStreamStatus(streamName).get().equals("ACTIVE");
    } catch (InterruptedException | ExecutionException e) {
      logger.error("failed get stream status.");
      logger.error(e.getMessage());
      return false;
    }
  }


  public boolean isNotStreamReady(final String streamName) throws ResourceNotFoundException, LimitExceededException {
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
    logger.debug("check stream ready with throw. stream: " + streamName);
    if (this.isNotStreamReady(streamName)) {
      logger.debug("stream is not ready.");
      throw new StreamIsNotReadyException("stream is not ready. stream: " + streamName);
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
}
