package com.aws.kinesis.api;

import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.model.*;
import com.utils.AppConfig;
import com.utils.AppUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * Management Kinesis stream.
 *
 */
public class ApiClient {
  private static final Logger logger = LoggerFactory.getLogger(ApiClient.class);

  private static final int MAX_RETRY_COUNT = AppConfig.getRetryAttemptCount();
  private static long BACKOFF_TIME_IN_MILLIS = AppConfig.getRetryBackoffTimeInMillis();

  private final String awsProfile;
  private final String awsRegion;
  private final AmazonKinesisAsync kinesisClient;

  /**
   * Constructor
   *
   * @param awsProfile aws account profile name, or use default from AWS CredentialsFactory
   * @param awsRegion  aws region name. of use default from CredentialsFactory
   * @param kinesisClient aws sdk kinesis client
   */
  private ApiClient(final String awsProfile, final String awsRegion, final AmazonKinesisAsync kinesisClient) {
    this.awsProfile = awsProfile;
    this.awsRegion = awsRegion;
    this.kinesisClient = kinesisClient;
  }

  public ApiClient(final String awsProfile, final String awsRegion) {
    this(awsProfile, awsRegion, KinesisSdkClientFactory.getInstance().get(awsProfile, awsRegion));
  }

  public ApiClient() {
    this(AppConfig.getAwsProfile(), AppConfig.getAwsRegion());
  }

  // Getter >>
  public String getAwsProfileName() {
    return this.awsProfile;
  }

  public String getAwsRegionName() {
    return this.awsRegion;
  }
  // << Getter

  public boolean createStream(final String streamName, final int shardCount) {
    logger.debug("create stream. name: " + streamName + ", shard count: " + shardCount);

    for (int retryCount = 1; retryCount <= MAX_RETRY_COUNT; retryCount++) {
      try {
        kinesisClient.createStream(streamName, shardCount);
        return true;
      } catch (ResourceInUseException e) {
        logger.debug("stream already exist. name: " + streamName);
        return true;
      } catch (LimitExceededException e) {
        logger.error("failed create stream. exceeded request limit, name: " + streamName);
        logger.error(e.getMessage());
        AppUtils.backoff("backoff due to failed create stream. name: " + streamName);
      } catch (Exception e) {
        logger.error("failed create stream. unknown exception, name: " + streamName + ", shard count: " + shardCount);
        logger.error(e.getMessage(), e);
        return false;
      }
    }
    logger.error("failed create stream. exceeded retry attempts. name: " + streamName);
    return false;
  }

  public boolean createStream(final String streamName) {
    return this.createStream(streamName, AppConfig.getKinesisShardCount());
  }

  public boolean deleteStream(final String streamName) {
    logger.debug("delete stream. name: " + streamName);

    for (int retryCount = 1; retryCount <= MAX_RETRY_COUNT; retryCount++) {
      try {
        kinesisClient.deleteStream(streamName);
        return true;
      } catch (ResourceNotFoundException e) {
        logger.debug("failed delete stream. stream not exist, name: " + streamName);
        return true;
      } catch (LimitExceededException e) {
        logger.error("failed delete stream. exceeded request limit, name: " + streamName);
        logger.error(e.getMessage());
        AppUtils.backoff("backoff due to failed delete stream. name:" + streamName);
      } catch (Exception e) {
        logger.error("failed delete stream. unknown exception, name: " + streamName);
        logger.error(e.getMessage(), e);
        return false;
      }
    }
    logger.error("failed delete stream. exceeded retry attempts. name: " + streamName);
    return false;
  }

  /**
   * Get stream description.
   *
   * @param streamName unchecked stream name.
   * @param exclusiveStartShardId exclusive shard id. (optional)
   *
   * @return stream description.
   */
  public Optional<StreamDescription> getStreamDesc(final String streamName,
                                                   final String exclusiveStartShardId) {
    logger.debug("get stream description. name: " + streamName);

    for (int retryCount = 1; retryCount <= MAX_RETRY_COUNT; retryCount++) {
      try {
        return Optional.of(kinesisClient.describeStream(streamName, exclusiveStartShardId).getStreamDescription());
      } catch (ResourceNotFoundException e) {
        logger.debug("failed get stream description. stream is not exist, name: " + streamName);
        return Optional.empty();
      } catch (LimitExceededException e) {
        logger.debug("failed get stream description. exceeded request limit, name: " + streamName);
        logger.error(e.getMessage());
        AppUtils.backoff("backoff due to failed get stream description, name: " + streamName);
      } catch (Exception e) {
        logger.error("failed get stream description. unknown exception, name: " + streamName);
        logger.error(e.getMessage(), e);
        return Optional.empty();
      }
    }
    logger.error("failed get stream description. exceeded retry attempts. name: " + streamName);
    return Optional.empty();
  }

  public Optional<StreamDescription> getStreamDesc(final String streamName) {
    return this.getStreamDesc(streamName, null);
  }

  /**
   * Get stream status
   *
   * @param streamName unchecked stream name.
   *
   * @return stream status. {@link StreamStatus}
   *
   *  Exist status : CREATING, DELETING, ACTIVE, UPDATING
   *  Not exist status(custom) : NOT_EXIST
   */
  public String getStreamStatus(final String streamName) {
    logger.debug("get stream desc.");

    return this.getStreamDesc(streamName)
      .map(StreamDescription::getStreamStatus)
      .orElse("NOT_EXIST");
  }

  public boolean isStreamExist(final String streamName) {
    logger.debug("check stream exists. name: " + streamName);

    return !this.getStreamStatus(streamName).equals("NOT_EXIST");
  }

  public boolean isNotStreamExist(final String streamName) {
    return !this.isStreamExist(streamName);
  }

  public boolean isStreamReady(final String streamName) {
    logger.debug("check stream ready. name: " + streamName);

    return this.getStreamStatus(streamName).equals("ACTIVE");
  }

  public boolean isNotStreamReady(final String streamName) {
    return this.isStreamReady(streamName);
  }

  public CompletableFuture<Boolean> watchStreamReady(final String streamName, final long intervalMillis) {
    logger.debug("watch stream ready. name: " + streamName + ", interval millis: " + intervalMillis);

    return CompletableFuture
      .supplyAsync(() -> {
        while (true) {
          final String streamStatus = this.getStreamStatus(streamName);

          switch (streamStatus) {
            case "ACTIVE":
              logger.debug("stream status is " + streamStatus + ". stop watching, name: " + streamName);
              return true;
            case "CREATING":
            case "UPDATING":
              logger.debug("stream status is " + streamStatus + ". backoff for stream will be ready, name: " + streamName);
              AppUtils.backoff("for stream ready.", intervalMillis);
              break;
            case "DELETING":
            case "NOT_EXIST":
            default:
              logger.debug("stream status is " + streamStatus + ". can't watch stream be ready, name: " + streamName);
              return false;
          }
        }
      });
  }

  public CompletableFuture<Boolean> watchStreamReady(final String streamName) {
    return this.watchStreamReady(streamName, BACKOFF_TIME_IN_MILLIS);
  }

  public boolean waitStreamReady(final String streamName, final long intervalMillis, final long waitTimeMillis) {
    logger.debug("wait stream ready. name: "
      + streamName + ", interval millis: "
      + intervalMillis + ", wait millis: "
      + waitTimeMillis);

    try {
      if (waitTimeMillis > 0) {
        return this.watchStreamReady(streamName, intervalMillis).get(waitTimeMillis, TimeUnit.MILLISECONDS);
      } else {
        return this.watchStreamReady(streamName, intervalMillis).get();
      }
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      logger.error("failed wait stream ready. name: " + streamName);
      logger.error(e.getMessage());
      return false;
    }
  }

  public boolean waitStreamReady(final String streamName) {
    return this.waitStreamReady(streamName, BACKOFF_TIME_IN_MILLIS, -1L);
  }

  public CompletableFuture<Boolean> watchStreamDelete(final String streamName, final long intervalMillis) {
    logger.debug("watch stream delete. name: " + streamName + ", interval millis: " + intervalMillis);

    return CompletableFuture
      .supplyAsync(() -> {
        while (true) {
          final String streamStatus = this.getStreamStatus(streamName);

          switch (streamStatus) {
            case "DELETING":
              logger.debug("stream status is " + streamStatus + ". backoff for stream will be delete, name: " + streamName);
              AppUtils.backoff("for stream delete.", intervalMillis);
              break;
            case "NOT_EXIST":
              logger.debug("stream status is " + streamStatus + ". stop watching, name: " + streamName);
              return true;
            case "ACTIVE":
            case "CREATING":
            case "UPDATING":
            default:
              logger.debug("stream status is " + streamStatus + ". can't watch stream be delete, name: " + streamName);
              return false;
          }
        }
      });
  }

  public CompletableFuture<Boolean> watchStreamDelete(final String streamName) {
    return this.watchStreamDelete(streamName, BACKOFF_TIME_IN_MILLIS);
  }

  public boolean waitStreamDelete(final String streamName, final long intervalMillis, final long waitTimeMillis) {
    logger.debug("wait stream delete. name: "
      + streamName + ", interval millis: "
      + intervalMillis + ", wait millis: "
      + waitTimeMillis);

    try {
      if (waitTimeMillis > 0) {
        return this.watchStreamDelete(streamName, intervalMillis).get(waitTimeMillis, TimeUnit.MILLISECONDS);
      } else {
        return this.watchStreamDelete(streamName, intervalMillis).get();
      }
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      logger.error("failed wait stream ready. name: " + streamName);
      logger.error(e.getMessage());
      return false;
    }
  }

  public boolean waitStreamDelete(final String streamName) {
    return this.waitStreamDelete(streamName, BACKOFF_TIME_IN_MILLIS, -1L);
  }

  /**
   * Get stream name list.
   *
   * @return returns an empty list if failed to get list or exception occurs otherwise returns stream list.
   */
  public List<String> getStreamList() {
    logger.debug("get stream list");

    final List<String> streamName = new ArrayList<>();

    for (int i = 1; i <= MAX_RETRY_COUNT; i++) {
      try {
        boolean hasMoreStreams = true;

        while (hasMoreStreams) {
          final ListStreamsResult listStreamsResult;

          if (streamName.size() > 0)
            listStreamsResult = kinesisClient.listStreams(streamName.get(streamName.size() - 1));
          else
            listStreamsResult = kinesisClient.listStreams();

          streamName.addAll(listStreamsResult.getStreamNames());
          hasMoreStreams = listStreamsResult.getHasMoreStreams();
          logger.debug("has more streams: " + hasMoreStreams);
        }

        return streamName;
      } catch (LimitExceededException e) {
        logger.error("failed get stream list. exceeded limit request.");
        logger.error(e.getMessage());
      }
      AppUtils.backoff("backoff due to failed get list streams.");
    }

    logger.error("failed get stream list. exceeded retry attempts.");
    return streamName;
  }

  /**
   * Get shard list.
   *
   * @param streamName unchecked stream name.
   *
   * @return returns an empty list if failed to get list or exception occurs otherwise returns shard list
   */
  public List<Shard> getShardList(final String streamName) {
    logger.debug("get shard list. stream: " + streamName);

    final List<Shard> shards = new ArrayList<>();

    String exclusiveStartShardId = null;

    do {
      final Optional<Boolean> hasMoreShards = this.getStreamDesc(streamName, exclusiveStartShardId)
        .map(streamDescription -> {
          shards.addAll(streamDescription.getShards());
          return streamDescription.getHasMoreShards();
        });

      if (hasMoreShards.orElse(false) && shards.size() > 0) {
        logger.debug("stream has more shards.");

        exclusiveStartShardId = shards.get(shards.size() - 1).getShardId();
      } else {
        exclusiveStartShardId = null;
      }
    } while (exclusiveStartShardId != null);

    return shards;
  }

  /**
   * Get shard-iterator
   *
   * @param streamName unchecked stream name.
   * @param shardIteratorType "TRIM_HORIZON" or "LATEST"
   *                          ("AT_SEQUENCE_NUMBER", "AFTER_SEQUENCE_NUMBER", "AT_TIMESTAMP", not support)
   *                          {@link ShardIteratorType}
   *
   * @return returns an optional type value that is a shard-iterator for the given stream's shard.
   */
  public Optional<String> getShardIterator(final String streamName,
                                           final Shard shard,
                                           final ShardIteratorType shardIteratorType) {
    logger.debug("get shardIterator. stream: " + streamName + ", type: " + shardIteratorType.toString());

    for(int i = 1; i <= MAX_RETRY_COUNT; i++) {
      try {
        return Optional.of(kinesisClient
          .getShardIterator(streamName, shard.getShardId(), shardIteratorType.toString())
          .getShardIterator());
      } catch (ResourceNotFoundException e) {
        logger.error("failed get shard iterator. stream is not exist, " +
          "name: " + streamName + ", shardId: " + shard.getShardId() + ", iterator type: " + shardIteratorType.toString());
        logger.error(e.getMessage());
        return Optional.empty();
      } catch (InvalidArgumentException e) {
        logger.error("failed get shard iterator. invalid argument, " +
          "name: " + streamName + ", shardId: " + shard.getShardId() + ", iterator type: " + shardIteratorType.toString());
        logger.error(e.getMessage());
        return Optional.empty();
      } catch (ProvisionedThroughputExceededException e) {
        logger.debug("failed get shard iterator. exceeded provisioned throughput," +
          "name: " + streamName + ", shardId: " + shard.getShardId() + ", iterator type: " + shardIteratorType.toString());
        AppUtils.backoff("backoff due to failed get shard iterator.n, name: " +
          "name: " + streamName + ", shardId: " + shard.getShardId() + ", iterator type: " + shardIteratorType.toString());
      }
    }

    logger.error("failed get shard iterator. exceeded retry attempts." +
      "name: " + streamName + ", shardId: " + shard.getShardId() + ", iterator type: " + shardIteratorType.toString());
    return Optional.empty();
  }

  /**
   * Get shard-iterator list.
   *
   * @param streamName
   * @param shardIteratorType "TRIM_HORIZON",
   *                          "LATEST"
   *                          ("AT_SEQUENCE_NUMBER", "AFTER_SEQUENCE_NUMBER", "AT_TIMESTAMP", not support)
   * @return returns an empty list if failed to get shard-iterator or exception occurs otherwise returns shard-iterator list
   */
  public List<String> getShardIteratorList(final String streamName,
                                           final ShardIteratorType shardIteratorType) {
    logger.debug("get shardIterator. stream: " + streamName + ", type: " + shardIteratorType.toString());

    final List<Shard> shardList = this.getShardList(streamName);

    // shard list not exist.
    if(shardList.isEmpty()) return new ArrayList<>();

    final Stream<CompletableFuture<Optional<String>>> shardIteratorListFuture = shardList.stream()
      .map(shard -> CompletableFuture.supplyAsync(() ->
        getShardIterator(streamName, shard, shardIteratorType)));

    return shardIteratorListFuture
      .map(future -> {
        try {
          return future.get().orElse(null);
        } catch (InterruptedException | ExecutionException e) {
          logger.error("failed wait get shard iterator future.");
          logger.error(e.getMessage());
          return null;
        }
      }).filter(Objects::nonNull).collect(Collectors.toList());
  }

  /**
   * Put records with PutRecordsRequest.
   *
   * @see {@link PutRecordsRequest}
   *
   * @param putRecordsRequest provided request.
   *
   * @return put record result.
   */
  public Optional<PutRecordsResult> putRecords(PutRecordsRequest putRecordsRequest) {
    logger.debug("put records request. stream name: " + putRecordsRequest.getStreamName() +
      ", count: " + putRecordsRequest.getRecords().size());

    for (int i = 1; i <= MAX_RETRY_COUNT; i++) {
      try {
        return Optional.of(kinesisClient.putRecords(putRecordsRequest));
      } catch (ResourceNotFoundException e) {
        logger.error("failed put records. stream is not exist, name: " + putRecordsRequest.getStreamName());
        return Optional.empty();
      } catch (InvalidArgumentException e) {
        logger.error("failed put records. invaild argument.");
        return Optional.empty();
      } catch (ProvisionedThroughputExceededException e) {
        logger.error("failed put records. exceeded provisioned throughput.");
        AppUtils.backoff("backoff due to failed put records");
      } catch (Exception e) {
        logger.error("failed put records.");
        logger.error(e.getMessage(), e);
        return Optional.empty();
      }
    }
    logger.error("failed put records. exceeded retry attempts");
    return Optional.empty();
  }

  /**
   * Get Records with GetRecordsRequest.
   *
   * @see {@link GetRecordsRequest}
   *
   * @param getRecordsRequest provided request.
   *
   * @return get record result.
   */
  public Optional<GetRecordsResult> getRecords(GetRecordsRequest getRecordsRequest) {
    logger.debug("get records request. shardIterator: " + getRecordsRequest.getShardIterator());

    for (int i = 1; i <= MAX_RETRY_COUNT; i++) {
      try {
        return Optional.of(kinesisClient.getRecords(getRecordsRequest));
      } catch (ResourceNotFoundException e) {
        logger.error("failed get records. stream is not exist");
        return Optional.empty();
      } catch (InvalidArgumentException e) {
        logger.error("failed get records. invaild argument.");
        return Optional.empty();
      } catch (ProvisionedThroughputExceededException e) {
        logger.error("failed get records. exceeded provisioned throughput.");
        AppUtils.backoff("backoff due to failed put records");
      } catch (Exception e) {
        logger.error("failed get records.");
        logger.error(e.getMessage(), e);
        return Optional.empty();
      }
    }
    logger.error("failed get records. exceeded retry attempts");
    return Optional.empty();
  }
}