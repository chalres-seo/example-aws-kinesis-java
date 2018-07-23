package com.aws.kinesis.api.consumer;

import com.amazonaws.services.kinesis.model.*;
import com.aws.kinesis.api.ApiClient;
import com.aws.kinesis.record.handler.HandlerFactory;
import com.aws.kinesis.record.handler.IRecordsHandler;
import com.sun.istack.internal.NotNull;
import com.sun.istack.internal.Nullable;
import com.utils.AppConfig;
import com.utils.AppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;

/**
 * AWS SDK Kinesis Consumer.
 *
 * The api consumer accepts handler and process it in consume-job.
 * Use handler {@link com.aws.kinesis.record.handler} or create custom handler class.
 *
 * @see {@link com.aws.kinesis.record.handler}
 *
 */
public class ApiConsumer {
  private static Logger logger = LoggerFactory.getLogger(ApiConsumer.class);
  private static long INTERVAL_TIME_MILLIS = AppConfig.getIntervalMillis();

  private final String streamName;
  private final ApiClient apiClient;

  /**
   * Constructor
   *
   * @param apiClient aws kinesis sdk client. otherwise create default client.
   * @param streamName unchecked stream name.
   *
   * @throws ResourceNotFoundException stream is not exist.
   */
  public ApiConsumer(@NotNull final ApiClient apiClient, final String streamName) throws ResourceNotFoundException {
    this.apiClient = apiClient;
    this.streamName = streamName;

    /**
     * check stream validate.
     */
    if (apiClient.isNotStreamExist(streamName)) {
      throw new ResourceNotFoundException("failed create apiConsumer. stream is not exist, name: " + streamName);
    }
  }

  public ApiConsumer(final String streamName) {
    this(new ApiClient(), streamName);
  }

  // Getter >>
  public String getStreamName() { return streamName; }
  // << Getter

  /**
   * Start point consumer.
   *
   * Consumer count is equal to shard count * handler count.
   *
   * @param intervalMillis consume interval millis.
   * @param handlers consume records handler list.
   *
   * @return consume loop future list.
   */
  public List<CompletableFuture<Void>> consume(final ShardIteratorType shardIteratorType, final long intervalMillis, final IRecordsHandler handler, final IRecordsHandler... handlers) {
    logger.debug("consumer start. stream name: " + streamName);

    final List<CompletableFuture<Void>> jobFutures = new ArrayList<>(handlers.length);
    final List<Shard> shardList = apiClient.getShardList(streamName);
    final List<IRecordsHandler> checkedHandlerList = this.getCheckedHandlerList(HandlerFactory.getInstance().mergeHandler(handler, handlers));

    logger.debug("consumer handler count: " + checkedHandlerList.size());
    if (checkedHandlerList.size() > 0) {
      for (Shard shard : shardList) {
        jobFutures.addAll(this.getConsumeJob(intervalMillis, shard, shardIteratorType, checkedHandlerList));
      }
    }

    logger.debug("consumer running job future count: " + jobFutures.size());
    return jobFutures;
  }

  public List<CompletableFuture<Void>> consume(@NotNull final ShardIteratorType shardIteratorType, final IRecordsHandler handler, final IRecordsHandler... handlers) {
    return this.consume(shardIteratorType, INTERVAL_TIME_MILLIS, handler, handlers);
  }

  /**
   * Handler validation.
   *
   * @param handlers unvalidated handler list.
   *
   * @return validated handler list.
   */
  private List<IRecordsHandler> getCheckedHandlerList(@NotNull IRecordsHandler[] handlers) {
    final List<IRecordsHandler> handlerList = new ArrayList<>();

    int handlerCount = 0;
    for (IRecordsHandler handler : handlers) {
      if (handler != null) {
        handlerCount++;
        handlerList.add(handler);
      }
    }

    if (handlerCount != handlers.length) {
      logger.error("there is null point handler. handler count: " + handlerCount + "/" + handlers.length);
    }
    return handlerList;
  }

  /**
   * Consumer job.
   *
   * Job depends on shard count.
   * Job count is equals to shard count.
   *
   * Job is consist of task.
   * Task is consis of handler.
   *
   *
   * @param intervalMillis consume interval.
   * @param shard job on shard.
   * @param handlers record handlers.
   *
   * @return job future list.
   */
  private List<CompletableFuture<Void>> getConsumeJob(final long intervalMillis,
                                                      @NotNull final Shard shard,
                                                      @NotNull ShardIteratorType shardIteratorType,
                                                      @NotNull final List<IRecordsHandler> handlers) {
    logger.debug("get consume job. stream name: " + streamName + ", shardId: " + shard.getShardId() + ", handler count: " + handlers.size());
    final List<CompletableFuture<Void>> taskFutures = new ArrayList<>(handlers.size());

    for (IRecordsHandler handler : handlers) {
      final CompletableFuture<Void> consumeTaskFuture = this.getConsumeTask(intervalMillis, shard, shardIteratorType, handler);

      if (consumeTaskFuture != null) {
        taskFutures.add(consumeTaskFuture);
      }
    }

    logger.debug("consumer running task future count: " + taskFutures.size());
    return taskFutures;
  }

  /**
   * Consumer task.
   *
   * Task depends on shard, handler.
   * Task count is equals to shard count * handler count.
   *
   * @param intervalMillis consume interval.
   * @param shard task on shard
   * @param shardIteratorType shard iterator type {@link ShardIteratorType}
   * @param handler record handler.
   *
   * @return task future list.
   */
  @Nullable
  private CompletableFuture<Void> getConsumeTask(final long intervalMillis,
                                                 @NotNull final Shard shard,
                                                 @NotNull ShardIteratorType shardIteratorType,
                                                 @NotNull final IRecordsHandler handler) {
    logger.debug("get consume task. stream name: " + streamName + ", shardId: " + shard.getShardId() + ", handler: " + handler.getClass().getName());

    Optional<String> getShardIterator = apiClient.getShardIterator(streamName, shard, shardIteratorType);

    if (getShardIterator.isPresent()) {
      return CompletableFuture.runAsync(this.taskLoop(intervalMillis, getShardIterator.get(), handler));
    } else {
      logger.error("failed get consume task. stream: " + streamName + ", shardId: " + shard.getShardId());
      return null;
    }
  }

  /**
   * Task Loop
   *
   * @param intervalMillis consume interval.
   * @param startShardIterator start shard iterator
   * @param handler record handler.
   *
   * @return task runnable task loop.
   */
  private Runnable taskLoop(final long intervalMillis,
                            @NotNull final String startShardIterator,
                            @NotNull final IRecordsHandler handler) {
    logger.debug("consume loop start. stream name: " + streamName + ", shard-iterator: " + startShardIterator +
      ", handler: " + handler.getClass().getName());


    return () -> {
      final GetRecordsRequest getRecordsRequest = new GetRecordsRequest()
        .withShardIterator(startShardIterator);

      while (true) {
        logger.debug("consume next loop. stream name: " + streamName + ", shard-iterator: " + getRecordsRequest.getShardIterator() +
          ", handler: " + handler.getClass().getName());

        final Optional<GetRecordsResult> getRecordsResult = apiClient.getRecords(getRecordsRequest);

        if (getRecordsResult.isPresent() && (getRecordsResult.get().getNextShardIterator() != null)) {
          handler.kinesisRecordsProcess(getRecordsResult.get().getRecords());
          getRecordsRequest.setShardIterator(getRecordsResult.get().getNextShardIterator());
        } else {
          logger.error("failed get records result. stop consume loop, " +
            "stream name: " + streamName + ", shard-iterator: " + getRecordsRequest.getShardIterator() +
            ", handler: " + handler.getClass().getName());
          break;
        }

        AppUtils.backoff("consume task loop. handler: " + handler.getClass().getName(), intervalMillis);
      }
    };
  }
}