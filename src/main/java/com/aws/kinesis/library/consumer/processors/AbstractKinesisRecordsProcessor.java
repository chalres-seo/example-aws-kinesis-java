package com.aws.kinesis.library.consumer.processors;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;
import com.aws.kinesis.record.handler.IRecordsHandler;
import com.utils.AppConfig;
import com.utils.AppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

abstract public class AbstractKinesisRecordsProcessor implements IKinesisRecordsProcessorImpl {
  private static final Logger logger = LoggerFactory.getLogger(AbstractKinesisRecordsProcessor.class);

  //private ConcurrentHashMap<String, Object> status = new java.util.concurrent.ConcurrentHashMap<String, Object>();

  private long nextCheckpointTimeInMillis;
  private String kinesisShardId;

  @Override
  public void initialize(InitializationInput initializationInput) {
    final String initShardId = initializationInput.getShardId();
    logger.info("Initializing record processor for shard: " + initShardId);
    this.kinesisShardId = initShardId;
  }

  @Override
  public void processRecords(ProcessRecordsInput processRecordsInput) {
    final List<Record> records = processRecordsInput.getRecords();
    IRecordProcessorCheckpointer checkpointer = processRecordsInput.getCheckpointer();

    // Process records and perform all exception handling.
    processRecordsWithRetries(records);

    // Checkpoint once every checkpoint interval.
    if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
      checkpoint(checkpointer);
      nextCheckpointTimeInMillis = System.currentTimeMillis() + AppConfig.getKclCheckPointIntervalMillis();
    }

  }

  @Override
  public void shutdown(ShutdownInput shutdownInput) {
    IRecordProcessorCheckpointer checkpointer = shutdownInput.getCheckpointer();
    ShutdownReason shutdownReason = shutdownInput.getShutdownReason();

    logger.debug("shutting down record processor. " +
      "processor: " + this.getClass().getCanonicalName() + ", " +
      "shard: " + kinesisShardId + ", " +
      "reson: " + shutdownReason);

    if (shutdownReason == ShutdownReason.TERMINATE) {
      checkpoint(checkpointer);
    }
  }

  /** Checkpoint with retries.
   *
   * @param checkpointer
   */
  public void checkpoint(IRecordProcessorCheckpointer checkpointer) {
    logger.info("Checkpointing shard " + kinesisShardId);

    for (int i = 0; i < AppConfig.getRetryAttemptCount(); i++) {
      try {
        checkpointer.checkpoint();
        break;
      } catch (ShutdownException e) {
        // Ignore checkpoint if the processor instance has been shutdown (fail over).
        logger.warn("Caught shutdown exception, skipping checkpoint.");
        break;
      } catch (ThrottlingException e) {
        // Backoff and re-attempt checkpoint upon transient failures
        if (i >= (AppConfig.getRetryAttemptCount() - 1)) {
          logger.error("Checkpoint failed after " + (i + 1) + "attempts.", e);
          break;
        } else {
          logger.debug("Transient issue when checkpointing - attempt " + (i + 1) + " of "
            + AppConfig.getRetryAttemptCount());
          logger.debug(e.getMessage());
        }
      } catch (com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException e) {
        // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
        logger.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
        break;
      }
      AppUtils.backoff("check point.");
    }
  }

  public List<IRecordsHandler> getCheckedHandlerList(IRecordsHandler[] handlers) {
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

  public String getShardId() {
    return this.kinesisShardId;
  }
}
