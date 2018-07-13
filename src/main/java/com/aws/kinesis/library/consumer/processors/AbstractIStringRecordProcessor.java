package com.aws.kinesis.library.consumer.processors;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

abstract public class AbstractIStringRecordProcessor implements IStringRecordProcessorImpl {
  private static final long BACKOFF_TIME_IN_MILLIS = 3000L;
  private static final int NUM_RETRIES = 10;
  private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L;

  private static final Logger logger = LoggerFactory.getLogger(AbstractIStringRecordProcessor.class);

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
      nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
    }
  }

  @Override
  public void shutdown(ShutdownInput shutdownInput) {
    final IRecordProcessorCheckpointer checkpointer = shutdownInput.getCheckpointer();
    final ShutdownReason reason = shutdownInput.getShutdownReason();

    logger.info("Shutting down record processor for shard: " + kinesisShardId);
    // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
    if (reason == ShutdownReason.TERMINATE) {
      checkpoint(checkpointer);
    }
  }

  @Override
  public void processRecordsWithRetries(List<Record> records) {
    for (Record record : records) {
      boolean processedSuccessfully = false;
      for (int i = 0; i < NUM_RETRIES; i++) {
        try {
          // Logic to process record goes here.
          processSingleRecord(record);
          processedSuccessfully = true;
          break;
        } catch (Throwable t) {
          logger.warn("Caught throwable while processing record " + record, t);
        }
        // backoff if we encounter an exception.
        backoff();
      }
      if (!processedSuccessfully) {
        logger.error("Couldn't process record. Skipping the record.");
        logger.error("skipped record : " + record);
      }
    }
  }


  /** Checkpoint with retries.
   *
   * @param checkpointer
   */
  public void checkpoint(IRecordProcessorCheckpointer checkpointer) {
    logger.info("Checkpointing shard " + kinesisShardId);

    for (int i = 0; i < NUM_RETRIES; i++) {
      try {
        checkpointer.checkpoint();
        break;
      } catch (ShutdownException se) {
        // Ignore checkpoint if the processor instance has been shutdown (fail over).
        logger.info("Caught shutdown exception, skipping checkpoint.", se);
        break;
      } catch (ThrottlingException e) {
        // Backoff and re-attempt checkpoint upon transient failures
        if (i >= (NUM_RETRIES - 1)) {
          logger.error("Checkpoint failed after " + (i + 1) + "attempts.", e);
          break;
        } else {
          logger.info("Transient issue when checkpointing - attempt " + (i + 1) + " of "
            + NUM_RETRIES, e);
        }
      } catch (com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException e) {
        // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
        logger.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
        break;
      }
      backoff();
    }
  }

  public void backoff() {
    try {
      Thread.sleep(BACKOFF_TIME_IN_MILLIS);
    } catch (InterruptedException e) {
      logger.debug("Interrupted sleep", e);
    }
  }

  public String getShardId() {
    return this.kinesisShardId;
  }
}
