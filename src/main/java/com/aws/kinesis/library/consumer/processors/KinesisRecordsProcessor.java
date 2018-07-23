package com.aws.kinesis.library.consumer.processors;

import com.amazonaws.services.kinesis.model.Record;
import com.aws.kinesis.record.handler.IRecordsHandler;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class KinesisRecordsProcessor extends AbstractKinesisRecordsProcessor {
  private final IRecordsHandler[] handlers;

  private KinesisRecordsProcessor() {
    super();
    this.handlers = null;
  }

  public KinesisRecordsProcessor(IRecordsHandler...handlers) {
    this.handlers = handlers;
  }

  @Override
  public void processRecordsWithRetries(final List<Record> records) {
    // List<CompletableFuture<Void>> futures = new ArrayList<>();
    for (IRecordsHandler handler : handlers) {
      CompletableFuture.runAsync(() -> handler.kinesisRecordsProcess(records));
    }
  }
}
