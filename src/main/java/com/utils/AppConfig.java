package com.utils;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class AppConfig {
  private static final Logger logger = LoggerFactory.getLogger(AppConfig.class);

  // read application.conf
  private static final Config conf = ConfigFactory.parseFile(new File("conf/application.conf")).resolve();

  private AppConfig() {}

  private static class LazyHolder {
    private static final AppConfig INSTANCE = new AppConfig();
  }

  public static AppConfig getInstance() {
    return AppConfig.LazyHolder.INSTANCE;
  }

  // common config
  public static long getIntervalMillis() { return conf.getLong("intervalMillis"); }

  // Retry config
  public static int getRetryAttemptCount() { return conf.getInt("retry.attemptCount"); }
  public static long getRetryBackoffTimeInMillis() { return conf.getLong("retry.backoffTimeInMillis"); }

  // aws account config
  public static String getAwsProfile() { return conf.getString("aws.profile"); }
  public static String getAwsRegion() { return conf.getString("aws.region"); }

  // kinesis config
  public static int getKinesisShardCount() { return conf.getInt("aws.kinesis.shardCount"); }
  public static ShardIteratorType getShardIteratorType() { return ShardIteratorType.valueOf(conf.getString("aws.kinesis.shardIteratorType")); }

  // kcl config
  public static long getKclCheckPointIntervalMillis() { return conf.getLong("aws.kcl.checkPointIntervalMillis"); }
  public static InitialPositionInStream getKclInitialPositionInStream() { return InitialPositionInStream.valueOf(conf.getString("aws.kcl.initialStreamPosition")); }

  // kpl config
  public static KinesisProducerConfiguration getKplDaemonProps() { return KinesisProducerConfiguration
    .fromPropertiesFile(conf.getString("aws.kpl.daemonPropsPath")); }
}