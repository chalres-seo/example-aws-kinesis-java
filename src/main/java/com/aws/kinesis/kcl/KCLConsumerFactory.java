package com.aws.kinesis.kcl;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class KCLConsumerFactory {
  private static Logger logger = LoggerFactory.getLogger(KCLConsumerFactory.class);

  private static ConcurrentHashMap<String, KinesisProducer> kplProducerList = new ConcurrentHashMap<>();

  private KCLConsumerFactory() {}

  public static KCLConsumerFactory getInstance() {
    return KCLConsumerFactory.LazyHolder.INSTANCE;
  }

  private static class LazyHolder {
    private static final KCLConsumerFactory INSTANCE = new KCLConsumerFactory();
  }

//  private KinesisProducer createKCLConsumer() {
//    logger.debug("create kinesis async client.");
//    logger.debug("aws profile name : default" );
//    logger.debug("aws region name : " + kplConfigProps.getRegion());
//
//    return new KinesisProducer(kplConfigProps);
//  }
//
//  private KinesisProducer createKCLConsumer(final String awsRegionName) {
//    logger.debug("create kinesis async client.");
//    logger.debug("aws profile name : default" );
//    logger.debug("aws region name : " + awsRegionName);
//
//    kplConfigProps
//      .setCredentialsProvider(CredentialsFactory.getInstance().getCredentialsProvider())
//      .setRegion(awsRegionName);
//    return new KinesisProducer(kplConfigProps);
//  }
//
//  private KinesisProducer createKCLConsumer(final String awsProfileName, final String awsRegionName) {
//    logger.debug("create kinesis async client.");
//    logger.debug("aws profile name : " + awsProfileName);
//    logger.debug("aws resion name : " + awsRegionName);
//
//    kplConfigProps
//      .setCredentialsProvider(CredentialsFactory.getInstance().getCredentialsProvider(awsProfileName))
//      .setRegion(awsRegionName);
//    return new KinesisProducer(kplConfigProps);
//  }
//
//  public KinesisProducer getKCLConsumer() {
//    logger.debug("[profile::default] get kinesis async client.");
//
//    return kplProducerList
//      .computeIfAbsent("default" + "::" + kplConfigProps.getRegion(),
//        k -> createKCLConsumer());
//  }
//
//  public KinesisProducer getKCLConsumer(final String awsRegionName) {
//    logger.debug("[profile::default] get kinesis async client.");
//
//    return kplProducerList
//      .computeIfAbsent("default" + "::" + awsRegionName,
//        k -> createKCLConsumer());
//  }
//
//  public KinesisProducer getKCLConsumer(final String awsProfileName, final String awsRegionName) {
//    logger.debug("[profile::" + awsProfileName + "] get kinesis async client.");
//    return kplProducerList
//      .computeIfAbsent(awsProfileName + "::" + awsRegionName,
//        k -> createKCLConsumer());
//  }
}
