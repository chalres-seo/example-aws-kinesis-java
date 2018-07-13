package com.aws.kinesis.library.producer;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.aws.credentials.CredentialsFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class KPLProducerFactory {
  private static Logger logger = LoggerFactory.getLogger(KPLProducerFactory.class);

  private static KinesisProducerConfiguration kplConfigProps = KinesisProducerConfiguration
    .fromPropertiesFile("conf/aws_kinesis_kpl_daemon.properties");

  private static ConcurrentHashMap<String, KPLProducer> kplProducerList = new ConcurrentHashMap<>();

  private KPLProducerFactory() {}

  public static KPLProducerFactory getInstance() {
    return KPLProducerFactory.LazyHolder.INSTANCE;
  }

  private static class LazyHolder {
    private static final KPLProducerFactory INSTANCE = new KPLProducerFactory();
  }

  private KPLProducer createKPLProducer() {
    logger.debug("create kinesis async client.");
    logger.debug("aws profile name : default" );
    logger.debug("aws region name : " + kplConfigProps.getRegion());

    return new KPLProducer(new KinesisProducer(kplConfigProps));
  }

  private KPLProducer createKPLProducer(final String awsRegionName) {
    logger.debug("create kinesis async client.");
    logger.debug("aws profile name : default" );
    logger.debug("aws region name : " + awsRegionName);

    kplConfigProps
      .setCredentialsProvider(CredentialsFactory.getInstance().getCredentialsProvider())
      .setRegion(awsRegionName);
    return new KPLProducer(new KinesisProducer(kplConfigProps));
  }

  private KPLProducer createKPLProducer(final String awsProfileName, final String awsRegionName) {
    logger.debug("create kinesis async client.");
    logger.debug("aws profile name : " + awsProfileName);
    logger.debug("aws resion name : " + awsRegionName);

    kplConfigProps
      .setCredentialsProvider(CredentialsFactory.getInstance().getCredentialsProvider(awsProfileName))
      .setRegion(awsRegionName);
    return new KPLProducer(new KinesisProducer(kplConfigProps));
  }

  public KPLProducer getKPLProducer() {
    logger.debug("[profile::default] get kinesis async client.");

    return kplProducerList
      .computeIfAbsent("default" + "::" + kplConfigProps.getRegion(),
        k -> createKPLProducer());
  }

  public KPLProducer getKPLProducer(final String awsRegionName) {
    logger.debug("[profile::default] get kinesis async client.");

    return kplProducerList
      .computeIfAbsent("default" + "::" + awsRegionName,
        k -> createKPLProducer(awsRegionName));
  }

  public KPLProducer getKPLProducer(final String awsProfileName, final String awsRegionName) {
    logger.debug("[profile::" + awsProfileName + "] get kinesis async client.");
    return kplProducerList
      .computeIfAbsent(awsProfileName + "::" + awsRegionName,
        k -> createKPLProducer(awsProfileName, awsRegionName));
  }
}
