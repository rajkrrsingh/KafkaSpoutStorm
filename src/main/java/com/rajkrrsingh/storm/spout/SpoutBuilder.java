package com.rajkrrsingh.storm.spout;

import backtype.storm.spout.SchemeAsMultiScheme;
import com.rajkrrsingh.storm.Keys;
import storm.kafka.*;

import java.util.Properties;


public class SpoutBuilder {
	
	public Properties configs = null;
	
	public SpoutBuilder(Properties configs) {
		this.configs = configs;
	}
	public KafkaSpout buildKafkaSpout() {
		BrokerHosts hosts = new ZkHosts(configs.getProperty(Keys.KAFKA_ZOOKEEPER));
		String topic = configs.getProperty(Keys.KAFKA_TOPIC);
		String zkRoot = configs.getProperty(Keys.KAFKA_ZKROOT);
		String groupId = configs.getProperty(Keys.KAFKA_CONSUMERGROUP);
		SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, groupId);
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
		return kafkaSpout;
	}
}
