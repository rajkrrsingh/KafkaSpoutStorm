package com.rajkrrsingh.storm;


public class Keys {
	
	
	public static final String TOPOLOGY_NAME                  = "topology";
	
	//kafka spout
	public static final String KAFKA_SPOUT_ID 					= "kafka-spout";
	public static final String KAFKA_ZOOKEEPER               = "kafka.zookeeper";
	public static final String KAFKA_TOPIC              = "kafa.topic";
	public static final String KAFKA_ZKROOT                    = "kafka.zkRoot";
	public static final String KAFKA_CONSUMERGROUP     = "kafka.consumer.group";
	public static final String KAFKA_SPOUT_COUNT          = "kafkaspout.count";
		
	//sink bolt
	public static final String SINK_TYPE_BOLT_ID = "sink-type-bolt";
	public static final String SINK_BOLT_COUNT = "sinkbolt.count";
	

	//hdfs bolt 
	public static final String HDFS_BOLT_ID = "hdfs-bolt";
	public static final String HDFS_BOLT_COUNT = "hdfsbolt.count";
	public static final String HDFS_FOLDER = "hdfs.folder";
	public static final String HDFS_PORT = "hdfs.port";
	public static final String HDFS_HOST = "hdfs.host";
	

	
	
	
}
