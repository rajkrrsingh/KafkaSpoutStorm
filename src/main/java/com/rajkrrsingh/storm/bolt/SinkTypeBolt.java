package com.rajkrrsingh.storm.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import com.rajkrrsingh.storm.Topology;

import java.util.Map;


public class SinkTypeBolt extends BaseRichBolt {


	private static final long serialVersionUID = 1L;
	private OutputCollector collector;
	

	public void execute(Tuple tuple) {
		String value = tuple.getString(0);
		System.out.println("Received in SinkType bolt : "+value);

		collector.emit(Topology.HDFS_STREAM,new Values(value));
		collector.ack(tuple);	
	}


	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(Topology.HDFS_STREAM, new Fields( "content" ));
	}

}
