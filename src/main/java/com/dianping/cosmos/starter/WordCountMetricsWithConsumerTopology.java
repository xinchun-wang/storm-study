package com.dianping.cosmos.starter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.metric.LoggingMetricsConsumer;

public class WordCountMetricsWithConsumerTopology extends WordCountTopology{
	public static void main(String[] args) throws Exception {

	    TopologyBuilder builder = buildTopology();

	    Config conf = new Config();

	    if (args != null && args.length > 0) {
	      conf.setNumWorkers(2);
	      conf.setNumAckers(1);
	      conf.registerMetricsConsumer(LoggingMetricsConsumer.class);
	      conf.registerMetricsConsumer(LoggingMetricsConsumer.class, 2);


	      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
	    }
	    else {
	      conf.setMaxTaskParallelism(3);
	      LocalCluster cluster = new LocalCluster();
	      cluster.submitTopology("word-count", conf, builder.createTopology());

	      Thread.sleep(10000);

	      cluster.shutdown();
	    }
	  }
}
