package com.dianping.cosmos.starter;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

@SuppressWarnings({"rawtypes"})
public class SplitSentenceBolt  extends BaseRichBolt{

    private static final long serialVersionUID = 1L;
    
    private OutputCollector collector;
    @Override
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        this.collector = collector;
        
    }

    @Override
    public void execute(Tuple input) {
        String sentence = input.getString(0);
        String[] words = sentence.split(" ");
        for(String word : words){
            if(word.length() >= 5){
                collector.emit("bigger", input, new Values(word));
            }
            else{
                collector.emit("smaller", input,  new Values(word));
            }
        }
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        Fields fileds = new Fields("word"); 
        declarer.declareStream("bigger", fileds);
        declarer.declareStream("smaller", fileds);        
    }

}
