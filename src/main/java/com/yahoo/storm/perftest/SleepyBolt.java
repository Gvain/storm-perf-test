package com.yahoo.storm.perftest;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: jiahong.ljh
 * Date: 14-6-5
 * Time: 下午7:42
 * To change this template use File | Settings | File Templates.
 */
public class SleepyBolt extends BaseRichBolt {
    private OutputCollector _collector;

    public SleepyBolt() {
        //Empty
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            Thread.sleep(600 * 1000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        _collector.emit(tuple, new Values(tuple.getString(0)));
        _collector.ack(tuple);
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("message"));
    }
}

