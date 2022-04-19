package org.apache.storm.kafka.spout;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

public class CountBolt implements IRichBolt {
    Map<String, Integer> counters;
    private OutputCollector collector;

    private String workerName;
    private FileWriter fw;
    private BufferedWriter bw;
    private PrintWriter out;
    private String logFile = "C:\\tmp\\stormlog";
    private Map<String, Integer> counts = new HashMap<String, Integer>();

    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.counters = new HashMap<String, Integer>();
        this.collector = collector;

        this.workerName = this.toString();
        try {
            this.fw = new FileWriter(logFile, true);
            this.bw = new BufferedWriter(fw);
            this.out = new PrintWriter(bw);
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    @Override
    public void execute(Tuple input) {
        String str = input.getString(0);

        if(!counters.containsKey(str)){
            counters.put(str, 1);
        }else {
            Integer c = counters.get(str) +1;
            counters.put(str, c);
        }

        collector.ack(input);

        out.println(this.workerName + ": Hello World!");
    }

    @Override
    public void cleanup() {
        for(Map.Entry<String, Integer> entry:counters.entrySet()){
            System.out.println("ATTENTION!");
            System.out.println(entry.getKey()+" : " + entry.getValue());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
