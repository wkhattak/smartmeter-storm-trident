package org.apache.storm.starter.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.Consumer;
import org.apache.storm.trident.testing.CountAsAggregator;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.windowing.InMemoryWindowsStoreFactory;
import org.apache.storm.trident.windowing.WindowsStoreFactory;
import org.apache.storm.trident.windowing.config.*;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Sample application of trident windowing which uses in-memory store for storing tuples in window.
 */
public class TridentWindowingInmemoryStoreTopology {
    private static final Logger LOG = LoggerFactory.getLogger(TridentWindowingInmemoryStoreTopology.class);

    public static StormTopology buildTopology(WindowsStoreFactory windowStore, WindowConfig windowConfig) throws Exception {
    	
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3, new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"), new Values("four score and seven years ago"),
                new Values("how many apples can you eat"), new Values("to be or not to be the person"));
        
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();

        Stream stream = topology.newStream("spout1", spout).parallelismHint(2)
        				.each(new Fields("sentence"), new Split(), new Fields("word"))
    					.window(windowConfig, windowStore, new Fields("word"), new CountAsAggregator(), new Fields("count"))
    					.peek(new Consumer() {
    											@Override
												public void accept(TridentTuple input) {
    												LOG.info("Received tuple: [{}]", input);
    											}
    					});

        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        WindowsStoreFactory mapState = new InMemoryWindowsStoreFactory();

        /*if (args.length == 0) {
            List<? extends WindowConfig> list = Arrays.asList(
                    SlidingCountWindow.of(1000, 100)
                    ,TumblingCountWindow.of(1000)
                    ,SlidingDurationWindow.of(new BaseWindowedBolt.Duration(6, TimeUnit.SECONDS), new BaseWindowedBolt.Duration(3, TimeUnit.SECONDS))
                    ,TumblingDurationWindow.of(new BaseWindowedBolt.Duration(3, TimeUnit.SECONDS))
            );

            for (WindowConfig windowConfig : list) {
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("wordCounter", conf, buildTopology(mapState, windowConfig));
                Utils.sleep(60 * 1000);
                cluster.shutdown();
            }
            System.exit(0);
        } else {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, buildTopology(mapState, SlidingCountWindow.of(1000, 100)));
        }*/
        
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("wordCounter", conf, buildTopology(mapState, TumblingDurationWindow.of(new BaseWindowedBolt.Duration(15, TimeUnit.SECONDS))));
        Utils.sleep(60 * 1000);
        cluster.shutdown();
    }

}