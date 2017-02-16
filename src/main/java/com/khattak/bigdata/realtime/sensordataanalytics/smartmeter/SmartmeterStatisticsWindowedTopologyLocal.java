package com.khattak.bigdata.realtime.sensordataanalytics.smartmeter;

import com.khattak.bigdata.realtime.sensordataanalytics.smartmeter.Utils;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.windowing.InMemoryWindowsStoreFactory;
import org.apache.storm.trident.windowing.WindowsStoreFactory;
import org.apache.storm.trident.windowing.config.TumblingDurationWindow;
import org.apache.storm.trident.windowing.config.WindowConfig;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class SmartmeterStatisticsWindowedTopologyLocal {
	

    private static final String KAFKA_SPOUT_ID = "smartmeter-kafka-spout";
    private static final String TOPOLOGY_NAME = "smartmeter-statistics-trident-topology";
    private static final String TOPIC_NAME = "smartmeter-readings";
    private static final String ZOOKEEPER_ROOT = "/cn-001";// used as root to store consumer's offset
    private static final String KAFKA_CONSUMER_ID = UUID.randomUUID().toString(); // should uniquely identify spout

    public static void main(String[] args) throws Exception {
    	
    	FixedBatchSpout spout = new FixedBatchSpout(new Fields("timestamp","meterId","fifteenSecondUsage","substationId","city","maxVoltage","minVoltage","powercutDuration"), 1000,
                				new Values("2016-07-03 18:42:20.074","C1",0.0015d,"ss1","Birmingham","240.44","235.93","0.0"),
            					new Values("2016-07-03 18:45:25.939","C2",0.0016d,"ss2","Manchester","242.76","238.15","0.0"),
        						new Values("2016-07-03 18:52:20.074","C1",0.0022d,"ss1","Birmingham","240.44","235.93","0.0"),
        						new Values("2016-07-03 18:53:25.939","C2",0.0006d,"ss2","Manchester","242.76","238.15","0.0"));
    	spout.setCycle(true);
    	
                
    	TridentTopology tridentTopology = new TridentTopology();
    	
    	WindowConfig windowConfig = TumblingDurationWindow.of(new BaseWindowedBolt.Duration(15, TimeUnit.SECONDS));
    	WindowsStoreFactory inMemoryWindowsStore = new InMemoryWindowsStoreFactory();

    	/*TridentState overAllStatistics = *///tridentTopology.newStream(KAFKA_SPOUT_ID, spout)
					//.window(windowConfig, inMemoryWindowsStore, new Fields("timestamp","meterId","fifteenSecondUsage","substationId","city","maxVoltage","minVoltage","powercutDuration"), new CityUsageAggregator(), new Fields("cityUsageMap"))
					//.each(new Fields("cityUsageMap"), new Debug("City usage statistics"));
					//.persistentAggregate(new MemoryMapState.Factory(), new Fields("cityUsageMap"), new DoubleSum(), new Fields("cityUsage")); // across all batches


    	//overAllStatistics.newValuesStream().each(new Fields("city","cityUsage"), new Debug("City usage statistics"));

    	//LocalDRPC drpc = new LocalDRPC();
        
        /*tridentTopology.newDRPCStream("city-usage",drpc)
 						.stateQuery(overAllStatistics, new Fields("args"), new MapGet(), new Fields("cityUsage"));*/
         
    	
        Config config = new Config();
        config.put(Config.TOPOLOGY_DEBUG, false);// true will log each and every message passing through storm
        config.put(Config.STORM_LOG4J2_CONF_DIR,"C:\\Users\\Admin\\Documents\\EclipseProjects\\smartmeter-storm-trident\\log4j2");
        
        
    	LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, config, tridentTopology.build());
        
        Utils.waitForSeconds(60);
               
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
    }
}
