package com.khattak.bigdata.realtime.sensordataanalytics.smartmeter;

import com.khattak.bigdata.realtime.sensordataanalytics.smartmeter.Utils;

import java.util.UUID;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.operation.builtin.Max;
import org.apache.storm.trident.operation.builtin.Min;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;


public class SmartmeterStatisticsTopologyLocal2 {
	

    private static final String KAFKA_SPOUT_ID = "smartmeter-kafka-spout";
    private static final String TOPOLOGY_NAME = "smartmeter-statistics-trident-topology";
    private static final String TOPIC_NAME = "smartmeter-readings";
    private static final String ZOOKEEPER_ROOT = "/cn-001";// used as root to store consumer's offset
    private static final String KAFKA_CONSUMER_ID = UUID.randomUUID().toString(); // should uniquely identify spout

    public static void main(String[] args) throws Exception {
    	
    	FixedBatchSpout spout = new FixedBatchSpout(new Fields("timestamp","meterId","fifteenSecondUsage","substationId","city","maxVoltage","minVoltage","powercutDuration"), 8,
                				new Values("2016-07-03 18:42:20.074","C1",0.0015d,"ss1","Birmingham",242.44d,235.93d,"0.0"),
            					new Values("2016-07-03 18:45:25.939","C2",0.0016d,"ss2","Manchester",241.76d,238.15d,"0.0"),
        						new Values("2016-07-03 18:52:20.074","C1",0.0022d,"ss1","Birmingham",243.50d,237.57d,"0.0"),
        						new Values("2016-07-03 18:53:25.939","C2",0.0006d,"ss2","Manchester",244.63d,239.05d,"0.0"));
    	spout.setCycle(true);
                
        TridentTopology tridentTopology = new TridentTopology();
        
        /*Stream streamMax = tridentTopology.newStream(KAFKA_SPOUT_ID, spout);
        		.persistentAggregate(new MemoryMapState.Factory(), new Fields("maxVoltage"), new CombinerMax(), new Fields("maximumVoltage")); // across all batches*/
				//.aggregate(new Fields("maxVoltage"), new Max("maxVoltage"), new Fields("maximumVoltage"))// only works on a single batch
				//.aggregate(new Fields("minVoltage"), new Min("minVoltage"), new Fields("minimumVoltage"))// only works on a single batch 
				//.chainEnd()
				//.each(new Fields("maximumVoltage"), new Debug("Min-Max debug"));
        		
        
        
        Config config = new Config();
        config.put(Config.TOPOLOGY_DEBUG, false);// true will log each and every message passing through storm
        //config.put(Config.STORM_LOG4J2_CONF_DIR,"C:\\Users\\Admin\\Documents\\EclipseProjects\\smartmeter-storm-trident\\log4j2");
        
    	LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, config, tridentTopology.build());
        
        Utils.waitForSeconds(600);
        
                cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
    }
}
