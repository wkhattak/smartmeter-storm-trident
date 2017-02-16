package com.khattak.bigdata.realtime.sensordataanalytics.smartmeter;


import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.windowing.InMemoryWindowsStoreFactory;
import org.apache.storm.trident.windowing.WindowsStoreFactory;
import org.apache.storm.trident.windowing.config.TumblingDurationWindow;
import org.apache.storm.trident.windowing.config.WindowConfig;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.DRPCClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;

import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;

public class SmartmeterStatisticsTopology {

    private static final String KAFKA_SPOUT_ID = "smartmeter-kafka-spout";
    //private static final String TOPOLOGY_NAME = "smartmeter-statistics-trident-topology";
    private static final String TOPIC_NAME = "smartmeter-readings";
    //private static final String ZOOKEEPER_ROOT = "/cn-001";// used as root to store consumer's offset
    private static final String KAFKA_CONSUMER_ID = UUID.randomUUID().toString(); // should uniquely identify spout
    private static final Logger LOG = Logger.getLogger(SmartmeterStatisticsTopology.class);

    public static void main(String[] args) throws Exception {
    	
    	//create table in hbase
    	createTable(args[1].split(":")[0], Integer.parseInt(args[1].split(":")[1]), "smartmeter_readings");

        ZkHosts zkHosts = new ZkHosts(args[1]);//192.168.70.136:2181
        TridentKafkaConfig tridentKafkaConfig = new TridentKafkaConfig(zkHosts, TOPIC_NAME, KAFKA_CONSUMER_ID);
        tridentKafkaConfig.scheme = new SchemeAsMultiScheme(new SmartmeterMessageScheme());
        tridentKafkaConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        OpaqueTridentKafkaSpout opaqueTridentKafkaSpout = new OpaqueTridentKafkaSpout(tridentKafkaConfig);
                
        TridentTopology tridentTopology = new TridentTopology();
        
        /*TridentState meterStatistics = tridentTopology.newStream(KAFKA_SPOUT_ID + "1", opaqueTridentKafkaSpout)
        		.each(new Fields("timestamp", "meterId","fifteenSecondUsage", "substationId", "city", "maxVoltage", "minVoltage", "powercutDuration"), new HbaseDataInsertion(args[1].split(":")[0], Integer.parseInt(args[1].split(":")[1]), "smartmeter_readings"), new Fields("recordInserted"))
				.each(new Fields("timestamp","meterId"), new HourCalculationAndDateFormat(), new Fields("roundedTimestamp","meterIdHour"))
				//.each(new Fields("timestamp", "meterId","fifteenSecondUsage", "substationId", "city", "maxVoltage", "minVoltage", "powercutDuration","roundedTimestamp","meterIdHour"), new Debug("All fields"))
				.groupBy(new Fields("meterIdHour"))// for efficient grouping and later querying
				.persistentAggregate(new MemoryMapState.Factory(), new Fields("fifteenSecondUsage"), new DoubleSum(), new Fields("hourUsage")); // across all batches
        
        meterStatistics.newValuesStream().each(new Fields("meterIdHour","hourUsage"), new Debug("Hourly usage statistics"));
        
        tridentTopology.newDRPCStream("customer-hourly-usage")//input needs to be the key e.g. "meterId:hourSinceEpoch"
        					.stateQuery(meterStatistics, new Fields("args"), new MapGet(), new Fields("hourUsage"));*/
        
        ////
        /*TridentState overAllStatistics = tridentTopology.newStream(KAFKA_SPOUT_ID + "2", opaqueTridentKafkaSpout)
				.groupBy(new Fields("city"))
				.persistentAggregate(new MemoryMapState.Factory(), new Fields("fifteenSecondUsage"), new DoubleSum(), new Fields("cityUsage")); // across all batches
        
        overAllStatistics.newValuesStream().each(new Fields("city","cityUsage"), new Debug("City usage statistics"));
        
        tridentTopology.newDRPCStream("city-usage")
        					.stateQuery(overAllStatistics, new Fields("args"), new MapGet(), new Fields("cityUsage"));*/
        ////
        
        ////
        /*
        TridentState overAllStatistics = tridentTopology.newStream(KAFKA_SPOUT_ID + "3", opaqueTridentKafkaSpout)
        		.each(new Fields("timestamp"), new Second15Second30SecondMinuteHourCalculation(), new Fields("Second","15Seconds","30Seconds","Minute","Hour"))
				.groupBy(new Fields("Minute"))
				.persistentAggregate(new MemoryMapState.Factory(), new Fields("fifteenSecondUsage"), new DoubleSum(), new Fields("MinuteTotalUsage")); // across all batches
        
        overAllStatistics.newValuesStream().each(new Fields("Minute","MinuteTotalUsage"), new Debug("Minute usage statistics"));
        
        tridentTopology.newDRPCStream("minute-total-usage")//input needs to be the key e.g. "MinutesSinceEpoch"
        					.stateQuery(overAllStatistics, new Fields("args"), new MapGet(), new Fields("MinuteTotalUsage"));// query value needs to start with alpha value else drpc query doesn't work
        ////
        */
        ////
        
        TridentState overAllStatistics = tridentTopology.newStream(KAFKA_SPOUT_ID, opaqueTridentKafkaSpout)
				.persistentAggregate(new MemoryMapState.Factory(), new Fields("fifteenSecondUsage"), new DoubleSum(), new Fields("currentTotalUsage")); // across all batches
        
        //overAllStatistics.newValuesStream().each(new Fields("currentTotalUsage"), new Debug("Total usage statistics"));
        
        tridentTopology.newDRPCStream("total-usage")
        					.stateQuery(overAllStatistics, new Fields("args"), new MapGet(), new Fields("currentTotalUsage"));
        ////
        
        ////
        /*
        WindowConfig windowConfig = TumblingDurationWindow.of(new BaseWindowedBolt.Duration(1, TimeUnit.SECONDS));
    	WindowsStoreFactory inMemoryWindowsStore = new InMemoryWindowsStoreFactory();
        
        Stream stream = tridentTopology.newStream(KAFKA_SPOUT_ID, opaqueTridentKafkaSpout)
        		.window(windowConfig, inMemoryWindowsStore, new Fields("fifteenSecondUsage","city"), new TotalUsageAggregator(), new Fields("totalUsage"))
        		.each(new Fields("cityUsageMap"), new HbaseCityUsageDataInsertion(args[1].split(":")[0], Integer.parseInt(args[1].split(":")[1]), "smartmeter_city_usage"), new Fields("recordInserted"));
        */
        ////
        
        
        /*
         * persistentAggregate is an additional abstraction built on top of partitionPersist that knows how to take a Trident aggregator and use it to apply updates to the source of state.
         * In this case, since this is a grouped stream, Trident expects the state you provide to implement the "MapState" interface. The grouping fields will be the keys in the state, 
         * and the aggregation result will be the values in the state.
         * 
         * When you do aggregations on non-grouped streams (a global aggregation), Trident expects your State object to implement the "Snapshottable" interface.
         * Use $MEMORY-MAP-STATE-GLOBAL$ as the key in the drpc client
         * 
         * MemoryMapState and MemcachedState each implement both of these interfaces.
         */
        
        Config config = new Config();
        //config.setNumWorkers(2);
        config.put(Config.TOPOLOGY_DEBUG, false);// true will log each and every message passing through storm
        
    	try {
    		//args[0] = name of topology
    		StormSubmitter.submitTopology(args[0], config, tridentTopology.build());
    	}
    	catch(AlreadyAliveException aae){
    		System.out.println(aae);
    	}
    	catch(InvalidTopologyException ite){
    		System.out.println(ite);
    	}
    }

	private static void createTable(String hbaseZookeeperIP, int hbaseZookeeperPort, String tableName) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", hbaseZookeeperIP);
		conf.setInt("hbase.zookeeper.property.clientPort", hbaseZookeeperPort);
		Connection conn = null;
		HBaseAdmin admin = null;
		
		try {
			conn = ConnectionFactory.createConnection(conf);
			admin = (HBaseAdmin)conn.getAdmin();

            String[] families = { "reading_data", "meter", "location","fifteen_second_data" };
      
			HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
			
			if (admin.tableExists(tableDesc.getTableName())) {
				LOG.error("Table > " + tableName + " already exists!");
			} 
			else {
			    for (int i = 0; i < families.length; i++) {
			    	tableDesc.addFamily(new HColumnDescriptor(families[i]));
			    }
			    admin.createTable(tableDesc);
			    LOG.info("Create table " + tableName + " ok.");
			}
		} 
		catch (IOException e1) {
			LOG.error("Error while creating connection or getting admin");
			LOG.error(e1.getMessage());
			e1.printStackTrace();
		}
		finally {
			admin.close();
		    conn.close();
		}
	}
}
