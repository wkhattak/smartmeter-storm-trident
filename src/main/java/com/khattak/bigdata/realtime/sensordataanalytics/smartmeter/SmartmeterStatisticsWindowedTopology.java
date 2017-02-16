/*
 * calculates 15 second usage across cities  
 */
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
//import org.apache.storm.trident.Stream;
//import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Debug;
//import org.apache.storm.trident.operation.builtin.MapGet;
//import org.apache.storm.trident.operation.builtin.Max;
//import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.windowing.InMemoryWindowsStoreFactory;
import org.apache.storm.trident.windowing.WindowsStoreFactory;
import org.apache.storm.trident.windowing.config.TumblingDurationWindow;
import org.apache.storm.trident.windowing.config.WindowConfig;
import org.apache.storm.tuple.Fields;
//import org.apache.storm.utils.DRPCClient;

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

public class SmartmeterStatisticsWindowedTopology {

    private static final String KAFKA_SPOUT_ID = "smartmeter-kafka-spout";
    //private static final String TOPOLOGY_NAME = "smartmeter-statistics-trident-topology";
    private static final String TOPIC_NAME = "smartmeter-readings";
    //private static final String ZOOKEEPER_ROOT = "/cn-001";// used as root to store consumer's offset
    private static final String KAFKA_CONSUMER_ID = UUID.randomUUID().toString(); // should uniquely identify spout
    private static final Logger LOG = Logger.getLogger(SmartmeterStatisticsWindowedTopology.class);

    public static void main(String[] args) throws Exception {
    	
    	//create tables in hbase for raw readings and computations
    	
    	// these are the CFs, not the columns. Columns are added at the time of data insertion
    	
    	// reading_data CF includes timestamp col
    	// meter CF includes id, type cols 
    	// location CF includes city, substationId cols
    	// fifteen_second_data CF includes min_voltage, max_voltage, power_cut_duration, usage cols
    	String[] columnFamilies = { "reading_data", "meter", "location","fifteen_second_data" };
    	createTable(args[1].split(":")[0], Integer.parseInt(args[1].split(":")[1]), "smartmeter_readings", columnFamilies);
    	
    	// timestamp CF includes key col
    	// Rest of each of the CFs consist of only 1 col named fifteen_second
    	String[] columnFamilies2 = { "timestamp", "london", "birmingham", "manchester","glasgow", "uk"};
    	createTable(args[1].split(":")[0], Integer.parseInt(args[1].split(":")[1]), "smartmeter_consumption", columnFamilies2);
    	
    	// timestamp CF includes key col
    	// Rest of each of the CFs consist of 2 cols named fifteen_second_min & fifteen_second_max
    	String[] columnFamilies3 = { "timestamp", "london", "birmingham", "manchester","glasgow", "uk"};
    	createTable(args[1].split(":")[0], Integer.parseInt(args[1].split(":")[1]), "smartmeter_voltage", columnFamilies3);

    	//setup config for Kafka spout
        ZkHosts zkHosts = new ZkHosts(args[1]);//192.168.70.136:2181
        TridentKafkaConfig tridentKafkaConfig = new TridentKafkaConfig(zkHosts, TOPIC_NAME, KAFKA_CONSUMER_ID);
        tridentKafkaConfig.scheme = new SchemeAsMultiScheme(new SmartmeterMessageScheme()); // dictates how the ByteBuffer consumed from Kafka gets deserialized into a storm tuple
        
        //As the topology runs, the Kafka spout keeps track of the offsets it has read and emitted by storing state information under the ZooKeeper path SpoutConfig.zkRoot+ "/" + SpoutConfig.id. In the case of failures it recovers from the last written offset in ZooKeeper.
        //This means that when a topology has run once the setting KafkaConfig.startOffsetTime will not have an effect for subsequent runs of the topology because now the topology will rely on the consumer state information (offsets) in ZooKeeper to determine from where it should begin (more precisely: resume) reading.
        // If you want to force the spout to ignore any consumer state information stored in ZooKeeper, then you should set the parameter KafkaConfig.ignoreZkOffsets to true. If true, the spout will always begin reading from the offset defined by KafkaConfig.startOffsetTime as described above.
        tridentKafkaConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();// only get any new messages that are being written to the topic
        
        // An opaque spout guarantees that batch contents will not overlap BUT contents may change -- Less taxing than transactional 
        // Transactional requires no overlap & contents should remain the same but this may not be possible if the source (kafka) encounters a problem and can't construct that exact batch straight away  
        // Refe to page 69 of the book "Storm blueprints"
        OpaqueTridentKafkaSpout opaqueTridentKafkaSpout = new OpaqueTridentKafkaSpout(tridentKafkaConfig);
                
        TridentTopology tridentTopology = new TridentTopology();
        
        // create 15 second timed window 
        // A tumbling window totally collapses (all tuples part of that time period vanish without any overlap) while in a sliding window there can be overlap as the window slides over
        WindowConfig windowConfig = TumblingDurationWindow.of(new BaseWindowedBolt.Duration(15, TimeUnit.SECONDS));
        // Where to store the state (tuples) of the tumbling window
    	WindowsStoreFactory inMemoryWindowsStore = new InMemoryWindowsStoreFactory();
        
    	//2 different actions are performed on the kafka spout
    	
    	// these set of operations are to compute the 15 second consumption
        tridentTopology.newStream(KAFKA_SPOUT_ID + "1", opaqueTridentKafkaSpout)
        	// Store raw readings in HBase 
    		.each(new Fields("timestamp", "meterId","fifteenSecondUsage", "substationId", "city", "maxVoltage", "minVoltage", "powercutDuration"), new HbaseReadingDataInsertion(args[1].split(":")[0], Integer.parseInt(args[1].split(":")[1]), "smartmeter_readings"), new Fields("recordInserted"))
    		// Now compute 15 second aggregation over the tumbling window for each city
    		.window(windowConfig, inMemoryWindowsStore, new Fields("fifteenSecondUsage","city"), new ConsumptionAggregator(), new Fields("consumptionMap"))
    		//  Insert the computed consumption figures in HBase smartmeter_consumption table
    		.each(new Fields("consumptionMap"), new HbaseConsumptionDataInsertion(args[1].split(":")[0], Integer.parseInt(args[1].split(":")[1]), "smartmeter_consumption"), new Fields("recordInserted"))
    		// Write output to log for debugging purposes
			.each(new Fields("consumptionMap"), new Debug("Consumption statistics"));

        // these set of operations are to compute the min/max voltage
        tridentTopology.newStream(KAFKA_SPOUT_ID + "2", opaqueTridentKafkaSpout)
        	// Calculate min/max voltage for each city in the past 15 seconds 
			.window(windowConfig, inMemoryWindowsStore, new Fields("minVoltage","maxVoltage","city"), new MinMaxVoltageAggregator(), new Fields("minMaxVoltageVoltageMap"))
			//  Insert the computed min/max figures in HBase smartmeter_voltage table 
			.each(new Fields("minMaxVoltageVoltageMap"), new HbaseVoltageDataInsertion(args[1].split(":")[0], Integer.parseInt(args[1].split(":")[1]), "smartmeter_voltage"), new Fields("recordInserted"))
			// Write output to log for debugging purposes
			.each(new Fields("minMaxVoltageVoltageMap"), new Debug("Voltage statistics"));
        
        
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

    /*
     * Creates HBase table with the required CFs
     */
	private static void createTable(String hbaseZookeeperIP, int hbaseZookeeperPort, String tableName,  String[] columnFamilies) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", hbaseZookeeperIP);
		conf.setInt("hbase.zookeeper.property.clientPort", hbaseZookeeperPort);
		Connection conn = null;
		HBaseAdmin admin = null;
		
		try {
			conn = ConnectionFactory.createConnection(conf);
			admin = (HBaseAdmin)conn.getAdmin();

			HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
			
			if (admin.tableExists(tableDesc.getTableName())) {
				LOG.error("Table > " + tableName + " already exists!");
			} 
			else {
			    for (int i = 0; i < columnFamilies.length; i++) {
			    	tableDesc.addFamily(new HColumnDescriptor(columnFamilies[i]));
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
