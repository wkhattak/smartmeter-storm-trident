package com.khattak.bigdata.realtime.sensordataanalytics.smartmeter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class HbaseReadingDataInsertion extends BaseFunction {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = Logger.getLogger(HbaseReadingDataInsertion.class);
    
    private Configuration config = null;
	private Connection conn = null;
	private String hbaseZookeeperIP = null;
	private int hbaseZookeeperPort = 2181;
	private String tablename = null;

    public HbaseReadingDataInsertion(String hbaseZookeeperIP, int hbaseZookeeperPort, String tableName){
		this.hbaseZookeeperIP = hbaseZookeeperIP;
		this.hbaseZookeeperPort = hbaseZookeeperPort;
		this.tablename = tableName;
    }
    
    @Override
    public void prepare(Map conf, TridentOperationContext context){
    	config = HBaseConfiguration.create();
    	config.set("hbase.zookeeper.quorum", hbaseZookeeperIP);
    	config.setInt("hbase.zookeeper.property.clientPort", hbaseZookeeperPort);	
		try {
			conn = ConnectionFactory.createConnection(config);
		} 
		catch (IOException e1) {
			LOG.error("Error while creating connection");
			LOG.error(e1.getMessage());
			e1.printStackTrace();
		}
    }
	
	@Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        
        Timestamp timestamp = (Timestamp)tuple.getValue(0);
		String meterId = (String) tuple.getValue(1);
		String meterType = meterId.split("\\$")[0];
		double fifteenSecondUsage = tuple.getDouble(2);
		String substationId = tuple.getValue(3).toString();
		String city = tuple.getValue(4).toString();
		double maxVoltage  = tuple.getDouble(5);
		double minVoltage  = tuple.getDouble(6);
		double powercutDuration  = tuple.getDouble(7);
        

		try {
   			List<CF> cfs = new ArrayList<CF>();
		
			cfs.add(new CF("reading_data", "timestamp",Bytes.toBytes(Long.toString(timestamp.getTime()))));
			cfs.add(new CF("meter", "id", Bytes.toBytes(meterId)));
			cfs.add(new CF("meter", "type", Bytes.toBytes(meterType)));
			cfs.add(new CF("location", "substationId", Bytes.toBytes(substationId)));
			cfs.add(new CF("location", "city", Bytes.toBytes(city)));
			cfs.add(new CF("fifteen_second_data", "usage", Bytes.toBytes(fifteenSecondUsage)));
			cfs.add(new CF("fifteen_second_data", "max_voltage", Bytes.toBytes(maxVoltage)));
			cfs.add(new CF("fifteen_second_data", "min_voltage", Bytes.toBytes(minVoltage)));
			cfs.add(new CF("fifteen_second_data", "power_cut_duration", Bytes.toBytes(powercutDuration)));
			addRecord(tablename, Bytes.toBytes(new Date().getTime()), cfs);
		} 
		catch (Exception e1) {
			LOG.error("Error while inserting record to table: " + tablename);
			LOG.error(e1.getMessage());
			e1.printStackTrace();
		}
		/*finally {
			admin.close();
		    conn.close();
		}*/
        
        
        List<Object> values = new ArrayList<Object>();
        values.add("hbase_insert_OK");
        collector.emit(values);
    }
    
    private void addRecord(String tableName, byte[] key, List<CF> cfs){
        try {
        	HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
            Table table = conn.getTable(tableDesc.getTableName());
            Put put = new Put(key);
            
            for (CF cf: cfs){
            	put.addColumn(Bytes.toBytes(cf.family), Bytes.toBytes(cf.qualifier), cf.value);
            }
            
            table.put(put);
            table.close();
            LOG.info("Rercord inserted successfully");
        } 
        catch (IOException e) {
        	LOG.error("Error while creating the record");
        	LOG.error(e.getMessage());
			e.printStackTrace();
	    }

    }
    
    private class CF {
		
		public final String family;
		public final String qualifier ;
		public final byte[] value ;
		public CF( String family, String qualifier, byte[] value){
		    this.family = family;
		    this.qualifier = qualifier;
		    this.value = value;
 	   }
	}
}
