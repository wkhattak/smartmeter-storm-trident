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
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HbaseConsumptionDataInsertion extends BaseFunction {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = Logger.getLogger(HbaseConsumptionDataInsertion.class);
    
    private Configuration config = null;
	private Connection conn = null;
	private String hbaseZookeeperIP = null;
	private int hbaseZookeeperPort = 2181;
	private String tablename = null;

    public HbaseConsumptionDataInsertion(String hbaseZookeeperIP, int hbaseZookeeperPort, String tableName){
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
        
		Map<String,String> consumption = (HashMap<String,String>)tuple.getValue(0);
		
		List<CF> cfs = new ArrayList<CF>();
		
		//timestamp for all cities should be same, so just pick the first one
		String timestamp = consumption.values().toArray()[0].toString().split("\\$")[0];
		cfs.add(new CF("timestamp", "key",Bytes.toBytes(timestamp)));// timestamp for records

		for (String cityId:consumption.keySet()){
			//String timestamp = cityUsage.get(key).split("\\$")[0];
			double kwh = Double.parseDouble(consumption.get(cityId).split("\\$")[1]);
			cfs.add(new CF(cityId.toLowerCase(), "fifteen_second", Bytes.toBytes(kwh)));
    	}
		
		try {
			addRecord(tablename, Bytes.toBytes(new Date().getTime()), cfs);
		} 
		catch (Exception e1) {
			LOG.error("Error while inserting record to table: " + tablename);
			LOG.error(e1.getMessage());
			e1.printStackTrace();
		}
        
        List<Object> values = new ArrayList<Object>();
        values.add("hbase_insert_OK");
        collector.emit(values);
    }
    
	/*
	 * This is where the records are actually added
	 */
    private void addRecord(String tableName, byte[] key, List<CF> cfs){
        try {
        	HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
            Table table = conn.getTable(tableDesc.getTableName());
            Put put = new Put(key);
            
            for (CF cf: cfs){
            	put.addColumn(Bytes.toBytes(cf.family), Bytes.toBytes(cf.qualifier), cf.value);// needs the name of CF, column name inside CF & the value for the column
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
		
		public final String family;// the name of the CF
		public final String qualifier ;// the name of the column inside the CF or can say the key
		public final byte[] value ;// the column value
		public CF( String family, String qualifier, byte[] value){
		    this.family = family;
		    this.qualifier = qualifier;
		    this.value = value;
 	   }
	}
}
