package com.khattak.bigdata.realtime.sensordataanalytics.smartmeter;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import joptsimple.internal.Strings;

public class SmartmeterMessageScheme extends StringScheme {
	
	private static final long serialVersionUID = 1L;
	public static final String FIELD_EVENT_TIME = "timestamp";
	public static final String FIELD_METER_ID  = "meterId";
    public static final String FIELD_FIFTEEN_SECOND_USAGE   = "fifteenSecondUsage";
    public static final String FIELD_SUBSTATION_ID = "substationId";
    public static final String FIELD_CITY  = "city";
    public static final String FIELD_MAX_VOLTAGE   = "maxVoltage";
    public static final String FIELD_MIN_VOLTAGE   = "minVoltage";
    public static final String FIELD_POWERCUT_DURATION   = "powercutDuration";
	private static final Logger LOG = Logger.getLogger(SmartmeterMessageScheme.class);
	
    /**
     * timestamp|meterId|fifteenSecondUsage|substationId|city|maxVoltage|minVoltage|powercutDuration
     * @param bytes
     * @return 
     */
	@Override
	public List<Object> deserialize(ByteBuffer bytes){
		String smartmeterEvent = null;
		try {
			//LOG.info("Deserializing smartmeterEvent...");
			smartmeterEvent = super.deserializeString(bytes);
			//LOG.info("Deserialized message successfully:" + smartmeterEvent);
			String[] data = smartmeterEvent.split("\\|");
						
			Timestamp timestamp = Timestamp.valueOf(cleanup(data[0]));
			String meterId = cleanup(data[1]);
			double fifteenSecondUsage = Double.parseDouble(cleanup(data[2]));
			String substationId = cleanup(data[3]);
			String city = cleanup(data[4]);
			double maxVoltage  = Double.parseDouble(cleanup(data[5]));
			double minVoltage  = Double.parseDouble(cleanup(data[6]));
			double powercutDuration  = Double.parseDouble(cleanup(data[7]));

			return new Values(timestamp, meterId,fifteenSecondUsage, substationId, city, maxVoltage, minVoltage, powercutDuration);
			
		} 
        catch (Exception e) {
            LOG.error("Error occurred while deserializing message: " + (!Strings.isNullOrEmpty(smartmeterEvent) ? smartmeterEvent : "null"));
            LOG.error(e);
            throw new RuntimeException(e);
		}
		
	}
        
	@Override
	public Fields getOutputFields() {
            return new Fields(FIELD_EVENT_TIME,
            				FIELD_METER_ID, 
            				FIELD_FIFTEEN_SECOND_USAGE,
            				FIELD_SUBSTATION_ID, 
            				FIELD_CITY,
            				FIELD_MAX_VOLTAGE,
            				FIELD_MIN_VOLTAGE,
            				FIELD_POWERCUT_DURATION
                            );
	}
        
    private String cleanup(String str) {
        if (str != null) {
            return str.trim().replace("\n", "").replace("\t", "");
        } 
        else {
            return str;
        }
        
    }

}