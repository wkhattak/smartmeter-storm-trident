package com.khattak.bigdata.realtime.sensordataanalytics.smartmeter;

import org.apache.storm.tuple.Values;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

/*
 * Finds the min/max voltage over a set of tuples
 * The passed in fields are "minVoltage","maxVoltage","city"
 * The output only contains the "minMaxVoltageVoltageMap" with city as the key and the min$max as the value
 */
public class MinMaxVoltageAggregator extends BaseAggregator<MinMaxVoltageAggregator.State> {

	private static final long serialVersionUID = 1L;
	private static final DecimalFormat df = new DecimalFormat("0.####");

	static class State {
        Map<String,String> voltageMap = new HashMap<String,String>();
    }
    
    @Override
    public State init(Object batchId, TridentCollector collector) {
        return new State();
    }

    @Override
    public void aggregate(State state, TridentTuple tuple, TridentCollector collector) {
    	String cityId = tuple.getStringByField("city");
    	double newMinValue = tuple.getDoubleByField("minVoltage");
    	double newMaxValue = tuple.getDoubleByField("maxVoltage");
    	
        if (!state.voltageMap.containsKey(cityId)){
        	state.voltageMap.put(cityId, df.format(newMinValue) + "$" + df.format(newMaxValue));
        }
        else{
        	double oldMinValue = Double.parseDouble(state.voltageMap.get(cityId).split("\\$")[0]);
        	double oldMaxValue = Double.parseDouble(state.voltageMap.get(cityId).split("\\$")[1]);
        	
        	newMinValue = newMinValue < oldMinValue ? newMinValue : oldMinValue;
        	newMaxValue = newMaxValue > oldMaxValue ? newMaxValue : oldMaxValue;
        	state.voltageMap.put(cityId, df.format(newMinValue) + "$" + df.format(newMaxValue));
        }
    }

    @Override
    public void complete(State state, TridentCollector collector) {
    	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    	String now = sdf.format(new Date());
    	double ukMinValue = Double.MAX_VALUE;
    	double ukMaxValue = Double.MIN_VALUE;
    	
    	for (String area : state.voltageMap.keySet()){
    		double minValue = Double.parseDouble(state.voltageMap.get(area).split("\\$")[0]);
    		double maxValue = Double.parseDouble(state.voltageMap.get(area).split("\\$")[1]);
    		
    		ukMinValue = ukMinValue < minValue ? ukMinValue : minValue;
    		ukMaxValue = ukMaxValue > maxValue ? ukMaxValue : maxValue;
    		
    		state.voltageMap.put(area, now + "$" + state.voltageMap.get(area));
    	}
    	
    	//add country min/max
    	state.voltageMap.put("uk", now + "$" + df.format(ukMinValue) + "$" + df.format(ukMaxValue));
    	
        collector.emit(new Values(state.voltageMap));
    }
    
}



