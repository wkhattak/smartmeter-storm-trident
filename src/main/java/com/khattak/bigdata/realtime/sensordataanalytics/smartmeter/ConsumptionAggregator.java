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
 * Computes the sum of voltage over a set of tuples
 * The passed in fields are "fifteenSecondUsage","city"
 * The output only contains the ""consumptionMap"" with city as the key and the timestamp$consumption as the value
 */
public class ConsumptionAggregator extends BaseAggregator<ConsumptionAggregator.State> {

	private static final long serialVersionUID = 1L;
	private static final DecimalFormat df = new DecimalFormat("0.####");

	static class State {
        Map<String,String> consumption = new HashMap<String,String>();
    }
    
    @Override
    public State init(Object batchId, TridentCollector collector) {
        return new State();
    }

    @Override
    public void aggregate(State state, TridentTuple tuple, TridentCollector collector) {
    	String cityId = tuple.getStringByField("city");
    	double newValue = tuple.getDoubleByField("fifteenSecondUsage");
    	
        if (!state.consumption.containsKey(cityId)){
        	state.consumption.put(cityId, df.format(newValue));
        }
        else{
        	double oldValue = Double.parseDouble(state.consumption.get(cityId));
        	state.consumption.put(cityId, df.format(oldValue + newValue));
        }
    }

    @Override
    public void complete(State state, TridentCollector collector) {
    	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    	String now = sdf.format(new Date());// Associate the 15 second sum with a timestamp as we can't use the tuple timestamp due to aggregation
    	double totalConsumption = 0.0d;
    	for (String area : state.consumption.keySet()){
    		double consumption = Double.parseDouble(state.consumption.get(area));
    		state.consumption.put(area, now + "$" + consumption);
    		totalConsumption += consumption;
    	}
    	
    	//add country wide consumption
    	state.consumption.put("uk", now + "$" + df.format(totalConsumption));
    	
        collector.emit(new Values(state.consumption));
    }
    
}



