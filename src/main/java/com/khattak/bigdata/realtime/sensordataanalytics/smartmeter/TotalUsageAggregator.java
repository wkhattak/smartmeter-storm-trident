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


public class TotalUsageAggregator extends BaseAggregator<TotalUsageAggregator.State> {

	private static final long serialVersionUID = 1L;
	private static final DecimalFormat df = new DecimalFormat("0.####");

	static class State {
        double totalUsage = 0.0d;
    }
    
    @Override
    public State init(Object batchId, TridentCollector collector) {
        return new State();
    }

    @Override
    public void aggregate(State state, TridentTuple tuple, TridentCollector collector) {
    	double newValue = tuple.getDoubleByField("fifteenSecondUsage");
    	state.totalUsage += newValue;
    }

    @Override
    public void complete(State state, TridentCollector collector) {
    	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    	String now = sdf.format(new Date());
        collector.emit(new Values(now + "$" + state.totalUsage));
    }
    
}



