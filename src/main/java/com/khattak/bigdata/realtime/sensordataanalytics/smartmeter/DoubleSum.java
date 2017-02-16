package com.khattak.bigdata.realtime.sensordataanalytics.smartmeter;

import java.text.DecimalFormat;

import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;


public class DoubleSum implements CombinerAggregator<Double> {

	private static final long serialVersionUID = -887877851296092016L;
	//private static DecimalFormat df = new DecimalFormat("0.####");

	@Override
    public Double init(TridentTuple tuple) {
        return tuple.getDouble(0);
    }

    @Override
    public Double combine(Double val1, Double val2) {
        return val1 + val2;
    }

    @Override
    public Double zero() {
        return 0.0d;
    }
    
}
