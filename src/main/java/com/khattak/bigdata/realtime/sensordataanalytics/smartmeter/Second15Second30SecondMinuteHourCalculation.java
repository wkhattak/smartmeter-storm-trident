package com.khattak.bigdata.realtime.sensordataanalytics.smartmeter;

//import org.apache.log4j.Logger;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class Second15Second30SecondMinuteHourCalculation extends BaseFunction {
    private static final long serialVersionUID = 1L;
    //private static final Logger LOG = Logger.getLogger(Second15Second30SecondMinuteHourCalculation.class);

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Timestamp ts = Timestamp.valueOf(tuple.getValue(0).toString());
        long milliSeconds = ts.getTime();
        long secondsSinceEpoch = milliSeconds / 1000;
        long fifteenSecondsSinceEpoch = milliSeconds / 1000 / 15;
        long thirtySecondsSinceEpoch = milliSeconds / 1000 / 30;
        long minutesSinceEpoch = milliSeconds / 1000 / 60;
        long hoursSinceEpoch = milliSeconds / 1000 / 60 / 60;

        
        List<Object> values = new ArrayList<Object>();
        values.add("t_" + secondsSinceEpoch); // value needs to start with alpha value else drpc query doesn't work
        values.add("t_" + fifteenSecondsSinceEpoch);
        values.add("t_" + thirtySecondsSinceEpoch);
        values.add("t_" + minutesSinceEpoch);
        values.add("t_" + hoursSinceEpoch);
        collector.emit(values);
    }
}
