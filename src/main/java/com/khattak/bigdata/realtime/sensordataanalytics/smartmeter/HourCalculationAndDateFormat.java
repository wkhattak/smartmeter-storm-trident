package com.khattak.bigdata.realtime.sensordataanalytics.smartmeter;

//import org.apache.log4j.Logger;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class HourCalculationAndDateFormat extends BaseFunction {
    private static final long serialVersionUID = 1L;
    //private static final Logger LOG = Logger.getLogger(HourCalculationAndDateFormat.class);

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Timestamp ts = Timestamp.valueOf(tuple.getValue(0).toString());
        String meterId = (String) tuple.getValue(1);
        long milliSeconds = ts.getTime();
        long hourSinceEpoch = milliSeconds / 1000 / 60 / 60;
        //LOG.info("Key =  [" + meterId + ":" + hourSinceEpoch + "]");
        String key = meterId + ":" + hourSinceEpoch;

        Date dt = new Date(milliSeconds);
        Calendar cal = Calendar.getInstance();
        cal.setTime(dt);
        cal.add(Calendar.HOUR,1);// as the usage is for the upcoming hour e.g. 10.34.345 means 34.345 minutes have passed for the 11th hour
        cal.set(Calendar.MINUTE,0);
        cal.set(Calendar.SECOND,0);
        cal.set(Calendar.MILLISECOND,0);
        
        List<Object> values = new ArrayList<Object>();
        SimpleDateFormat simpleFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        values.add(simpleFormatter.format(cal.getTime()));// for display purposes only, the actual groupby will happen based on the key as it is more efficient to do grouping on a single field  
        values.add(key);
        collector.emit(values);
    }
}
