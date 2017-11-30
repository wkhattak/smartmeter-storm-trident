# Smartmeter Analytics - Realtime Layer
## Overview   
  
A Java library that calculates previous 15 second power consumption across cities & min/max voltage by getting realtime smartmeter readings from a Kafka queue & processing them via Storm.

## Requirements
See [POM file](./pom.xml)


## Usage Example
```java
$STORM_HOME/bin/storm jar smartmeter-storm-trident-0.0.1-jar-with-dependencies.jar com.khattak.bigdata.realtime.sensordataanalytics.smartmeter.SmartmeterStatisticsWindowedTopology smartmeter-statistics-trident-windowed-topology 192.168.70.136:2181
```

## License
The content of this project is licensed under the [Creative Commons Attribution 3.0 license](https://creativecommons.org/licenses/by/3.0/us/deed.en_US).