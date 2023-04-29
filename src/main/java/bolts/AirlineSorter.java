package bolts;

import java.util.HashMap;
import java.util.TreeMap;
import java.util.Map;
import java.util.Arrays;
import java.util.ArrayList;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class AirlineSorter extends BaseBasicBolt {

	Integer id;
	String name;
	Map<String, String> airportCodes;
	Map<String, Map<String,Integer>> combinedCounters;

	/**
	 * At the end of the spout (when the cluster is shutdown
	 * We will show the word counters
	 */
	@Override
	public void cleanup() {
		
		System.out.println("--********** Airline Sorter ["+name+"-"+id+"] *************--");

		for(Map.Entry<String, Map<String, Integer>> combinedMap: combinedCounters.entrySet()) {
			Map<String, Integer> airlinesMap = combinedMap.getValue();
			int count = 0;
			
			System.out.println("At Airport: "+combinedMap.getKey()+"("+airportCodes.get(combinedMap.getKey())+")");
			
			for(Map.Entry<String, Integer> airlineMap: airlinesMap.entrySet()) {
				System.out.println("\t\t"+airlineMap.getKey()+" : "+airlineMap.getValue());
				count = count+airlineMap.getValue();
			} 
			System.out.println("total #flights = "+count+"\n");
		}
		long finish = System.currentTimeMillis();
		System.out.println("******Finish time*********\n"+finish);
	}

	/**
	 * On create 
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		this.airportCodes = new HashMap<String, String>();
		this.combinedCounters = new TreeMap<String, Map<String,Integer>>();
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}


	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		 
		 /**
 		 * Reading the data fields emitted from the HubIdentifier
		 */
		 String this_airport_city = input.getStringByField("airport.city");
		 String this_airport_code = input.getStringByField("airport.code");
		 String flight = input.getValueByField("flight").toString();
		 String[] flightObject = flight.split(",");
		 String airlineCode = flightObject[1].trim().replace("\"","").trim(); 
		 Map<String, Integer> airlineCounters = new HashMap<String,Integer>();
	
		/**
 		* Store the airport city with respective airport code to display in output	
		*/
		if(!airportCodes.containsKey(this_airport_code)){
			airportCodes.put(this_airport_code, this_airport_city);
		}
	
		/**
 		* This condition filters out the data values that are empty or invalid string
 		*/	
		if(airlineCode.length() > 0){

		airlineCode = airlineCode.substring(0,3);
		
		/**
		 * If the airline code dosn't exist in the airport we will create
		 * this, if not We will add 1 
		 */
			
		if(!combinedCounters.containsKey(this_airport_code)){
			airlineCounters.put(airlineCode, 1);	
			combinedCounters.put(this_airport_code, airlineCounters);
		}
		else{
			airlineCounters = combinedCounters.get(this_airport_code);
			if(!airlineCounters.containsKey(airlineCode)){
				airlineCounters.put(airlineCode, 1);
			}
			else {
				Integer count = airlineCounters.get(airlineCode)+1;
				airlineCounters.put(airlineCode, count);
			}
			combinedCounters.put(this_airport_code,airlineCounters);
		}
		}
	}
}
