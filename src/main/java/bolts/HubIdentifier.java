package bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.task.TopologyContext;
import backtype.storm.spout.SpoutOutputCollector;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.FileReader;
import java.io.BufferedReader;
import java.util.Map;
import java.util.Arrays;
import java.lang.Math;

public class HubIdentifier extends BaseBasicBolt {

	private SpoutOutputCollector collector;
	private FileReader fileReader;
	private Object[][] object = new Object[40][4];
	public void prepare(Map conf, TopologyContext context) {
		try{
			this.fileReader = new FileReader(conf.get("AirportsData").toString());
			BufferedReader reader = new BufferedReader(fileReader);
			String str;
			int i=0;
			try {
				while((str = reader.readLine())!=null) {
					str = str.trim();
					if(str.length()!=0) {
						object[i] = str.split(",");	
						i++;
					}
				}
			}catch (Exception ex) { 
				throw new RuntimeException("Error in reading airport data "+ex);
			}
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Error reading file ["+conf.get("AirportsData")+"]");
		}
	}

	public void cleanup() {}

	/**
	 * The bolt will receive the line from the
	 * words file and process it to Normalize this line
	 * 
	 * The normalize will be put the words in lower case
	 * and split the line to get all words in this 
	 */
	public void execute(Tuple input, BasicOutputCollector collector) {
	
	String callSign = input.getStringByField("call sign");
	String latitude = input.getStringByField("latitude");
	String longitude = input.getStringByField("longitude");
	String verticalRate = input.getStringByField("vertical rate");
	String velocity = input.getStringByField("velocity");
        Object[] emitObject = new Object[3];
	
	for(int i=0; i<40 ; i++) {
		/**
 		* Necessary conditions
 		*/
		Boolean latitudeCondition = Math.abs(Float.parseFloat(object[i][2].toString()) - Float.parseFloat(latitude)) <= 0.286;  
		Boolean longitudeCondition = Math.abs(Float.parseFloat(object[i][3].toString()) - Float.parseFloat(longitude)) <= 0.444; 
		
		/**
 		* Additional feature conditions
 		*/
		Boolean rateCondition = Math.abs(Float.parseFloat(verticalRate)) != 0; 
		Boolean velocityCondition =  Math.abs(Float.parseFloat(velocity)) < 50;
		
		if(latitudeCondition && longitudeCondition) {
				emitObject[0] = object[i][0].toString();	
				emitObject[1] = object[i][1].toString();	
				emitObject[2] = input.getValues();	
				//System.out.println("City "+object[i][0].toString()+" code "+object[i][1].toString());
				//System.out.println(" tuple "+input.getValues());
				collector.emit(new Values(emitObject));
		}
	   }
	
	}

	/**
	 * The bolt will only emit the field "word" 
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("airport.city","airport.code","flight"));
	}
}
