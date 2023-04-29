package spouts;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;
import java.util.Arrays;
import java.io.IOException;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class FlightDataReader extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private FileReader fileReader;
	private boolean completed = false;
	public void ack(Object msgId) {
		System.out.println("OK:"+msgId);
	}
	public void close() {}
	public void fail(Object msgId) {
		System.out.println("FAIL:"+msgId);
	}

	/**
	 * The only thing that the methods will do It is emit each 
	 * file line
	 */
	public void nextTuple() {
		/**
		 * The nextuple it is called forever, so if we have been readed the file
		 * we will wait and then return
		 */
		if(completed){
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				//Do nothing
			}
			return;
		}
		String str;
		//Open the reader
		BufferedReader reader = new BufferedReader(fileReader);
		
		/**
 		*The parser to read the data from flights.txt 
		*/ 	
		Object[] tuple= new Object[17];
		try{	
			//Read all lines
			while((str = reader.readLine()) != null){
 				if (str.contains("\"states\":")) {
                		 //skip the "states" line
                                	 continue;
                		}
 				if (str.contains("\"time\":")) {
                		 //skip the "states" line
                                	 continue;
				}
			
				if( str.contains("["))	{
					int index=0;
					str = reader.readLine();
					while(!str.contains("]")){
						str = str.trim();
						if(str == null || str.contains("null")) str = Integer.toString(0);
				 		if(str.length()==1 || str.length() == 0)
							 tuple[index] = str;
						else
							 tuple[index] = str.substring(0,str.length()-1);
						index++;
						str = reader.readLine();
					}
				 	this.collector.emit(new Values(tuple));
				}
			}
		}catch(Exception e){
			throw new RuntimeException("Error reading tuple",e);
		}finally{
			completed = true;
		}
	}

	/**
	 * We will create the file and get the collector object
	 */
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		try {
			this.fileReader = new FileReader(conf.get("FlightsFile").toString());
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Error reading file ["+conf.get("FlightsFile")+"]");
		}
		this.collector = collector;
	}

	/**
	 * Declare the output field "word"
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields( "transponder address", "call sign", "origin country", "last timestamp1","last timestamp2", "longitude", "latitude", "altitude (barometric)", "surface or air", "velocity", "degree north = 0", "vertical rate", "sensors","altitude (geometric)", "transponder code", "special purpose","origin"));
	}
}
