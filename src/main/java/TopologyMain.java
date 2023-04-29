import spouts.FlightDataReader;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.AirlineSorter;
import bolts.HubIdentifier;


public class TopologyMain {
	public static void main(String[] args) throws InterruptedException {
         
        //Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("flight-data-reader",new FlightDataReader());
		builder.setBolt("hub-identifier", new HubIdentifier(),10)
			.shuffleGrouping("flight-data-reader");
		builder.setBolt("airline-sorter", new AirlineSorter(),6)
			.fieldsGrouping("hub-identifier", new Fields("airport.city"));
		
        //Configuration
		Config conf = new Config();
		conf.put("FlightsFile", args[0]);
		conf.put("AirportsData", args[1]);
		conf.setDebug(false);
        //Topology run
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();
		long start = System.currentTimeMillis();
		System.out.println("*******Start Time******\n"+start);
		cluster.submitTopology("Flights-Analysis-Toplogie", conf, builder.createTopology());
		Thread.sleep(10000);
		cluster.shutdown();
	}
}
