package stormTest;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

/**
 * @ author: create by LuJuHui
 * @ date:2018/8/6
 */
public  class MyTopology {
	public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
		TopologyBuilder topo = new TopologyBuilder();
		topo.setSpout("MySpolt", new MySpout());
		topo.setBolt("MyBolt", new MyBolt()).shuffleGrouping("MySpolt");

		/*提交到本地*/
		LocalCluster localCluster = new LocalCluster();
		Config conf = new Config();
		localCluster.submitTopology("mytopo", conf, topo.createTopology());
		
		/*提交到集群*/
		/*StormSubmitter.submitTopology("mytopo",new Config(),topo.createTopology());*/
	}
}
