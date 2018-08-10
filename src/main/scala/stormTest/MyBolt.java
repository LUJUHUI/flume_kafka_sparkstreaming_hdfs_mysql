package stormTest;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * @ author: create by LuJuHui
 * @ date:2018/8/6
 */
public class MyBolt extends BaseRichBolt {

	/*这是bolt的初始化代码，只执行一次*/
	@Override
	public void prepare(Map map, TopologyContext context, OutputCollector outputCollector) {

	}

	/*核心逻辑代码，也是一个死循环*/
	int sum = 0;

	@Override
	public void execute(Tuple input) {
		Integer num = input.getIntegerByField("number");
		System.out.println(sum += num);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

	}
}
