package stormTest;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * @ author: create by LuJuHui
 * @ date:2018/8/6
 */
public class MySpout extends BaseRichSpout {
	//Map map;
	//TopologyContext context;
	SpoutOutputCollector collect;

	/*代码最开始运行时，进行初始化*/
	@Override
	public void open(Map map, TopologyContext context, SpoutOutputCollector collect) {
		//this.map = map;
		//this.context = context;
		this.collect = collect;

	}

	/*这个地方比较重要，其实就是一个死循环*/
	int i = 0;

	@Override
	public void nextTuple() {
		/*获取一个发射器，往下一层发射数据 spou -> bolt*/
		collect.emit(new Values(i++));
		Utils.sleep(1000);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("number"));
	}
}

