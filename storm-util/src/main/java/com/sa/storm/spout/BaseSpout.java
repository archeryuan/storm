package com.sa.storm.spout;

import java.util.Date;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import com.sa.storm.domain.tuple.BaseTuple;
import com.sa.storm.domain.tuple.TaskResult;

public abstract class BaseSpout implements IRichSpout {

	private static final long serialVersionUID = 1L;

	private SpoutOutputCollector collector;

	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	abstract public void close();

	abstract public void nextTuple();

	abstract public void ack(Object msgId);

	abstract public void fail(Object msgId);

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(TaskResult.class.getName(), new Fields(TaskResult.class.getName()));
		declareOutputs(declarer);
	}

	/**
	 * Called in declareOutputFields() to declare output fields and streams information
	 * 
	 * @see com.sa.storm.bolt.BaseBolt.declareOutputFields
	 * @param declarer
	 */
	abstract public void declareOutputs(OutputFieldsDeclarer declarer);

	public void activate() {

	}

	public void deactivate() {

	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	/**
	 * Emit output into the stream named after the class name of the object with messageId specified.<BR>
	 * Spout's ack() or fail() will be called if messageId is not null when the emitted tuple is fully processed.
	 * 
	 * @param output
	 * @param remarks
	 * @param messageId
	 */
	protected void emit(BaseTuple output, String remarks, String messageId) {
		addHistory(output, remarks);
		getCollector().emit(output.getClass().getName(), new Values(output), messageId);
	}

	protected void emit(TaskResult result, String messageId) {
		emit(result, result.toString(), messageId);
	}

	/**
	 * Declare the output field and stream by the object class.
	 * 
	 * @see org.apache.storm.topology.OutputFieldsDeclarer.declareStream
	 * @param declarer
	 * @param clazz
	 */
	protected void declareFieldByClass(OutputFieldsDeclarer declarer, Class<?> clazz) {
		declarer.declareStream(clazz.getName(), new Fields(clazz.getName()));
	}

	protected SpoutOutputCollector getCollector() {
		return collector;
	}

	/**
	 * @param collector
	 *            the collector to set
	 */
	protected void setCollector(SpoutOutputCollector collector) {
		this.collector = collector;
	}

	/**
	 * Add history to the tuple
	 * 
	 * @param tuple
	 * @param remarks
	 */
	protected void addHistory(BaseTuple tuple, String remarks) {
		if (remarks != null && !remarks.isEmpty()) {
			tuple.addHistory(new Date() + " - " + getClass().getName() + " - " + remarks);
		}
	}
}
