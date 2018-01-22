package com.sa.storm.bolt;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import com.sa.storm.domain.tuple.BaseTuple;
import com.sa.storm.domain.tuple.TaskResult;
import com.sa.storm.framework.App;

/**
 * Basic bolt that accept an input and ack the anchor after normal processing. The stream and fields name are named by convention using
 * class name of the input/output.
 * 
 * @author Kelvin Wong
 * 
 */
public abstract class BaseBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;

	private static Logger log = LoggerFactory.getLogger(BaseBolt.class);

	private TopologyContext topologyContext;

	private transient ApplicationContext springContext;

	private OutputCollector collector;

	private boolean isAutoAck;

	private boolean isAutoFail;

	public BaseBolt() {
		this.isAutoAck = true;
		this.isAutoFail = true;
	}

	public BaseBolt(boolean isAutoAck, boolean isAutoFail) {
		this.isAutoAck = isAutoAck;
		this.isAutoFail = isAutoFail;
	}

	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		this.topologyContext = context;
		this.collector = collector;
	}

	public void execute(Tuple input) {
		try {
			process(input);

			if (isAutoAck) {
				log.info("Ack the input automatically after process sucessful, input: {}", input);
				getCollector().ack(input);
			}
		} catch (Exception e1) {
			try {
				log.error("Error in process, will handle the error, input: " + input, e1);
				onError(input, e1);
				if (isAutoAck) {
					log.error("Ack the input automatically after handling error, input: {}", input);
					getCollector().ack(input);
				}
			} catch (Exception e2) {
				log.error("Error cannot be handled, input: " + input, e2);
				if (isAutoFail) {
					log.info("Fail the input automatically, {}", input);
					getCollector().fail(input);
				}
			}
		}
	}

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

	/**
	 * Called in execute() to implement the logics of this bolt. At the end the anchor is acknowledged if there are no exception thrown. All
	 * thrown exception will be handled in onError()
	 * 
	 * @see com.sa.storm.bolt.BaseBolt.execute
	 * @see com.sa.storm.bolt.BaseBolt.onError
	 * @param input
	 * @throws Exception
	 */
	abstract public void process(Tuple input) throws Exception;

	/**
	 * Called in execute() when there are exception thrown in process(). If there are exception thrown within this method, the fail() method
	 * will be called and Storm will handle the retries
	 * 
	 * Only non-recoverable exception (e.g. database connection error, connection issues) should be thrown, all other business logic and
	 * implementation error should be handled within this method
	 * 
	 * @see com.sa.storm.bolt.BaseBolt.execute
	 * @see com.sa.storm.bolt.BaseBolt.process
	 * @param input
	 * @param e
	 * @throws Exception
	 */
	abstract public void onError(Tuple input, Exception e) throws Exception;

	protected TopologyContext getTopologyContext() {
		return this.topologyContext;
	}

	/**
	 * Obtain the Spring framework context. The context is lazy initialized if it does not exists yet
	 * 
	 * @return
	 */
	protected ApplicationContext getSpringContext() {
		if (springContext == null) {
			springContext = App.getInstance().getContext();
		}

		return springContext;
	}

	/**
	 * Obtain the Storm collector object
	 * 
	 * @return
	 */
	protected OutputCollector getCollector() {
		return this.collector;
	}

	/**
	 * Emit the output into stream, with optional output
	 * 
	 * @param stream
	 * @param input
	 * @param output
	 * @param optionalOutputs
	 * @param remarks
	 */
	protected void emit(String stream, Tuple input, BaseTuple output, List<Object> optionalOutputs, String remarks) {
		addHistory(output, remarks);
		getCollector().emit(stream, input, buildValues(output, optionalOutputs));
	}

	/**
	 * Emit the output into the stream named by output class name
	 * 
	 * @see org.apache.storm.task.OutputCollector.emit
	 * @param input
	 * @param output
	 * @param remarks
	 */
	protected void emit(Tuple input, BaseTuple output, String remarks) {
		emit(output.getClass().getName(), input, output, null, remarks);
	}
	
	protected void emitWithoutAnchor(BaseTuple output, String remarks) {
		getCollector().emit(output.getClass().getName(), buildValues(output, null));
	}

	/**
	 * Emit the output into the stream named by output class name, with optional outputs
	 * 
	 * @param input
	 * @param output
	 * @param optionalOutput
	 * @param remarks
	 */
	protected void emit(Tuple input, BaseTuple output, List<Object> optionalOutput, String remarks) {
		emit(output.getClass().getName(), input, output, optionalOutput, remarks);
	}

	/**
	 * Emit the result
	 * 
	 * @param input
	 * @param result
	 */
	protected void emit(Tuple input, TaskResult result) {
		emit(input, result, result.toString());
	}

	/**
	 * Emit the output into stream
	 * 
	 * @param stream
	 * @param inputs
	 * @param output
	 * @param optionalOutputs
	 * @param remarks
	 */
	protected void emit(String stream, List<Tuple> inputs, BaseTuple output, List<Object> optionalOutputs, String remarks) {
		addHistory(output, remarks);
		getCollector().emit(stream, inputs, buildValues(output, optionalOutputs));
	}

	/**
	 * Emit the output into the stream named after the class name of the object. Accept multiple anchors.
	 * 
	 * @param inputs
	 * @param output
	 * @param remarks
	 */
	protected void emit(List<Tuple> inputs, BaseTuple output, String remarks) {
		emit(output.getClass().getName(), inputs, output, null, remarks);
	}

	/**
	 * Declare the output field and stream, by default named with object class
	 * 
	 * @see org.apache.storm.topology.OutputFieldsDeclarer.declareStream
	 * @param declarer
	 * @param clazz
	 */
	protected void declareOutputByClass(OutputFieldsDeclarer declarer, Class<?> clazz) {
		declareOutputByClass(declarer, clazz.getName(), clazz, null);
	}

	/**
	 * Declare the output field and stream, by default named with object class, with optional fields
	 * 
	 * @param declarer
	 * @param clazz
	 * @param optionalFields
	 */
	protected void declareOutputByClass(OutputFieldsDeclarer declarer, Class<?> clazz, List<String> optionalFields) {
		declareOutputByClass(declarer, clazz.getName(), clazz, optionalFields);
	}

	/**
	 * Declare the output field and stream, with optional fields
	 * 
	 * @param declarer
	 * @param stream
	 * @param clazz
	 * @param optionalFields
	 */
	protected void declareOutputByClass(OutputFieldsDeclarer declarer, String stream, Class<?> clazz, List<String> optionalFields) {

		List<String> fieldNames = new ArrayList<String>();
		fieldNames.add(clazz.getName());

		if (optionalFields != null && !optionalFields.isEmpty()) {
			fieldNames.addAll(optionalFields);
		}

		declarer.declareStream(stream, new Fields(fieldNames));
	}

	/**
	 * Check if this bolt allow auto ack after normal process
	 * 
	 * @return
	 */
	protected boolean isAutoAck() {
		return this.isAutoAck;
	}

	/**
	 * Check if this bolt allow auto fail when onError cannot handle errors
	 * 
	 * @return
	 */
	protected boolean isAutoFail() {
		return this.isAutoFail;
	}

	/**
	 * Get input from the field named after the class name
	 * 
	 * @see org.apache.storm.tuple.Tuple.getValueByField
	 * @param input
	 * @param clazz
	 * @return
	 */
	protected BaseTuple getInputByClass(Tuple input, Class<?> clazz) {
		return (BaseTuple) input.getValueByField(clazz.getName());
	}

	/**
	 * Add history to the tuple
	 * 
	 * @param tuple
	 * @param remarks
	 */
	protected void addHistory(BaseTuple tuple, String remarks) {
		if (remarks != null && !remarks.isEmpty()) {
			final String history = new Date() + " - " + getTopologyContext().getThisTaskId() + " - " + getClass().getName() + " - "
					+ remarks;
			tuple.addHistory(history);
			log.info("Added history {}", history);
		}
	}

	/**
	 * Get the task ID for current task within the global Storm clusters
	 * 
	 * @param context
	 * @return
	 */
	protected String getGlobalTaskId() {
		return new StringBuffer(getTopologyContext().getStormId()).append("_").append(getLocalTaskId()).toString();
	}

	/**
	 * Get the task ID for current task within the local topology
	 * 
	 * @return
	 */
	protected int getLocalTaskId() {
		return getTopologyContext().getThisTaskId();
	}

	/**
	 * Construct Storm Values object
	 * 
	 * @param output
	 * @param optionalOutputs
	 * @return
	 */
	protected Values buildValues(BaseTuple output, List<Object> optionalOutputs) {
		Values values = new Values(output);

		if (optionalOutputs != null && !optionalOutputs.isEmpty()) {
			for (Object extraValue : optionalOutputs) {
				values.add(extraValue);
			}
		}

		return values;
	}

	/**
	 * Check if the tuple is a tick tuple that emit with specified frequency
	 * 
	 * 
	 * @param tuple
	 * @return
	 */
	protected static boolean isTickTuple(Tuple tuple) {
		return tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
	}
}
