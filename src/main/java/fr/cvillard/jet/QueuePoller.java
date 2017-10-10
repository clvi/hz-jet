package fr.cvillard.jet;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.IQueue;
import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.util.concurrent.TimeUnit;

/**
 * Processor that polls a HazelCast IQueue and emit items for further processing
 */
public class QueuePoller extends AbstractProcessor {

	/**
	 * Hazelcast logger
	 */
	private ILogger logger;

	/**
	 * the name of the input queue
	 */
	private String inputQueueName;

	/**
	 * The name of the stop flag
	 */
	private String stopFlagName;

	/**
	 * The queue to poll
	 */
	private IQueue<Metric> inputQueue;

	/**
	 * This flag will be set to true if the pollers must stop their execution after next element polling
	 */
	private IAtomicReference<Boolean> stopFlag;

	@Override
	protected void init(@Nonnull Context context) throws Exception {
		HazelcastInstance hzInstance = context.jetInstance().getHazelcastInstance();
		inputQueue = hzInstance.getQueue(inputQueueName);
		stopFlag = hzInstance.getAtomicReference(stopFlagName);
		logger = hzInstance.getLoggingService().getLogger(QueuePoller.class);
	}

	public QueuePoller(String inputQueueName, String stopFlagName) {
		this.inputQueueName = inputQueueName;
		this.stopFlagName = stopFlagName;
		// poller can block threads so it must not be cooperative to do not block other threads doing other tasks
		setCooperative(false);
	}

	@Override
	public boolean complete() {
		try {
			// Use poll() method with timeout to check periodically for stop flag
			Metric metric = inputQueue.poll(20, TimeUnit.MILLISECONDS);
			if(metric != null) { // metric is null if poll timeout expired
				while (!tryEmit(metric)) {
					Thread.sleep(100);
				}
			}
			return shouldComplete();
		} catch (InterruptedException ignored) {
			return shouldComplete();
		} catch (Exception e) {
			return true;
		}
	}

	/**
	 * Check if the stop flag is true and if the poller should stop its polling
	 * @return the value of the stop flag
	 */
	private boolean shouldComplete() {
		if(stopFlag.get()) {
			logger.info("Stop flag is true, polling stopped.");
			return true;
		} else {
			return false;
		}
	}
}
