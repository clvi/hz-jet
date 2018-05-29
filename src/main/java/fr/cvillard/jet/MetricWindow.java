package fr.cvillard.jet;

import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.IQueue;
import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.logging.ILogger;

import java.util.AbstractMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Perform simple Customer enrichment on a stream of Metrics.
 */
public class MetricWindow {
	/**
	 * Number of generated metrics on input
	 */
	private static final int NB_ITEMS = 10_000;

	/**
	 * Number of Customer known in the Customer's map
	 */
	private static final int NB_CUSTOMERS = 20;

	/**
	 * The name of the input map, storing the raw Metrics
	 */
	private static final String SOURCE_QUEUE_NAME = "sourceQueue";

	/**
	 * The name of the output map, storing the enriched metrics
	 */
	private static final String OUTPUT_MAP_NAME = "outputMap";

	/**
	 * The name of the flag used to cleanly stop queue pollers
	 */
	private static final String STOP_FLAG_NAME = "stopFlag";
	/**
	 * Launch Jet instance, populate the maps, launch the batch Job to process metrics and check output
	 *
	 * @param args unused
	 * @throws ExecutionException   if an error occur during Jet job execution
	 * @throws InterruptedException if Jet job or wait on counter of processed elements got interrupted
	 */
	public static void main(String[] args) throws ExecutionException, InterruptedException {

		// launch Jet with default configuration
		JetInstance jet = Jet.newJetInstance();

		// stop jet on termination
		Runtime.getRuntime().addShutdownHook(new Thread(Jet::shutdownAll));

		// Create an additional instance; it will automatically discover the first one and form a cluster
		Jet.newJetInstance();

		// get Logger from Hazelcast for simplicity purpose
		ILogger logger = jet.getHazelcastInstance().getLoggingService().getLogger(MetricWindow.class);

		// prepare stop flag
		IAtomicReference<Boolean> stopFlag = jet.getHazelcastInstance().getAtomicReference(STOP_FLAG_NAME);
		stopFlag.set(false);

		// preload map of metrics
		ThreadLocalRandom rnd = ThreadLocalRandom.current();

		IQueue<Metric> inputQueue = jet.getHazelcastInstance().getQueue(SOURCE_QUEUE_NAME);
		ExecutorService inputService = Executors.newSingleThreadExecutor();
		inputService.submit(() -> {
			int generated = 0;
			while (generated < NB_ITEMS) {
				try {
					Thread.sleep(1);
				} catch (InterruptedException ignored) {
				}
				// add 1 to avoid nbCalls to be equal to 0, which is not supported as bound for rnd.nextInt below
				final int nbCalls = rnd.nextInt(10) + 1;
				inputQueue.offer(new Metric(System.currentTimeMillis(), rnd.nextInt(NB_CUSTOMERS), nbCalls, rnd.nextInt(nbCalls)));
				generated++;
				if (generated % 100 == 0) {
					logger.info(generated + " items generated.");
				}
			}
		});

		logger.info("Input generator started");

		// treatment pipeline
		Pipeline pipeline = Pipeline.create();
		// read input queue as stream
		pipeline
				// read input queue as stream
				.<Metric>drawFrom(Sources.streamFromProcessor("source",
						ProcessorMetaSupplier.of(() -> new QueuePoller(SOURCE_QUEUE_NAME, STOP_FLAG_NAME), 1)))
				// add timestamps to events. Auto-generate watermarks
				.addTimestamps()
				// define the sliding window length and refresh rate
				.window(WindowDefinition.sliding(TimeUnit.SECONDS.toMillis(1), TimeUnit.MILLISECONDS.toMillis(10)))
				// group event per customer
				.groupingKey(Metric::getCustomerId)
				// define a custom aggregator to count success and failure of calls
				.aggregate(AggregateOperation
						.withCreate(DropCountAccumulator::new)
						.andAccumulate((DropCountAccumulator a, Metric item) -> a.add(item.getNumberOfCalls(), item.getNumberOfErrors()))
						.andCombine(DropCountAccumulator::combine)
						.andDeduct(DropCountAccumulator::deduct)
						.andFinish(DropCountAccumulator::total)
				)
				// transform the result to write it to a map
				.map((windowValue) -> new AbstractMap.SimpleEntry<>(
						new WindowKey(windowValue.getTimestamp(), windowValue.getKey()), windowValue.getValue()))
				// output to the map
				.drainTo(Sinks.map(OUTPUT_MAP_NAME));

		// execute the graph and DO NOT wait for completion
		Future<Void> job = jet.newJob(pipeline).getFuture();

		IMapJet<WindowKey, WindowValue> outputMap = jet.getMap(OUTPUT_MAP_NAME);

		// wait for all input items to be consumed
		while (inputQueue.size() > 0) {
			Thread.sleep(5000);
			logger.info("Received items : " + outputMap.size());
		}

		stopFlag.set(true);

		logger.info("Stop flag set. Waiting for job completion.");

		// wait for job completion
		try {
			job.get(1, TimeUnit.MINUTES);
		} catch (TimeoutException te) {
			logger.warning("Job failed to complete in timeout, cancelling job.");
			job.cancel(true);
		}

		logger.info("Job complete. Please observe IMap '" + OUTPUT_MAP_NAME + "' for sliding window results.");

		outputMap.forEach((key, value) -> System.out.println(key + " : " + value));

		logger.info("Terminating Jet instances.");

		// Shutdown Jet instances
		Jet.shutdownAll();

		System.exit(0);
	}

}
