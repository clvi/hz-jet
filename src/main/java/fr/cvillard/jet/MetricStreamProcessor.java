package fr.cvillard.jet;

import com.hazelcast.core.EntryView;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Edge;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.processor.Processors;
import com.hazelcast.jet.processor.Sinks;
import com.hazelcast.jet.stream.IStreamMap;
import com.hazelcast.logging.ILogger;

import java.util.AbstractMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Perform simple Customer enrichment on a stream of Metrics.
 */
public class MetricStreamProcessor {
	/**
	 * Number of generated metrics on input
	 */
	private static final int NB_ITEMS = 100_000;

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
	 * The name of the error output map, storing the metrics that could not be processed
	 */
	private static final String ERROR_OUTPUT_MAP_NAME = "errorOutputMap";

	/**
	 * The name of the map storing the customers
	 */
	private static final String CUSTOMER_MAP_NAME = "customers";

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
		ILogger logger = jet.getHazelcastInstance().getLoggingService().getLogger(MetricStreamProcessor.class);

		// preload map of customer for enrichment
		IMap<Integer, Customer> customerMap = jet.getHazelcastInstance().getMap(CUSTOMER_MAP_NAME);
		for (int i = 0; i < NB_CUSTOMERS; i++) {
			customerMap.put(i, new Customer(i, "Customer " + i));
		}

		// prepare stop flag
		IAtomicReference<Boolean> stopFlag = jet.getHazelcastInstance().getAtomicReference(STOP_FLAG_NAME);
		stopFlag.set(false);

		// preload map of metrics
		ThreadLocalRandom rnd = ThreadLocalRandom.current();

		IQueue<Metric> inputQueue = jet.getHazelcastInstance().getQueue(SOURCE_QUEUE_NAME);
		// Use AtomicLong to be final
		final AtomicLong generated = new AtomicLong(0);
		ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
		service.scheduleWithFixedDelay(() -> {
			// add 1 to avoid nbCalls to be equal to 0, which is not supported as bound for rnd.nextInt below
			final int nbCalls = rnd.nextInt(10) + 1;
			// we voluntarily generate some metrics with non existing customers to see error output in action
			inputQueue.offer(new Metric(System.currentTimeMillis(), rnd.nextInt(NB_CUSTOMERS + 5), nbCalls, rnd.nextInt(nbCalls)));
			long gen = generated.incrementAndGet();
			if (gen % 100 == 0) {
				logger.info(gen + " items generated.");
			}
		}, 1000, 1, TimeUnit.MILLISECONDS);

		logger.info("Input generator started");

		// treatment dag
		DAG dag = new DAG();
		// read input queue as stream
		Vertex source = dag.newVertex("source", () -> new QueuePoller(SOURCE_QUEUE_NAME, STOP_FLAG_NAME));
		// enrich metric with Customer and transform to EnrichedMetric
		Vertex enricher = dag.newVertex("enricher", () -> new MetricEnricher(CUSTOMER_MAP_NAME));
		// transform EnrichedMetric to Map entry
		Vertex outputMapper = dag.newVertex("outputMapper",
				Processors.map((EnrichedMetric m) -> new AbstractMap.SimpleEntry<>(m.getMetric().getTimestampMs(), m)));
		// output to map
		Vertex output = dag.newVertex("output", Sinks.writeMap(OUTPUT_MAP_NAME));
		// output errors to another map
		Vertex errorOutput = dag.newVertex("errorOutput", Sinks.writeMap(ERROR_OUTPUT_MAP_NAME));

		dag
				.edge(Edge.between(source, enricher))
				// we need to control the ordinal of the output here to send items to the right output
				.edge(Edge.from(enricher, MetricEnricher.NORMAL_OUTPUT_ORDINAL).to(outputMapper))
				.edge(Edge.between(outputMapper, output))
				// the enricher will dispatch items between normal and error output
				.edge(Edge.from(enricher, MetricEnricher.ERROR_OUTPUT_ORDINAL).to(errorOutput));

		// execute the graph and DO NOT wait for completion
		Future<Void> job = jet.newJob(dag).execute();

		IStreamMap<Long, EnrichedMetric> outputMap = jet.getMap(OUTPUT_MAP_NAME);
		IStreamMap errorMap = jet.getMap(ERROR_OUTPUT_MAP_NAME);

		int receivedItems = 0;
		while (receivedItems < NB_ITEMS) {
			Thread.sleep(3000); // log each 3 seconds
			// get output maps size
			final int normalCount = outputMap.size();
			final int errorCount = errorMap.size();

			receivedItems = normalCount + errorCount;

			logger.info("Received items (normal / error / total): " + normalCount + " / " + errorCount + " / " + receivedItems);
		}

		logger.info(NB_ITEMS + " received, stopping metric generator.");

		// shutdown input generator
		service.shutdown();
		service.awaitTermination(1, TimeUnit.MINUTES);

		logger.info("Metric generator stopped. Setting stop flag to end pollers.");

		// set distributed flag to stop queue pollers
		stopFlag.set(true);

		logger.info("Stop flag set. Waiting for job completion.");

		// wait for job completion
		try {
			job.get(1, TimeUnit.MINUTES);
		} catch (TimeoutException te) {
			logger.warning("Job failed to complete in timeout, cancelling job.");
			job.cancel(true);
		}

		logger.info("Job complete. Final statistics:");

		// compute average latency
		long totalLatencyMs = 0;
		long minLatency = Long.MAX_VALUE;
		long maxLatency = 0;
		for (Long key : outputMap.keySet()) {
			EntryView<Long, EnrichedMetric> mapEntry = outputMap.getEntryView(key);
			long latency = mapEntry.getCreationTime() - mapEntry.getValue().getMetric().getTimestampMs();

			minLatency = Math.min(minLatency, latency);
			maxLatency = Math.max(maxLatency, latency);

			totalLatencyMs += latency;
		}

		// get output maps size
		final int normalCount = outputMap.size();
		final int errorCount = errorMap.size();

		receivedItems = normalCount + errorCount;
		long avgLatency = totalLatencyMs / normalCount;

		logger.info("Received items (normal / error / total): " + normalCount + " / " + errorCount + " / " + receivedItems);
		logger.info("Latency (ms, min / avg / max): " + minLatency + " / " + avgLatency + " / " + maxLatency);

		logger.info("Terminating Jet instances.");

		// Shutdown Jet instances
		Jet.shutdownAll();

		System.exit(0);
	}
}
