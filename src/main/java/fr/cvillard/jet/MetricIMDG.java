package fr.cvillard.jet;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

public class MetricIMDG {


	/**
	 * Number of generated metrics on input
	 */
	private static final int NB_ITEMS = 1_000_000;

	/**
	 * Number of Customer known in the Customer's map
	 */
	private static final int NB_CUSTOMERS = 20;

	/**
	 * The name of the input map, storing the raw Metrics
	 */
	static final String SOURCE_MAP_NAME = "sourceMap";

	/**
	 * The name of the output map, storing the enriched metrics
	 */
	static final String OUTPUT_MAP_NAME = "outputMap";

	/**
	 * The name of the error output map, storing the metrics that could not be processed
	 */
	static final String ERROR_OUTPUT_MAP_NAME = "errorOutputMap";

	/**
	 * The name of the map storing the customers
	 */
	static final String CUSTOMER_MAP_NAME = "customers";

	/**
	 * Launch Jet instance, populate the maps, launch the batch Job to process metrics and check output
	 *
	 * @param args unused
	 * @throws ExecutionException   if an error occur during Jet job execution
	 * @throws InterruptedException if Jet job or wait on counter of processed elements got interrupted
	 */
	public static void main(String[] args) throws ExecutionException, InterruptedException {

		// launch HZ with default configuration
		HazelcastInstance hzInstance = Hazelcast.newHazelcastInstance();

		// stop instances on termination
		Runtime.getRuntime().addShutdownHook(new Thread(Hazelcast::shutdownAll));

		// Create an additional instance; it will automatically discover the first one and form a cluster
		Hazelcast.newHazelcastInstance();

		// get Logger from Hazelcast for simplicity purpose
		ILogger logger = hzInstance.getLoggingService().getLogger(MetricIMDG.class);

		// preload map of customer for enrichment
		IMap<Integer, Customer> customerMap = hzInstance.getMap(CUSTOMER_MAP_NAME);
		for (int i = 0; i < NB_CUSTOMERS; i++) {
			customerMap.put(i, new Customer(i, "Customer " + i));
		}

		// preload map of metrics
		IMap<String, Metric> inputMap = hzInstance.getMap(SOURCE_MAP_NAME);
		ThreadLocalRandom rnd = ThreadLocalRandom.current();
		// initialize timestamp to now - 5s
		final long startTimeMs = System.currentTimeMillis() - 5 * 1000;
		// parallel map loading to fully use CPU
		int nbCPUs = Runtime.getRuntime().availableProcessors();
		ExecutorService service = Executors.newFixedThreadPool(nbCPUs * 2);
		List<Future> futures = new ArrayList<>(10);
		// submit tasks of 10.000 items each
		int maxIdx = -1;
		while (maxIdx < NB_ITEMS) {
			int startIdx = maxIdx + 1;
			// intermediate variable localMaxIdx is necessary to have effectively final variable in lambda expression
			int localMaxIdx = startIdx + Math.min(100_000, NB_ITEMS);
			maxIdx = localMaxIdx;
			futures.add(service.submit(() -> {
				for (int i = startIdx; i < localMaxIdx; i++) {
					// add 1 to avoid nbCalls to be equal to 0, which is not supported as bound for rnd.nextInt below
					final int nbCalls = rnd.nextInt(10) + 1;
					final long metricTimeMs = startTimeMs - i; // ensure there is no collision since timestamp is the key on the input map
					// we voluntarily generate some metrics with non existing customers to see error output in action
					inputMap.set(Long.toString(metricTimeMs), new Metric(metricTimeMs, rnd.nextInt(NB_CUSTOMERS + 5),
							nbCalls, rnd.nextInt(nbCalls)));
				}
			}));
		}

		for (Future fut : futures) {
			fut.get(); // wait for init completion
		}

		logger.info("Initial loading done, will start Batch Job...");

		// create a distributed executor
		IExecutorService executor = hzInstance.getExecutorService("enricherThreadPool");

		long processingStartTime = System.currentTimeMillis();

		// submit the task to all members
		Map<Member, Future<Void>> pendingResults = executor.submitToAllMembers(new MetricEnrichmentIMDGProcessor());

		// wait for completion
		for (Future<Void> pendingResult : pendingResults.values()) {
			pendingResult.get();
		}

		long endTimeMs = System.currentTimeMillis();

		// get output maps size
		final int normalCount = hzInstance.getMap(OUTPUT_MAP_NAME).size();
		final int errorCount = hzInstance.getMap(ERROR_OUTPUT_MAP_NAME).size();

		logger.info("Received items (normal / error / total): " + normalCount + " / " + errorCount + " / " + (normalCount + errorCount));

		logger.info("Processing took " + (endTimeMs - processingStartTime) + " ms.");

		// Shutdown Jet instances
		Hazelcast.shutdownAll();

		// exit cleanly
		System.exit(0);
	}
}
