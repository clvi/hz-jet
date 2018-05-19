package fr.cvillard.jet;

import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.IQueue;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.logging.ILogger;

import java.nio.channels.Pipe;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

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
	 * The sliding window length in milliseconds
	 */
	private static final long SLIDING_WINDOW_LENGTH_MS = 1000;

	/**
	 * The sliding window step length in milliseconds
	 */
	private static final long SLIDING_WINDOW_STEP_MS = 10;

	/**
	 * The lag between each watermark and the highest observed timestamp, in milliseconds
	 */
	private static final long WATERMARK_LAG_MS = 100;

	/**
	 * The minimal difference between 2 watermarks
	 */
	private static final long WATERMARK_MIN_STEP = 100;

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
		StreamStage<Metric> enrichedStage = pipeline
				// read input queue as stream
				.<Metric>drawFrom(Sources.streamFromProcessor("source",
						ProcessorMetaSupplier.of(() -> new QueuePoller(SOURCE_QUEUE_NAME, STOP_FLAG_NAME), 1)));
//
// 		Vertex watermark = dag.newVertex("wm", Processors.insertWatermarks(Metric::getTimestampMs,
//				// The watermark is always the given lag (in ms) behind the soonest observed timestamp
//				WatermarkPolicies.withFixedLag(WATERMARK_LAG_MS),
//				// Ensure each new watermark has at least the given difference with the previous one
//				WatermarkEmissionPolicy.emitByMinStep(WATERMARK_MIN_STEP)));
//
//		AggregateOperation<Metric, DropCountAccumulator, WindowValue> op = AggregateOperation.of(
//				DropCountAccumulator::new,
//				(a, item) -> a.add(item.getNumberOfCalls(), item.getNumberOfErrors()),
//				DropCountAccumulator::combine,
//				DropCountAccumulator::deduct,
//				DropCountAccumulator::total
//		);
//		// produces TimestampedEntry<Integer, WindowValue>
//		Vertex aggregate = dag.newVertex("aggregator",
//				Processors.aggregateToSlidingWindow(
//						Metric::getCustomerId,
//						Metric::getTimestampMs,
//						TimestampKind.EVENT,
//						WindowDefinition.slidingWindowDef(SLIDING_WINDOW_LENGTH_MS, SLIDING_WINDOW_STEP_MS),
//						op
//				));
//
//		Vertex map = dag.newVertex("mapper",
//				Processors.map((TimestampedEntry<Integer, Double> te) ->
//								new AbstractMap.SimpleEntry<>(new WindowKey(te), te.getValue())));
//
//		// The peekInput logs all messages coming into the output vertex
//		Vertex output = dag.newVertex("output", DiagnosticProcessors.peekInput(Sinks.writeMap(OUTPUT_MAP_NAME)));
//
//		dag
//				.edge(Edge.between(source, watermark).isolated())
//				.edge(Edge.between(watermark, aggregate).partitioned(Metric::getCustomerId, Partitioner.HASH_CODE).distributed())
//				.edge(Edge.between(aggregate, map).isolated())
//				.edge(Edge.between(map, output));
//
//		// execute the graph and DO NOT wait for completion
//		Future<Void> job = jet.newJob(dag).execute();
//
//		IStreamMap<WindowKey, WindowValue> outputMap = jet.getMap(OUTPUT_MAP_NAME);

//		// wait for all input items to be consumed
//		while (inputQueue.size() > 0) {
//			Thread.sleep(5000);
//			logger.info("Received items : " + outputMap.size());
//		}
//
//		stopFlag.set(true);
//
//		logger.info("Stop flag set. Waiting for job completion.");
//
//		// wait for job completion
//		try {
//			job.get(1, TimeUnit.MINUTES);
//		} catch (TimeoutException te) {
//			logger.warning("Job failed to complete in timeout, cancelling job.");
//			job.cancel(true);
//		}

		logger.info("Job complete. Please observe IMap '" + OUTPUT_MAP_NAME + "' for sliding window results.");

		logger.info("Terminating Jet instances.");

		// Shutdown Jet instances
		Jet.shutdownAll();

		System.exit(0);
	}

}
