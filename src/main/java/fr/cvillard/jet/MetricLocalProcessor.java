package fr.cvillard.jet;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

import java.util.concurrent.Callable;

import static fr.cvillard.jet.MetricIMDG.CUSTOMER_MAP_NAME;
import static fr.cvillard.jet.MetricIMDG.ERROR_OUTPUT_MAP_NAME;
import static fr.cvillard.jet.MetricIMDG.OUTPUT_MAP_NAME;
import static fr.cvillard.jet.MetricIMDG.SOURCE_MAP_NAME;

public class MetricLocalProcessor implements Runnable {

	private HazelcastInstance hzInstance;
	private String key;

	public MetricLocalProcessor(HazelcastInstance hzInstance, String key) {
		this.hzInstance = hzInstance;
		this.key = key;
	}

	@Override
	public void run() {
		IMap<Integer, Customer> customerMap = hzInstance.getMap(CUSTOMER_MAP_NAME);
		IMap<String, Metric> metricMap = hzInstance.getMap(SOURCE_MAP_NAME);
		IMap<Long, EnrichedMetric> outputMap = hzInstance.getMap(OUTPUT_MAP_NAME);
		IMap<Long, Metric> errorMap = hzInstance.getMap(ERROR_OUTPUT_MAP_NAME);

		Metric metric = metricMap.get(key);
		Customer customer = customerMap.get(metric.getCustomerId());
		if (customer != null) {
			outputMap.set(metric.getTimestampMs(), new EnrichedMetric(metric, customer));
		} else {
			errorMap.set(metric.getTimestampMs(), metric);
		};
	}
}
