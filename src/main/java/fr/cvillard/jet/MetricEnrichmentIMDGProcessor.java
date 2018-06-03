package fr.cvillard.jet;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;

import java.io.Serializable;
import java.util.concurrent.Callable;

import static fr.cvillard.jet.MetricIMDG.CUSTOMER_MAP_NAME;
import static fr.cvillard.jet.MetricIMDG.ERROR_OUTPUT_MAP_NAME;
import static fr.cvillard.jet.MetricIMDG.OUTPUT_MAP_NAME;
import static fr.cvillard.jet.MetricIMDG.SOURCE_MAP_NAME;

/**
 * Enrich local metrics with customers and put them to output maps
 */
public class MetricEnrichmentIMDGProcessor implements Callable<Void>, HazelcastInstanceAware, Serializable {

	private HazelcastInstance hzInstance;

	@Override
	public Void call() {
		IMap<Integer, Customer> customerMap = hzInstance.getMap(CUSTOMER_MAP_NAME);
		IMap<String, Metric> metricMap = hzInstance.getMap(SOURCE_MAP_NAME);
		IMap<Long, EnrichedMetric> outputMap = hzInstance.getMap(OUTPUT_MAP_NAME);
		IMap<Long, Metric> errorMap = hzInstance.getMap(ERROR_OUTPUT_MAP_NAME);

		for(String metricKey : metricMap.localKeySet()) {
			Metric metric = metricMap.get(metricKey);
			Customer customer = customerMap.get(metric.getCustomerId());
			if(customer != null) {
				outputMap.set(metric.getTimestampMs(), new EnrichedMetric(metric, customer));
			} else {
				errorMap.set(metric.getTimestampMs(), metric);
			}
		}
		// we use a callable which return null to be able to check when the processing has ended
		return null;
	}

	@Override
	public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
		this.hzInstance = hazelcastInstance;
	}
}
