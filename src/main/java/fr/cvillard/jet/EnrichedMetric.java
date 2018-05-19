package fr.cvillard.jet;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * Represent a Metric enriched with its Customer full data
 */
public class EnrichedMetric implements DataSerializable {

	private Metric metric;

	private Customer customer;

	public EnrichedMetric(Metric metric, Customer customer) {
		this.metric = metric;
		this.customer = customer;
	}

	public EnrichedMetric() {
	}

	/**
	 * Check if the metric has been correctly enriched (ie. the customer is present)
	 * @return true if the customer is present, false otherwise
	 */
	public boolean isEnriched() {
		return customer != null;
	}

	/**
	 * @return customer
	 */
	public Customer getCustomer() {
		return customer;
	}

	/**
	 * @param customer Value of customer
	 */
	public void setCustomer(Customer customer) {
		this.customer = customer;
	}

	/**
	 * @return metric
	 */
	public Metric getMetric() {
		return metric;
	}

	/**
	 * @param metric Value of metric
	 */
	public void setMetric(Metric metric) {
		this.metric = metric;
	}

	@Override
	public void writeData(ObjectDataOutput out) throws IOException {
		if(metric != null) {
			metric.writeData(out);
		}
		if (customer != null) {
			customer.writeData(out);
		}
	}

	@Override
	public void readData(ObjectDataInput in) throws IOException {
		metric = new Metric();
		metric.readData(in);
		customer = new Customer();
		customer.readData(in);
	}

	@Override
	public String toString() {
		return "EnrichedMetric{" +
				"metric=" + metric +
				", customer=" + customer +
				'}';
	}
}
