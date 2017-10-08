package fr.cvillard.jet;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * A simple Metric, representing a number of calls performed by a customer, and the number of errors received during these calls.
 */
public class Metric implements DataSerializable {

	private long timestampMs;

	private int customerId;

	private int numberOfCalls;

	private int numberOfErrors;

	public Metric(long timestampMs, int customerId, int numberOfCalls, int numberOfErrors) {
		this.timestampMs = timestampMs;
		this.customerId = customerId;
		this.numberOfCalls = numberOfCalls;
		this.numberOfErrors = numberOfErrors;
	}

	// Needed for serialization
	public Metric() {
	}

	/**
	 * @return timestampMs
	 */
	public long getTimestampMs() {
		return timestampMs;
	}

	/**
	 * @param timestampMs Value of timestampMs
	 */
	public void setTimestampMs(long timestampMs) {
		this.timestampMs = timestampMs;
	}

	/**
	 * @return customerId
	 */
	public int getCustomerId() {
		return customerId;
	}

	/**
	 * @param customerId Value of customerId
	 */
	public void setCustomerId(int customerId) {
		this.customerId = customerId;
	}

	/**
	 * @return numberOfCalls
	 */
	public int getNumberOfCalls() {
		return numberOfCalls;
	}

	/**
	 * @param numberOfCalls Value of numberOfCalls
	 */
	public void setNumberOfCalls(int numberOfCalls) {
		this.numberOfCalls = numberOfCalls;
	}

	/**
	 * @return numberOfErrors
	 */
	public int getNumberOfErrors() {
		return numberOfErrors;
	}

	/**
	 * @param numberOfErrors Value of numberOfErrors
	 */
	public void setNumberOfErrors(int numberOfErrors) {
		this.numberOfErrors = numberOfErrors;
	}

	@Override
	public String toString() {
		return "Metric{" +
				"timestampMs=" + timestampMs +
				", customerId=" + customerId +
				", numberOfCalls=" + numberOfCalls +
				", numberOfErrors=" + numberOfErrors +
				'}';
	}

	@Override
	public void writeData(ObjectDataOutput out) throws IOException {
		out.writeLong(timestampMs);
		out.writeInt(customerId);
		out.writeInt(numberOfCalls);
		out.writeInt(numberOfErrors);
	}

	@Override
	public void readData(ObjectDataInput in) throws IOException {
		timestampMs = in.readLong();
		customerId = in.readInt();
		numberOfCalls = in.readInt();
		numberOfErrors = in.readInt();
	}
}
