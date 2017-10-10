package fr.cvillard.jet;

import com.hazelcast.jet.TimestampedEntry;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * This key is used for the aggregation output map
 */
public class WindowKey implements DataSerializable {

	/**
	 * The window timestamp
	 */
	private long timestamp;

	/**
	 * The customer ID
	 */
	private int customerId;

	public WindowKey() {
	}

	public WindowKey(long timestamp, int customerId) {
		this.timestamp = timestamp;
		this.customerId = customerId;
	}

	public WindowKey(TimestampedEntry<Integer, Double> entry) {
		this.timestamp = entry.getTimestamp();
		this.customerId = entry.getKey();
	}

	/**
	 * @return timestamp
	 */
	public long getTimestamp() {
		return timestamp;
	}

	/**
	 * @param timestamp Value of timestamp
	 */
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
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

	@Override
	public void writeData(ObjectDataOutput out) throws IOException {
		out.writeLong(timestamp);
		out.writeInt(customerId);
	}

	@Override
	public void readData(ObjectDataInput in) throws IOException {
		timestamp = in.readLong();
		customerId = in.readInt();
	}

	@Override
	public String toString() {
		return "WindowKey{" +
				"timestamp=" + timestamp +
				", customerId=" + customerId +
				'}';
	}
}
