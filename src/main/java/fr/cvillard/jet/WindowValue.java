package fr.cvillard.jet;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * Stores a couple of values to be able to compute a call failure ratio on a given timeframe per Customer
 */
public class WindowValue implements DataSerializable {

	/**
	 * The total calls count
	 */
	private long totalCount;

	/**
	 * The failed calls count
	 */
	private long failedCount;

	public WindowValue() {
	}

	public WindowValue(long totalCount, long failedCount) {
		this.totalCount = totalCount;
		this.failedCount = failedCount;
	}

	/**
	 * @return totalCount
	 */
	public long getTotalCount() {
		return totalCount;
	}

	/**
	 * @param totalCount Value of totalCount
	 */
	public void setTotalCount(long totalCount) {
		this.totalCount = totalCount;
	}

	/**
	 * @return failedCount
	 */
	public long getFailedCount() {
		return failedCount;
	}

	/**
	 * @param failedCount Value of failedCount
	 */
	public void setFailedCount(long failedCount) {
		this.failedCount = failedCount;
	}

	@Override
	public void writeData(ObjectDataOutput out) throws IOException {
		out.writeLong(totalCount);
		out.writeLong(failedCount);
	}

	@Override
	public void readData(ObjectDataInput in) throws IOException {
		totalCount = in.readLong();
		failedCount = in.readLong();
	}

	@Override
	public String toString() {
		return "WindowValue{" +
				"totalCount=" + totalCount +
				", failedCount=" + failedCount +
				'}';
	}
}
