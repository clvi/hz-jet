package fr.cvillard.jet;

/**
 * This accumulator is a updated version of {@link java.util.concurrent.atomic.LongAccumulator} that sum 2 Long values
 */
public class DropCountAccumulator {

	/**
	 * The total call count
	 */
	private long totalCount;

	/**
	 * The failed call count
	 */
	private long failedCount;

	/**
	 * Creates a new instance with {@code totalCount == 0} and {@code failedCount == 0}.
	 */
	public DropCountAccumulator() {
	}

	/**
	 * Creates a new instance with the specified values.
	 */
	public DropCountAccumulator(long total, long failed) {
		this.totalCount = total;
		this.failedCount = failed;
	}

	public DropCountAccumulator add(long totalCount, long failedCount) {
		this.totalCount += totalCount;
		this.failedCount += failedCount;
		return this;
	}

	public DropCountAccumulator combine(DropCountAccumulator that) {
		this.totalCount += that.totalCount;
		this.failedCount += that.failedCount;
		return this;
	}

	public DropCountAccumulator deduct(DropCountAccumulator that) {
		this.totalCount -= that.totalCount;
		this.failedCount -= that.failedCount;
		return this;
	}

	public WindowValue total() {
		return new WindowValue(totalCount, failedCount);
	}

	@Override
	public boolean equals(Object o) {
		return this == o ||
				o != null
						&& this.getClass() == o.getClass()
						&& this.totalCount == ((DropCountAccumulator) o).totalCount
						&& this.failedCount == ((DropCountAccumulator) o).failedCount;
	}

	@Override
	public int hashCode() {
		int result = (int) (totalCount ^ (totalCount >>> 32));
		result = 31 * result + (int) (failedCount ^ (failedCount >>> 32));
		return result;
	}

	@Override
	public String toString() {
		return "DropCountAccumulator{" +
				"totalCount=" + totalCount +
				", failedCount=" + failedCount +
				'}';
	}
}
