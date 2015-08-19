/**
 * Class DoubleWritable:
 * MapReduce IO class that wraps a Double.
 * Can be used by client to implement his/her map/reduce functions
 *
 * This class implements the AddInterface. This is required because we would like to
 * correctly sum up OutCollector values for a particular Key.
 */

public class DoubleWritable implements Comparable<DoubleWritable>, AddInterface {

        // Double value that is wrapped by this class
	private Double value;
	
	public Double get() { 
		return value;
	}
	
	public void set(String s) {
        this.value = Double.parseDouble(s);
	}

        /**
         * A way to add multiple DoubleWritables
         * @param o
         */
	@Override
	public void add(Object o) {
		DoubleWritable i = (DoubleWritable) o;
		value += i.get();
	}

        /**
         * compare one DoubleWritable to another
         * @param o
         * @return
         */
	@Override
	public int compareTo(DoubleWritable o) {
		return (this.value.compareTo(o.value));
	}

	@Override
        public String toString() {
    	return value.toString();
    }
}
