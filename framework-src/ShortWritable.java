/**
 * Class ShortWritable:
 * MapReduce IO class that wraps a Short.
 * Can be used by client to implement his/her map/reduce functions
 *
 * This class implements the AddInterface. This is required because we would like to
 * correctly sum up OutCollector values for a particular Key.
 */
public class ShortWritable implements Comparable<ShortWritable>, AddInterface {

	private Short value;
	
	public Short get() {
		return value;
	}
	
	public void set(String s) {
        this.value = Short.parseShort(s);
	}
	
	@Override
	public void add(Object o) {
		ShortWritable i = (ShortWritable) o;
		value = (short) (value + i.get());
	}

	@Override
	public int compareTo(ShortWritable o) {
		return (this.value.compareTo(o.value));
	}
	
	@Override
    public String toString() {
    	return value.toString();
    }
}
