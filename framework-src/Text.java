/**
 * Class Text:
 * MapReduce IO class that wraps a String.
 * Can be used by client to implement his/her map/reduce functions
 *
 * This class implements the AddInterface. This is required because we would like to
 * correctly sum up OutCollector values for a particular Key.
 */

public class Text implements Comparable<Text>, AddInterface {

	private String value;
	
	public void set(String value) {
		this.value = value;
	}
	
	@Override
	public String toString() {
		return value;
	}

	@Override
	public int compareTo(Text other) {
		return (value.compareTo(other.toString()));
	}

	@Override
	public void add(Object o) {
		Text i = (Text) o;
		value += i.toString();
	}

}
