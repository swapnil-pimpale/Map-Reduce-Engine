/**
 * Class KeyValue: Abstraction for an intermediate key-value pair
 * It wraps the key and the value.
 * The keys should be comparable to one another so that they can be sorted
 * @param <KEY>
 * @param <VALUE>
 */

public class KeyValue<KEY extends Comparable<KEY> , VALUE> implements Comparable<KeyValue<KEY,VALUE> > {

	KEY key;
	VALUE value;
	
	public KeyValue(KEY key, VALUE value) {
		this.key = key;
		this.value = value;
	}
	
	@Override
	public int compareTo(KeyValue<KEY, VALUE> o) {
		return (key.compareTo(o.getKEY()));
	}

	public KEY getKEY() {
		return key;
	}
	
	public VALUE getVALUE() {
		return value;
	}

}
