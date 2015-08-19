

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

	private KEY getKEY() {
		return key;
	}
	
	private VALUE getVALUE() {
		return value;
	}

}
