

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
