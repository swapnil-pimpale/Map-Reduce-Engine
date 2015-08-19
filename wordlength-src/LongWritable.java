
public class LongWritable implements Comparable<LongWritable>, AddInterface {

	private Long value;
	
	public Long get() {
		return value;
	}
	
	public void set(String s) {
        this.value = Long.parseLong(s);
	}
	

	@Override
	public void add(Object o) {
		LongWritable i = (LongWritable) o;
		value += i.get();
	}

	@Override
	public int compareTo(LongWritable o) {
		return (this.value.compareTo(o.value));
	}
	
	@Override
    public String toString() {
    	return value.toString();
    }
	
}
