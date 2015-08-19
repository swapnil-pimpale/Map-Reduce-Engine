
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class OutputCollector<KEY extends Comparable<KEY>, VALUE> {

	private List<KeyValue<KEY,VALUE>> list;
	
	public OutputCollector() {
		list = new ArrayList<KeyValue<KEY,VALUE>>();
	}

	public void collect(KEY key, VALUE value) {
		list.add(new KeyValue<KEY,VALUE>(key,value));
	}

	public void sortByKeys() {
		Collections.sort(list);
	}
	
	public Iterator<KeyValue<KEY,VALUE>> getIterator() {
		return list.iterator();
	}
}