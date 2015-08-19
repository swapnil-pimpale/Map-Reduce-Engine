/**
 * Class Partitioner:
 * This implements a partition function which determines to which reducer should a particular file be sent to.
 * All nodes implement the same partition function and hence one key gets mapped to the same
 * reducer from any DataNode
 */

public class Partitioner {

	private int numberOfReducers;
	private String key;
	
	public Partitioner(int numberOfReducers, String key) {
		setNumberOfReducers(numberOfReducers);
		setKey(key);
	}

	public int getNumberOfReducers() {
		return numberOfReducers;
	}

	public void setNumberOfReducers(int numberOfReducers) {
		this.numberOfReducers = numberOfReducers;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}
	
	public int partition() {
		int value = (key.hashCode())%numberOfReducers; 
		if(value<0) {
			value *= -1;
		}
		return value;
	}

}
