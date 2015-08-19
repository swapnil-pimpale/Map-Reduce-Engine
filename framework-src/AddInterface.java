/**
 * AddInterface: This interface declares a generic add() method.
 * This interface is implemented by all the *Writable classes
 * (like IntWritable, LongWritable, etc)
 *
 * This method is used during the Reduce phase of the job to
 * correctly sum up OutCollector values for a particular Key.
 * It provides a generic way of adding two values of the same type.
 */
public interface AddInterface {
	public void add(Object o);
}
