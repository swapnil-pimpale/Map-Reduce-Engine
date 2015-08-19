/**
 * Class IntWritable:
 * MapReduce IO class that wraps a Integer.
 * Can be used by client to implement his/her map/reduce functions
 *
 * This class implements the AddInterface. This is required because we would like to
 * correctly sum up OutCollector values for a particular Key.
 */

public class IntWritable implements Comparable<IntWritable>, AddInterface {
        // Integer value that is wrapped by this class
        private Integer value;
             
        public void set(String s) {
                this.value = Integer.parseInt(s);
        }

        public Integer get() {
                return this.value;
        }

        /**
         * compare one DoubleWritable to another
         * @param o
         * @return
         */
        @Override
        public int compareTo(IntWritable o) {
                return (this.value.compareTo(o.value));
        }
        
        @Override
        public String toString() {
        	return value.toString();
        }

        /**
         * A way to add multiple DoubleWritables
         * @param o
         */
        @Override
        public void add(Object o) {
                IntWritable i = (IntWritable) o;
                value += i.get();

        }


}