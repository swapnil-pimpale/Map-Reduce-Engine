

public class IntWritable implements Comparable<IntWritable>, AddInterface {
        private Integer value;
             
        public void set(String s) {
                this.value = Integer.parseInt(s);
        }

        public Integer get() {
                return this.value;
        }

        @Override
        public int compareTo(IntWritable o) {
                return (this.value.compareTo(o.value));
        }
        
        @Override
        public String toString() {
        	return value.toString();
        }

		@Override
		public void add(Object o) {
			IntWritable i = (IntWritable) o;
			value += i.get();
			
		}     
       
}