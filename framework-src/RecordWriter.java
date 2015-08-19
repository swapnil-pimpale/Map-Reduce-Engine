import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;


public class RecordWriter {
	private BufferedWriter bw;
	
	public RecordWriter(String outputFilePath) {
		try {
			this.bw = new BufferedWriter(new FileWriter(outputFilePath, true));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void writeRecord(String line) {
		try {
			bw.write(line);
			bw.newLine();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void writeChunk(Chunk chunk) {
		String line;
		Iterator<String> itr = chunk.getChunkIterator();
		while(itr.hasNext()) {
			writeRecord(itr.next());
		}		
	}
	public void close() {
		try {
			bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
