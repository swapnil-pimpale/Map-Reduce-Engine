/**
 * RecordReader: Provides Abstraction for BufferedReader.
 * Reading one records is equivalent to reading a line
 */
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;


public class RecordReader {	
	private BufferedReader br;
	
	public RecordReader(String inputFilePath) {
		try {
			this.br = new BufferedReader(new FileReader(inputFilePath));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public String getNextRecord() {
		try {
			return br.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public void close() {
		try {
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
