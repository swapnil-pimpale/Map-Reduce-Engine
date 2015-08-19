/**
 * Chunk: This class represents a chunk in our Distributed FS.
 *
 * Each chunk is associated with a "unique" chunkID across the DFS.
 * A chunk is a collection of records where each record is a line in the
 * input file. Number of lines in a chunk is defined in the config file.
 */


import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class Chunk implements Serializable {
	private List<String> chunk;
	private int chunkID;

        /**
         * Constructor: Creates a new ArrayList of Strings to store records
         * from the input file.
         * @param chunkID: sets the unique chunkID for this chunk
         */
	public Chunk(int chunkID) {
		chunk = new ArrayList<String>();
		this.chunkID = chunkID;
	}

        /**
         * addRecordToChunk:
         * @param line: Add this line (Record) to the chunk
         */
	public void addRecordToChunk(String line) {
		chunk.add(line);
	}

        /**
         * Getter and Setter for chunkIDs
         */
	public int getChunkID() {
		return chunkID;
	}

	public void setChunkID(int chunkID) {
		this.chunkID = chunkID;
	}
	
	public Iterator<String> getChunkIterator() {
		return chunk.iterator();
	}

        /**
         * removeChunkData: Free up the chunk data when we no more need it.
         */
	public void removeChunkData() {
		chunk = null;
	}
}
