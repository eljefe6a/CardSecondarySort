import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class CardPartitioner<K2, V2> extends Partitioner<CardWritable, IntWritable> {

	/**
	 * Partitions the cards based on suit
	 */
	public int getPartition(CardWritable key, IntWritable value, int numReduceTasks) {
		// NOTE: For simplicity just the suit is used for partitioning.
		// A more advanced partitioner could divide the card numbers too
		int suit = key.suit.ordinal();

		return suit % numReduceTasks;
	}
}
