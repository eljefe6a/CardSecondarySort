import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CardReducer extends Reducer<CardWritable, IntWritable, CardWritable, NullWritable> {

	@Override
	public void reduce(CardWritable key, Iterable<IntWritable> values, Context context) throws IOException,
			InterruptedException {
		// Reducer call will get all values for a suit
		// with the values in sorted order
		for (IntWritable value : values) {
			context.write(new CardWritable(key.suit, value.get()), NullWritable.get());
		}
	}
}