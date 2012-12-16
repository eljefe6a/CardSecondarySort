import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class CardMapper extends Mapper<LongWritable, Text, CardWritable, IntWritable> {
	private static final Logger logger = Logger.getLogger(CardMapper.class);

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();

		String values[] = line.split(" ");

		if (values.length == 2) {
			try {
				// Every line should have values[0] be the suit
				// and values[1] be the card number
				Suit suit = Suit.valueOf(values[0].toUpperCase());
				int cardNumber = Integer.parseInt(values[1]);

				// Emit with a composite key made up of suit
				// and card number
				context.write(new CardWritable(suit, cardNumber), new IntWritable(cardNumber));
			} catch (Exception e) {
				logger.error("Error processing the line \"" + line + "\"");
			}
		} else {
			logger.warn("The input line had more spaces than were expected.  Had " + values.length
					+ " expected 2.  The line was \"" + line + "\"");
		}
	}
}
