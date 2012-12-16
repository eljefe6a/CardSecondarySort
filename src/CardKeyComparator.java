import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.Logger;

public class CardKeyComparator extends WritableComparator {
	private static final Logger logger = Logger.getLogger(CardKeyComparator.class);

	protected CardKeyComparator() {
		super(CardWritable.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		CardWritable cw1 = (CardWritable) w1;
		CardWritable cw2 = (CardWritable) w2;

		// Sorting on the composite key so that suit and card number
		// are taken into account.  This is how values are passed in
		// to the reducer in sorted order.
		int res = cw1.compareTo(cw2);

		logger.info("Comparing " + cw1 + " and " + cw2 + " result: " + res);

		return res;
	}
}
