import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CardGroupComparator extends WritableComparator {
	protected CardGroupComparator() {
		super(CardWritable.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		CardWritable cw1 = (CardWritable) w1;
		CardWritable cw2 = (CardWritable) w2;

		// Only group on suits so all cards of the same suit
		// go to the same reducer call
		return CardWritable.compareSuits(cw1, cw2);
	}

}