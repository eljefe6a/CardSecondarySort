import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CardDriver extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.printf("Usage: CardDriver <input dir> <output dir>\n");
			return -1;
		}

		Job job = new Job(getConf());
		job.setJarByClass(CardDriver.class);
		job.setJobName("Secondary Sort on cards");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(CardMapper.class);
		job.setReducerClass(CardReducer.class);

		// Need to use these class for secondary sort
		// Partitions only on suit
		job.setPartitionerClass(CardPartitioner.class);
		// Sorts on suit and card number
		job.setSortComparatorClass(CardKeyComparator.class);
		// Groups on suit
		job.setGroupingComparatorClass(CardGroupComparator.class);

		job.setMapOutputKeyClass(CardWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(CardWritable.class);
		job.setOutputValueClass(NullWritable.class);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new CardDriver(), args);
		System.exit(exitCode);
	}
}
