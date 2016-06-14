package jp.ac.nii;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {

	public static class TokenizerMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text wordText = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// Convert Text to String and make it lower case
			String line = value.toString().toLowerCase();
			String[] words = line.split(" ");
			for (String word : words) {
				if (isWord(word)) {
					// Convert String to Text for Hadoop
					wordText.set(word);
					context.write(wordText, one);
				}
			}
		}

		private boolean isWord(String word) {
			for (int i = 0; i < word.length(); i++) {
				if (!Character.isAlphabetic(word.charAt(i))) {
					return false;
				}
			}
			return word.length() > 0;
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.out.println("hadoop jar MapReduceExcercise1-0.0.1-SNAPSHOT-jar-with-dependencies.jar jp.ac.nii.WordCount <input_hdfs_file> <output_hdfs_dir>");
			return;
		}
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCount.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
