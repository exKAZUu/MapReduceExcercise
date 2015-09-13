package jp.ac.nii;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 以下の式の分母（denominator）を計算するジョブのJobです。
 *   関連度 = 商品Xと商品Yのペアの総数 / 商品Xを含むペアの総数
 */
public class AllPairAggregationJob {

	// HDFS上の入力ファイル「購入ペア」(/exkazuu/ex3/input/goods_pair)
	private static final Path inputFile = new Path(FilePathConstants.FILE_BASE
			+ "/" + FilePathConstants.GOODS_PAIR_FILE_NAME);

	// HDFS上に出力されるファイル「関連度分母データ」(/exkazuu/ex3/input/denomination)
	private static final Path outputFile = new Path(FilePathConstants.FILE_BASE
			+ "/" + FilePathConstants.DENOMINATION_FILE_NAME);

	public static Job create() throws IOException {
		Job job = Job.getInstance();
		job.setJobName("AllPairAggregationJob");
		job.setJarByClass(AllPairAggregationJob.class);

		// TODO: MapperクラスとReducerクラスを設定するコードを記載してください
		job.setMapperClass(AllPairAggregationMapper.class);
		job.setReducerClass(AllPairAggregationReducer.class);

		// TODO: 中間データのKeyとValueの型を設定するコードを記載してください
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		// TODO: 利用するInputFormatとOutputFormatを設定するコードを記載してください
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// TODO: HDFS上の入力ファイルと出力ファイルのパスを設定するコードを記載してください
		FileInputFormat.addInputPath(job, inputFile);
		FileOutputFormat.setOutputPath(job, outputFile);

		job.setNumReduceTasks(10);
		return job;
	}
}
