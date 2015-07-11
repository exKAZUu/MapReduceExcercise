package jp.ac.nii.backend;

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
public class AllPairAggregationJob extends Job {

	// HDFS上の入力ファイル「購入ペア」(/user/root/hadoop_exercise/3/data/goods_pair)
	private static final Path inputFile = new Path(FilePathConstants.FILE_BASE
			+ "/" + FilePathConstants.GOODS_PAIR_FILE_NAME);

	// HDFS上に出力されるファイル「関連度分母データ」(/user/root/hadoop_exercise/3/data/denomination)
	private static final Path outputFile = new Path(FilePathConstants.FILE_BASE
			+ "/" + FilePathConstants.DENOMINATION_FILE_NAME);

	public AllPairAggregationJob() throws IOException {
		this.setJobName("AllPairAggregationJob");
		this.setJarByClass(AllPairAggregationJob.class);

		// TODO: MapperクラスとReducerクラスを設定するコードを記載してください
		this.setMapperClass(AllPairAggregationMapper.class);
		this.setReducerClass(AllPairAggregationReducer.class);

		// TODO: 中間データのKeyとValueの型を設定するコードを記載してください
		this.setMapOutputKeyClass(Text.class);
		this.setMapOutputValueClass(IntWritable.class);
		this.setOutputKeyClass(NullWritable.class);
		this.setOutputValueClass(Text.class);

		// TODO: 利用するInputFormatとOutputFormatを設定するコードを記載してください
		this.setInputFormatClass(TextInputFormat.class);
		this.setOutputFormatClass(TextOutputFormat.class);

		// TODO: HDFS上の入力ファイルと出力ファイルのパスを設定するコードを記載してください
		FileInputFormat.addInputPath(this, inputFile);
		FileOutputFormat.setOutputPath(this, outputFile);

		this.setNumReduceTasks(10);
	}
}
