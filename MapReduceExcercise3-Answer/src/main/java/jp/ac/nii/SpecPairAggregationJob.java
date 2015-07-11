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
 * 以下の式の分子（numerator）を計算するジョブのJobです。
 *   関連度 = 商品Xと商品Yのペアの総数 / 商品Xを含むペアの総数
 */
public class SpecPairAggregationJob extends Job {

	private static final Path inputFile = new Path(FilePathConstants.FILE_BASE
			+ "/" + FilePathConstants.GOODS_PAIR_FILE_NAME);
	private static final Path outputFile = new Path(FilePathConstants.FILE_BASE
			+ "/" + FilePathConstants.NUMERATOR_FILE_NAME);

	public SpecPairAggregationJob() throws IOException {
		this.setJobName("SpecPairAggregationJob");
		this.setJarByClass(SpecPairAggregationJob.class);

		// TODO: MapperクラスとReducerクラスをセットするコードを記載してください
		this.setMapperClass(SpecPairAggregationMapper.class);
		this.setReducerClass(SpecPairAggregationReducer.class);

		this.setMapOutputKeyClass(Text.class);
		this.setMapOutputValueClass(IntWritable.class);

		// TODO: 出力データのKeyとValueのクラスを設定するコードを記載してください
		this.setOutputKeyClass(NullWritable.class);
		this.setOutputValueClass(Text.class);

		this.setInputFormatClass(TextInputFormat.class);
		this.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(this, inputFile);
		FileOutputFormat.setOutputPath(this, outputFile);

		// TODO: Reduceタスクを10並列で実行するコードを記載してください
		this.setNumReduceTasks(10);
	}
}
