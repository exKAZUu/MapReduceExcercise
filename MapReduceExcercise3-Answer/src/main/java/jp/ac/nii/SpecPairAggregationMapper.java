package jp.ac.nii;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 以下の式の分子（numerator）を計算するジョブのMapperです。
 *  関連度 = 商品Xと商品Yのペアの総数 / 商品Xを含むペアの総数
 */
// TODO: 型パラメータを補完してください
// ヒント： Mapperの出力データのKeyとValueの型は、ReducerのKeyとValueの型と一致させる
public class SpecPairAggregationMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	private static final IntWritable one = new IntWritable(1);

	private Text keyOut = new Text();

	@Override
	public void map(LongWritable keyIn, Text valueIn, Context context)
			throws IOException, InterruptedException {

		String[] goodsPair = valueIn.toString().split(",");

		// TODO: 商品のペアを昇順にソートするコードを記載してください
		// ヒント：Arraysクラスのsortメソッドが利用できる
		Arrays.sort(goodsPair);

		keyOut.set(goodsPair[0] + "," + goodsPair[1]);

		// TODO: 昇順でカンマ区切りの商品ペアをKey、「1」をValueとして中間データを出力するコードを記載してください
		context.write(keyOut, one);
	}
}
