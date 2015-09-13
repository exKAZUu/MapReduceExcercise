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
public class SpecPairAggregationMapper extends Mapper<> {

	private static final IntWritable one = new IntWritable(1);

	private Text keyOut = new Text();

	@Override
	public void map(LongWritable keyIn, Text valueIn, Context context)
			throws IOException, InterruptedException {
	    // TODO: 商品ペアの名前を昇順でソートした後、キーを「商品X,商品Y」という文字列、バリューを1にしてペアの頻度を計算するMapperを作ろう
      
		String[] goodsPair = valueIn.toString().split(",");

		// TODO: 商品のペアを昇順にソートするコードを記載してください
		// ヒント：Arraysクラスのsortメソッドが利用できる
		TODO;

		keyOut.set(goodsPair[0] + "," + goodsPair[1]);

		// TODO: 昇順でカンマ区切りの商品ペアをKey、「1」をValueとして中間データを出力するコードを記載してください
		TODO;
	}
}
