package jp.ac.nii;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 以下の式の分子（numerator）を計算するジョブのReducerです。
 *  関連度 = 商品Xと商品Yのペアの総数 / 商品Xを含むペアの総数
 */
public class SpecPairAggregationReducer extends
		Reducer<> {

	private static final NullWritable nullWritable = NullWritable.get();

	private Text valueOut = new Text();

	@Override
	public void reduce(Text keyIn, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		Iterator<IntWritable> iterator = values.iterator();

		// 特定の商品のペアの出現回数をカウント
		while (iterator.hasNext()) {
			sum += iterator.next().get();
		}

		valueOut.set(keyIn.toString() + "," + sum);

		// TODO 空のKeyと、 商品のペアとペアの出現回数がカンマ区切りで記録されたデータをValueとして出力するコードを記載してください
		TODO;
	}
}
