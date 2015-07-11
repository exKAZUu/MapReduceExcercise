package jp.ac.nii;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 以下の式の関連度を計算するジョブのReducerです。
 *   関連度 = 商品Xと商品Yのペアの総数 / 商品Xを含むペアの総数
 * このクラスは完成しています。
 */
public class RelativityCalculationReducer extends
		Reducer<Text, Text, NullWritable, Text> {

	private static final NullWritable nullWritable = NullWritable.get();

	private Text valueOut = new Text();

	@Override
	public void reduce(Text keyIn, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Iterator<Text> iterator = values.iterator();

		// 一番最初のvalueが分母になるようにRelativityCalculationJobでソート方法を設定済み
		// #dがついたキーは、同じキーの中でも辞書順で最も大きい
		// 辞書順で大きい順にソートするようにしているため、#dがついたキーが必ず最初に来る
		// また、同じキーのバリューを集めるときに、#dを無視して処理しているので、分母データと分子データが混ざる
		double denominator = Double.parseDouble(iterator.next().toString());

		String keyStr = keyIn.toString();
		while (iterator.hasNext()) {
			String[] numeratorGoodsAndNum = iterator.next().toString()
					.split(",");
			double numerator = Double.parseDouble(numeratorGoodsAndNum[1]);
			double relativity = numerator / denominator;

			if (relativity * 1000 > 25) {
				String keyWithoutSharpD = keyStr.substring(0, keyStr.length() - 2);
				valueOut.set(keyWithoutSharpD + "," + numeratorGoodsAndNum[0] + "," + relativity);
				context.write(nullWritable, valueOut);
			}
		}
	}
}
