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
		// ヒント1: 正しく他のファイルが書けていれば、valuesの先頭は分母データで、2個目以降は分子データになる
		// ヒント2: 正しく他のファイルが書けていれば、keyは「あんドーナツ#d」というように、末尾に#dが付いている
		
		String goodsNameWithSharpD = keyIn.toString();
		String goodsName = goodsNameWithSharpD.substring(0, goodsNameWithSharpD.length() - 2)/* TODO: 商品Xの名前を設定 */;
		Iterator<Text> iterator = values.iterator();

		// 一番最初のvalueが分母になるようにソート済み（RelativityCalculationJob）
		double denominator = Integer.parseInt(iterator.next().toString());

		while (iterator.hasNext()) {
			String[] nameAndNumerator = iterator.next().toString().split(",");
			String pairGoodsName = nameAndNumerator[0] /* TODO: 関連度を計算する商品Yの名前を設定 */;
			double relativity = Integer.parseInt(nameAndNumerator[1]) / denominator /* TODO: 関連度を計算 */;

			// 関連度が低すぎる（0.025以下）ペアは関連していないとみなしてフィルタリング
			if (relativity > 0.025) {
				valueOut.set(goodsName + "," + pairGoodsName + "," + relativity);
				context.write(nullWritable, valueOut);
			}
		}
	}
}
