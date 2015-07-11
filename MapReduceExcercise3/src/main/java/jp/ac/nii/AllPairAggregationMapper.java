package jp.ac.nii;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 以下の式の分母（denominator）を計算するジョブのMapperです。
 *   関連度 = 商品Xと商品Yのペアの総数 / 商品Xを含むペアの総数
 */
// TODO: 型パラメータを補完してください
//　ヒント： FileInputFormat系(TextInputFormatなど)を使用する場合、入力のKeyはLongWritable、ValueはText型です
public class AllPairAggregationMapper extends
		Mapper<> {

	private static final IntWritable one = new IntWritable(1);

	private Text keyOut = new Text();

	@Override
	public void map(LongWritable keyIn, Text valuein, Context context)
			throws IOException, InterruptedException {
		// 商品名,商品名のペアを「,」を区切り文字として分割
		String[] goodsPair = valuein.toString().split(",");

		for (String goods : goodsPair) {
			// TODO: ワードカウントの要領で、商品名をKeyに、「1」をValueに設定し、中間データを出力するコードを記載してください
			TODO;
		}
	}
}
