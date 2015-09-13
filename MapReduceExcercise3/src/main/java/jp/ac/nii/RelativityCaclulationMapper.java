package jp.ac.nii;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 以下の式の関連度を計算するジョブのMapperです。 
 * 関連度 = 商品Xと商品Yのペアの総数 / 商品Xを含むペアの総数
 */
public class RelativityCaclulationMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	private Writer writer;

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		String filePath = ((FileSplit) context.getInputSplit()).getPath()
				.toString();

		if (filePath.indexOf(FilePathConstants.FILE_BASE + "/"
				+ FilePathConstants.DENOMINATION_FILE_NAME) > 0) {
			writer = new DenominationWriter();
		} else if (filePath.indexOf(FilePathConstants.FILE_BASE + "/"
				+ FilePathConstants.NUMERATOR_FILE_NAME) > 0) {
			writer = new NumeratorWriter();
		} else {
			throw new RuntimeException("Invalid Input File : " + filePath);
		}
	}

	@Override
	public void map(LongWritable keyIn, Text valueIn, Context context)
			throws IOException, InterruptedException {

		writer.write(keyIn, valueIn, context);
	}

	private interface Writer {
		public void write(LongWritable keyIn, Text valueIn, Context context)
				throws IOException, InterruptedException;
	}

	private class DenominationWriter implements Writer {
		@Override
		public void write(LongWritable keyIn, Text valueIn, Context context)
				throws IOException, InterruptedException {
			// 商品名,出現回数
			String[] goodsNameAndNum = valueIn.toString().split(",");

			// TODO: 分母(DenominationWriter)と分子(NumeratorWriter)を区別するためにキーの末尾に"#d"文字列を追加
			context.write(, new Text(goodsNameAndNum[1]));
		}
	}

	private class NumeratorWriter implements Writer {
		private Text keyOut = new Text();
		private Text valueOut = new Text();

		@Override
		public void write(LongWritable keyIn, Text valueIn, Context context)
				throws IOException, InterruptedException {
			// 商品名1,商品名2,出現回数
			String[] goodsPairAndNum = valueIn.toString().split(",");
			
			// TODO: キーが「商品名1」でバリューが「商品名2,出現回数」と、キーが「商品名2」でバリューが「商品名1,出現回数」で2回出力してください
			TODO;
		}
	}
}
