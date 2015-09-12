package jp.ac.nii;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * 以下の式の関連度を計算するジョブのJobです。 
 * 関連度 = 商品Xと商品Yのペアの総数 / 商品Xを含むペアの総数
 * このクラスは完成しています。
 */
public class RelativityCalculationJob extends Job {

	private static final Path denominationtFile = new Path(
			FilePathConstants.FILE_BASE + "/"
					+ FilePathConstants.DENOMINATION_FILE_NAME);
	private static final Path numerationFile = new Path(
			FilePathConstants.FILE_BASE + "/"
					+ FilePathConstants.NUMERATOR_FILE_NAME);
	private static final Path outputFile = new Path(FilePathConstants.FILE_BASE
			+ "/" + FilePathConstants.RELATED_GOODS_FILE_NAME);

	public RelativityCalculationJob() throws IOException {
		this.setJobName("RelativityCalculationJob");
		this.setJarByClass(RelativityCalculationJob.class);

		this.setMapperClass(RelativityCaclulationMapper.class);
		this.setReducerClass(RelativityCalculationReducer.class);

		this.setMapOutputKeyClass(Text.class);
		this.setMapOutputValueClass(Text.class);
		this.setOutputKeyClass(NullWritable.class);

		this.setInputFormatClass(TextInputFormat.class);
		this.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(this, denominationtFile);
		FileInputFormat.addInputPath(this, numerationFile);
		FileOutputFormat.setOutputPath(this, outputFile);

		// キーの並び順をどうするか（ノードの割り当て前のキーのソート処理の制御）
		this.setSortComparatorClass(RelativityCalculationSortComparator.class);
		// どのノードでReduce処理を実行するか
		this.setPartitionerClass(RelativityCalculationPartitioner.class);
		// どのキーとどのキーを同一とみなすか（Reduceの処理単位の制御）
		this.setGroupingComparatorClass(RelativityCalculationGroupComparator.class);

		this.setNumReduceTasks(10);
	}

	public static Text removeSharpD(Text key) {
		String keyStr = key.toString();
		if (keyStr.endsWith("#d")) {
			return new Text(keyStr.substring(0, keyStr.length() - 2));
		}
		return key;
	}

	private static class RelativityCalculationPartitioner extends
			HashPartitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {
			// キーの末尾に"#d"が入っている場合は取り除いてハッシュ値を得る
			key = removeSharpD(key);
			return super.getPartition(key, value, numReduceTasks);
		}
	}

	private static class RelativityCalculationGroupComparator extends
			WritableComparator {

		public RelativityCalculationGroupComparator() {
			super(Text.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			try {
				int n1 = WritableUtils.decodeVIntSize(b1[s1]);
				int n2 = WritableUtils.decodeVIntSize(b2[s2]);

				String a = Text.decode(b1, s1 + n1, l1 - n1);
				String b = Text.decode(b2, s2 + n2, l2 - n2);

				return compare(new Text(a), new Text(b));
			} catch (CharacterCodingException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			if (a == b) {
				return 0;
			}
			Text left = removeSharpD((Text) a);
			Text right = removeSharpD((Text) b);
			return -left.compareTo(right);
		}
	}

	private static class RelativityCalculationSortComparator extends
			WritableComparator {

		public RelativityCalculationSortComparator() {
			super(Text.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			try {
				int n1 = WritableUtils.decodeVIntSize(b1[s1]);
				int n2 = WritableUtils.decodeVIntSize(b2[s2]);

				String a = Text.decode(b1, s1 + n1, l1 - n1);
				String b = Text.decode(b2, s2 + n2, l2 - n2);

				return compare(new Text(a), new Text(b));
			} catch (CharacterCodingException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			if (a == b) {
				return 0;
			}
			return -a.compareTo(b);
		}
	}
}
