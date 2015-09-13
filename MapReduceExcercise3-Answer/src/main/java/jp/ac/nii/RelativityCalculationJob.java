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
public class RelativityCalculationJob {

	private static final Path denominationtFile = new Path(
			FilePathConstants.FILE_BASE + "/"
					+ FilePathConstants.DENOMINATION_FILE_NAME);
	private static final Path numerationFile = new Path(
			FilePathConstants.FILE_BASE + "/"
					+ FilePathConstants.NUMERATOR_FILE_NAME);
	private static final Path outputFile = new Path(FilePathConstants.FILE_BASE
			+ "/" + FilePathConstants.RELATED_GOODS_FILE_NAME);

	public static Job create() throws IOException {
		Job job = Job.getInstance();
		job.setJobName("RelativityCalculationJob");
		job.setJarByClass(RelativityCalculationJob.class);

		job.setMapperClass(RelativityCaclulationMapper.class);
		job.setReducerClass(RelativityCalculationReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, denominationtFile);
		FileInputFormat.addInputPath(job, numerationFile);
		FileOutputFormat.setOutputPath(job, outputFile);

		// キーの並び順をどうするか（Reduceタスクの割り当て前のキーのソート処理の制御）
		job.setSortComparatorClass(RelativityCalculationSortComparator.class);
		// どのReduceタスクでキー（と対応するバリュー）を処理するか（Reduceタスクの割り当て制御）
		job.setPartitionerClass(RelativityCalculationPartitioner.class);
		// どのキーとどのキーを同一とみなして、Reducerのバリューリストに集約するか（Reduceの処理単位の制御）
		job.setGroupingComparatorClass(RelativityCalculationGroupComparator.class);

		job.setNumReduceTasks(10);
		return job;
	}

	public static Text removeSharpD(Text key) {
		String keyStr = key.toString();
		if (keyStr.endsWith("#d")) {
			return new Text(keyStr.substring(0, keyStr.length() - 2));
		}
		return key;
	}

	/**
	 * 以下のように分母データと分子データが入り乱れているので、例えば以下の
	 * <あんドーナツ, ところてん,1200(注：分子データ)>, <あんドーナツ#d, 5400(注：分母データ)>, <あんドーナツ, 生シュークリーム,2000(注：分子データ)>
	 * 「あんドーナツ」と「あんドーナツ#d」が同じキーと見なして、同じReduceタスクで処理されるようにハッシュ計算処理を制御する。
	 */
	private static class RelativityCalculationPartitioner extends
			HashPartitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {
			// キーの末尾に"#d"が入っている場合は取り除いてハッシュ値を得る
			key = removeSharpD(key);
			return super.getPartition(key, value, numReduceTasks);
		}
	}

	/**
	 * 以下のように分母データと分子データが入り乱れているので、例えば以下の
	 * <あんドーナツ, ところてん,1200(注：分子データ)>, <あんドーナツ#d, 5400(注：分母データ)>, <あんドーナツ, 生シュークリーム,2000(注：分子データ)>
	 * 「あんドーナツ」と「あんドーナツ#d」を同じキーと見なして、バリューリストにまとめられるように比較処理を制御する。
	 */
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

	/**
	 * 以下のように分母データと分子データが入り乱れているので、例えば以下の
	 * <あんドーナツ, ところてん,1200(注：分子データ)>, <あんドーナツ#d, 5400(注：分母データ)>, <あんドーナツ, 生シュークリーム,2000(注：分子データ)>
	 * キーに対するソート時の比較処理を制御することで、以下のように分母データが先頭に来るようにする。
	 * <あんドーナツ#d, 5400(注：分母データ)>, <あんドーナツ, ところてん,1200(注：分子データ)>, <あんドーナツ, 生シュークリーム,2000(注：分子データ)>
	 */
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
