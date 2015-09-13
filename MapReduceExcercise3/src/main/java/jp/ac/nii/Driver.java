package jp.ac.nii;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * このクラスは完成しています。
 */
public class Driver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws IOException, ClassNotFoundException,
			InterruptedException {
		// 引数に応じて実行するジョブを変更する
		if (args.length == 0 || "all".equals(args[0])) {
			int returnCode = 0;

			returnCode |= runAllPairAggregationJob(args);
			returnCode |= runSpecPairAggregationJob(args);
			returnCode |= runRelativityCalculationJob(args);

			return returnCode;
		}

		else if ("allpair".equals(args[0])) {
			return runAllPairAggregationJob(args);
		} else if ("specpair".equals(args[0])) {
			return runSpecPairAggregationJob(args);
		} else if ("relativity".equals(args[0])) {
			return runRelativityCalculationJob(args);
		}

		return -1;
	}

	public int runAllPairAggregationJob(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Job allPairJob = AllPairAggregationJob.create();

		return (allPairJob.waitForCompletion(true)) ? 0 : 1;
	}

	public int runSpecPairAggregationJob(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Job specPairJob = SpecPairAggregationJob.create();

		return (specPairJob.waitForCompletion(true)) ? 0 : 1;
	}

	public int runRelativityCalculationJob(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Job relativityJob = RelativityCalculationJob.create();

		return (relativityJob.waitForCompletion(true)) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {

		int returnCode = ToolRunner.run(new Driver(), args);
		System.exit(returnCode);
	}
}
