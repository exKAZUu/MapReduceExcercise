package jp.ac.nii;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.ja.JapaneseAnalyzer;
import org.apache.lucene.analysis.ja.JapaneseBaseFormFilter;
import org.apache.lucene.analysis.ja.JapaneseKatakanaStemFilter;
import org.apache.lucene.analysis.ja.JapanesePartOfSpeechStopFilter;
import org.apache.lucene.analysis.ja.JapaneseTokenizer;
import org.apache.lucene.analysis.miscellaneous.LengthFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public class WordCount {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text wordText = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			List<String> words = tokenize(value.toString());
			for (String word : words) {
				wordText.set(word);
				context.write(wordText, one);
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static List<String> tokenize(String japaneseText) {
		JapaneseTokenizer tokenizer = new JapaneseTokenizer(null, true,
				JapaneseTokenizer.Mode.NORMAL);
		TokenStream stream = tokenizer;

		// 参考サイト
		// http://www.mwsoft.jp/programming/hadoop/mapreduce_with_lucene_filter.html
		
		// 小文字に統一
		stream = new LowerCaseFilter(stream);
		// 「こと」「これ」「できる」などの頻出単語を除外
		stream = new StopFilter(stream, JapaneseAnalyzer.getDefaultStopSet());
		// 16文字以上の単語は除外(あまり長い文字列はいらないよね)
		stream = new LengthFilter(stream, 1, 16);
		// 動詞の活用を揃える(疲れた => 疲れる)
		stream = new JapaneseBaseFormFilter(stream);
		// 助詞、助動詞、接続詞などを除外する
		stream = new JapanesePartOfSpeechStopFilter(stream,
				JapaneseAnalyzer.getDefaultStopTags());
		// カタカナ長音の表記揺れを吸収
		stream = new JapaneseKatakanaStemFilter(stream);

		ArrayList<String> result = new ArrayList<String>();

		try {
			tokenizer.setReader(new StringReader(japaneseText));
			stream.reset();
			while (stream.incrementToken()) {
				CharTermAttribute term = stream
						.getAttribute(CharTermAttribute.class);
				result.add(term.toString());
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				tokenizer.close();
			} catch (IOException e) {
			}
		}

		return result;
	}

	public static void main(String[] args) throws Exception {
		for (String word : tokenize("寿司が食べたい。")) {
			System.out.println(word);
		}
		for (String word : tokenize("メガネが嫌いだ。")) {
			System.out.println(word);
		}

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCount.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}