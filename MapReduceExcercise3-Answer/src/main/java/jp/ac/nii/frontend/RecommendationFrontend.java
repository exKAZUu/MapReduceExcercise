package jp.ac.nii.frontend;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Iterator;
import java.util.LinkedHashMap;

import jp.ac.nii.backend.Driver;
import jp.ac.nii.backend.FilePathConstants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class RecommendationFrontend {

	private Configuration conf;
	private FileSystem fs;
	private LinkedHashMap<Integer, String> goodsListMap = new LinkedHashMap<Integer, String>();
	private LinkedHashMap<String, Double> recommendGoodsMap = new LinkedHashMap<String, Double>();

	public RecommendationFrontend() throws IOException {
		conf = new Configuration();
		String urlStr = conf.get("fs.default.name");
		System.out.println(urlStr);
		fs = FileSystem.get(URI.create(urlStr), conf);

	}

	private void loadGoodsList() throws IOException {

		String hdfsGoodsListFile = FilePathConstants.FILE_BASE + "/"
				+ FilePathConstants.GOODS_LIST_FILE_NAME;

		BufferedReader reader = null;

		try {
			try {
				FSDataInputStream is = fs.open(new Path(
						FilePathConstants.FILE_BASE + "/"
								+ FilePathConstants.GOODS_LIST_FILE_NAME));
				reader = new BufferedReader(new InputStreamReader(is));

			} catch (IOException e) {
				e.printStackTrace();
				System.err
						.println("商品リストファイル(goods_list)をロードできません。HDFS上に商品リストファイルが配置されていない可能性があります。");
				System.err
						.println("hadoop dfs -putでHDFS上に商品リストファイルをアップロードしてください。");
				System.exit(1);
			}

			try {
				String line;
				int num = 0;
				while ((line = reader.readLine()) != null) {
					goodsListMap.put(Integer.valueOf(num), line);
					num++;
				}
			} catch (IOException e) {
				e.printStackTrace();
				System.err.println("商品リストファイルの読み込み中に例外が発生しました。");
				System.err.println("担当講師に確認してください。");
				System.exit(1);
			}
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					throw e;
				}
			}
		}

	}

	private void showGoodsList() {

		Iterator<Integer> iterator = goodsListMap.keySet().iterator();

		System.out.println("商品番号\t商品名");
		while (iterator.hasNext()) {
			int goodsNumber = iterator.next();
			System.out.println(goodsNumber + "\t"
					+ goodsListMap.get(goodsNumber));
		}

	}

	private int selectGoods() throws IOException {
		BufferedReader reader = null;

		try {

			reader = new BufferedReader(new InputStreamReader(System.in));

			try {
				String inputNum;
				while (true) {
					System.out
							.print("商品番号を入力し、商品を選択してください[-1 = 終了 , -2 = 商品リストの再表示] >");
					inputNum = reader.readLine();
					try {
						if (goodsListMap
								.containsKey(Integer.parseInt(inputNum))) {
							return Integer.parseInt(inputNum);
						} else if (inputNum.equals("-1")) {
							return Integer.parseInt(inputNum);
						} else if (inputNum.equals("-2")) {
							showGoodsList();
						} else {
							continue;
						}
					} catch (NumberFormatException e) {
						continue;
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
				System.err.println("商品番号の入力中に例外が発生しました。プログラムを終了します。");
				System.exit(1);
				return 1;
			}
		} finally {
			try {
				if (reader != null) {
					reader.close();
				}
			} catch (IOException e) {
				throw e;
			}
		}
	}

	private static enum InnerState {
		BEGIN, FIND_SELECTED_GOODS
	};

	private void loadRecommendationGoods(int selectedNo) throws IOException {
		String relatedGoodsFile = FilePathConstants.FILE_BASE + "/"
				+ FilePathConstants.RELATED_GOODS_FILE_NAME;

		BufferedReader reader = null;

		Path relatedGoodsPath = null;
		try {

			relatedGoodsPath = new Path(relatedGoodsFile);
			FileStatus fileStatus = fs.getFileStatus(relatedGoodsPath);

			if (!fileStatus.isDir()) {
				throw new FileNotFoundException(relatedGoodsPath.toString());
			}
		} catch (IOException e) {
			e.printStackTrace();
			System.err
					.println("関連商品データ(related_goods)をロードできません。HDFS上に関連商品データが作られていない可能性があります。");
			System.err
					.println("バックエンドジョブを実行し、関連商品データを作成してください。バックエンドジョブの実行は次のように行います。");
			System.err.println("hadoop jar recommendation-0.1.jar "
					+ Driver.class.getName());
			System.exit(1);
		}

		try {
			FileStatus[] statusList = fs.listStatus(relatedGoodsPath);
			int count = 0;
			String selectedGoods = goodsListMap
					.get(Integer.valueOf(selectedNo));

			toploop: for (FileStatus status : statusList) {
				try {
					if (count >= 3)
						break;

					Path entityPath = status.getPath();
					if (!entityPath.getName().startsWith("_")) {
						FSDataInputStream is = fs.open(entityPath);
						reader = new BufferedReader(new InputStreamReader(is));

						String goodsPair;
						InnerState state = InnerState.BEGIN;

						while (true) {

							if (count >= 3) {
								break toploop;
							}

							goodsPair = reader.readLine();
							if (goodsPair == null)
								break;

							if (!goodsPair.startsWith(selectedGoods)) {

								if (state == InnerState.BEGIN) {
									continue;
								} else {
									state = InnerState.BEGIN;
									break;
								}
							}

							state = InnerState.FIND_SELECTED_GOODS;
							String[] separated = goodsPair.split(",");
							recommendGoodsMap.put(separated[1],
									Double.parseDouble(separated[2]));
							count++;
						}
					}
				} catch (IOException e) {
					throw e;

				} finally {
					try {
						if (reader != null) {
							reader.close();
						}
					} catch (IOException e) {
						throw e;
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			System.err.println("関連商品データの読み込み中に例外が発生しました。");
			System.err.println("担当講師に確認してください。");
			System.exit(1);
		} finally {
			try {
				if (reader != null) {
					reader.close();
				}
			} catch (IOException e) {
				throw e;
			}
		}

	}

	private void showRecommendGoods(int goodsNum) {
		String goodsName = goodsListMap.get(Integer.valueOf(goodsNum));
		System.out.println(goodsName + "とよく一緒に購入されている、おすすめの商品はこちらです。");

		Iterator<String> iterator = recommendGoodsMap.keySet().iterator();
		while (iterator.hasNext()) {
			System.out.println(iterator.next());
		}
	}

	public void recommendMain() throws IOException {
		loadGoodsList();
		showGoodsList();

		int selectedNo = selectGoods();

		if (selectedNo == -1) {
			System.out.println("ByeBye!");
			System.exit(0);
		}

		loadRecommendationGoods(selectedNo);
		showRecommendGoods(selectedNo);
	}

	public static void main(String[] args) throws IOException {

		RecommendationFrontend frontend = new RecommendationFrontend();
		frontend.recommendMain();

	}
}
