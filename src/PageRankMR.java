package PageRank;

import java.io.IOException;

import java.io.InputStreamReader;
import java.util.*;
import java.io.*;
import java.net.*;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.PrintWriter;
import java.net.URI;

public class PageRankMR {

	// public static long N;

	static StringBuffer temp, temp1;

	// static int c1, c2 = 0;

	public static class Map extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// String mapperKey = "N=";
			String line = value.toString();
			if (!line.trim().equals("")) {
				int N = context.getConfiguration().getInt("N", 1);
				int iFlag = context.getConfiguration().getInt("iFlag", 1);
				String[] mapperKey = line.split("    ");
				double currentPR = Double.parseDouble(mapperKey[1]);
				if (iFlag == 1) {
					currentPR = currentPR / N;
				}
				int externalLinks = mapperKey.length - 2;
				double prFraction = currentPR / externalLinks;
				// for the HashMap
				int offset = value.toString().indexOf("    ") + 3;

				for (int i = 0; i < mapperKey.length; i++) {
					// if(!mapperKey[i].trim().equals(""))
					if (i == 0) {
						context.write(new Text(mapperKey[i].trim()),
								new DoubleWritable(0.0));
					}
					if (i > 1)
						context.write(new Text(mapperKey[i].trim()),
								new DoubleWritable(prFraction));
				}
			}
		}
	}

	public static class Reduce extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {

			int N = context.getConfiguration().getInt("N", 1);
			double sum = 0;
			int offset1, offset2, count = 0;
			for (DoubleWritable val : values) {
				sum += val.get();
			}

			double pR = (1 - 0.85) / N + 0.85 * sum;

			context.write(key, new DoubleWritable(pR));
		}
	}

	public void call(String[] args, int pageCount, int iter, String bucketName)
			throws Exception {

		Configuration conf = new Configuration();

		conf.setInt("N", pageCount);
		conf.setInt("iFlag", iter);

		Job job = new Job(conf, "pagerank");
		job.setJarByClass(PageRankMR.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(1);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		Path outPath = new Path(args[1]);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileSystem dfs = FileSystem.get(outPath.toUri(), conf);

		if (dfs.exists(outPath)) {
			dfs.delete(outPath, true);
		}
		String temp = outPath.toString().replace("1", "");
		job.waitForCompletion(true);
		boolean f = true;
		// if(iter == 1 || iter == 8){
		while (!job.isComplete()) {
			if (f) {
				System.out.println(iter);
				f = false;
			}

		}
		if (dfs.exists(new Path(outPath + "/PageRank.iter" + iter + ".out"))) {
			dfs.delete(new Path(outPath + "/PageRank.iter" + iter + ".out"),
					true);
		}
		dfs.rename(new Path(outPath + "/part-r-00000"), new Path(temp
				+ "/PageRank.iter" + iter + ".out"));
		// }
		// StringBuffer v;
		/*
		 * if (dfs.exists(new Path(temp+"/PageRank.outlink.out"))) {
		 * dfs.delete(new Path(temp+"/PageRank.outlink.out"), true); }
		 */
		String uriStr = "s3n://" + bucketName + "/";
		URI uri = URI.create(uriStr);
		FileSystem fs = FileSystem.get(uri, new Configuration());
		Path pt = new Path("s3n://" + bucketName + "/result" + "/PageRank.iter"
				+ iter + ".out");
		BufferedReader br1 = new BufferedReader(new InputStreamReader(
				fs.open(pt)));

		FileSystem fs1 = FileSystem.get(uri, new Configuration());

		pt = new Path("s3n://" + bucketName + "/result"
				+ "/PageRank.outlink.out");
		BufferedReader br11 = new BufferedReader(new InputStreamReader(
				fs1.open(pt)));
		String strLine, strLine1;
		HashMap<String, Double> hM = new HashMap<String, Double>();
		String[] tA;
		while ((strLine = br1.readLine()) != null) {
			tA = strLine.split("\t");
			hM.put(tA[0].trim(), Double.parseDouble(tA[1].trim()));
		}

		FSDataOutputStream fsout = dfs.create(new Path(new URI(uriStr
				+ "result/PageRank.outlink1.out")));
		StringBuffer v;
		PrintWriter writer = new PrintWriter(fsout);

		while ((strLine1 = br11.readLine()) != null) {
			String t1 = strLine1.subSequence(0, strLine1.indexOf("    "))
					.toString().trim();
			String t2 = strLine1
					.subSequence(strLine1.indexOf("    "), strLine1.length())
					.toString().trim();
			t2 = t2.subSequence(t2.indexOf("    "), t2.length()).toString()
					.trim();
			writer.append(t1 + "    " + hM.get(t1) + "    " + t2);
			writer.append('\n');
		}
		System.out.println("Before");
		
		
		if (dfs.exists(new Path(new URI(uriStr + "result/PageRank.outlink.out")))) {
			dfs.delete(
					new Path(new URI(uriStr + "result/PageRank.outlink.out")),
					true);
		}
			
				
		
		
		
		System.out.println("After");
		writer.close();
		fs.close();
		fs1.close();
		br1.close();
		br11.close();
		dfs.close();
		
		FileSystem dfs1 = FileSystem.get(outPath.toUri(), new Configuration());
		
		System.out.println(dfs1.rename(new Path(new URI(uriStr + "result/PageRank.outlink1.out")),
				new Path(new URI(uriStr + "result/PageRank.outlink.out"))));
	
		System.out.println("End");
		dfs1.close();
	}

}
