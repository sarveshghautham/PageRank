import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PageRankMR {
	
	public static long N;

    public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        
    	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//    		String mapperKey = "N=";
            String line = value.toString();
            String[] mapperKey = line.split("    ");
            double currentPR = Double.parseDouble(mapperKey[1]);
            int externalLinks = mapperKey.length - 3;
            double prFraction = currentPR/externalLinks;
            for( int i = 2; i < mapperKey.length; i ++ ){
            context.write(new Text(mapperKey[i].trim()), new DoubleWritable(prFraction));
            }
        }
    } 

    public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
            throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            
            double pR = (1 - 0.85)/N  + 0.85 * sum;
            context.write(key, new DoubleWritable(sum));
        }
    }

    public void call(String[] args, long pageCount) throws Exception {
        Configuration conf = new Configuration();
        N = pageCount;
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
        
        job.waitForCompletion(true);
        
        if (dfs.exists(new Path("result/PageRank.iter1.out"))) {
			dfs.delete(new Path("result/PageRank.iter1.out"), true);
		}
        
        dfs.rename(new Path(outPath+"/part-r-00000"), new Path("result/PageRank.iter1.out"));
		
    }

}
