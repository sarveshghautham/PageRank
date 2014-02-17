package PageRank;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PageCountMR {
	
	public static long pageCount;

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        
    	@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    		String mapperKey = "N=";
            String line = value.toString();
            String []lines = line.split(System.getProperty("line.separator"));
            if(lines[lines.length - 1].trim().equals("")){
            	context.write(new Text(mapperKey), new IntWritable(lines.length - 1));
            }else{
            	context.write(new Text(mapperKey), new IntWritable(lines.length));
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
            throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            pageCount = sum;
            context.write(key, new IntWritable(sum));
        }
    }

    public void call(String[] args, String bucketName) throws Exception {
        Configuration conf = new Configuration();
        
        Job job = new Job(conf, "pagecount");
        job.setJarByClass(PageCountMR.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

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

        String temp = outPath.toString().replace("1", "");
        
        if (dfs.exists(new Path(outPath+"/PageRank.n.out"))) {
			dfs.delete(new Path(outPath+"/PageRank.n.out"), true);
		}
        
        dfs.rename(new Path(outPath+"/part-r-00000"), new Path(temp+"/PageRank.n.out"));
        
        dfs.close();
		
        String uriStr = "s3n://" + bucketName + "/"; 
		URI uri = URI.create(uriStr); 
		FileSystem fs = FileSystem.get(uri, new Configuration());
		Path pt=new Path("s3n://" + bucketName + "/result/PageRank.n.out");
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
        String line=br.readLine();
		br.close();
        fs.close();
        int pCount = Integer.parseInt(line.split("=")[1].trim());
        
        PageRankMR o = new PageRankMR();
        String arg[] = {temp+"/PageRank.outlink.out", outPath+""}; 
        for(int iter = 1; iter < 9; iter++){
        	o.call(arg, pCount, iter, bucketName);
        	
        }
        
        String arg1[] = {temp+"/PageRank.iter1.out", outPath.toString()};
        PageRankSort sortPr = new PageRankSort();
        sortPr.call(arg1, 1, pCount);
        
        arg1[0] = temp+"/PageRank.iter8.out";
        sortPr = new PageRankSort();
        sortPr.call(arg1, 8, pCount);

        
        
        
        

    }

}
