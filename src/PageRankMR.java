import java.io.IOException;
import java.util.*;

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
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.File;
import java.io.PrintWriter;

public class PageRankMR {
	
	public static long N;
	
	static HashMap<String, StringBuffer> sHM = new HashMap<String, StringBuffer>();
	static StringBuffer temp, temp1;
	static int c1, c2 = 0;
	
    public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        
    	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//    		String mapperKey = "N=";
            String line = value.toString();
            
            String[] mapperKey = line.split("    ");
            double currentPR = Double.parseDouble(mapperKey[1]);
            int externalLinks = mapperKey.length - 2;
            double prFraction = currentPR/externalLinks;
            // for the HashMap
            int offset = value.toString().indexOf("    ") + 3;
            sHM.put(mapperKey[0], new StringBuffer(value.toString()).insert(offset, "_|_|"));
            
            for( int i = 0; i < mapperKey.length; i ++ ){
//            	if(!mapperKey[i].trim().equals(""))
            	if( i != 1 )
            		context.write(new Text(mapperKey[i].trim()), new DoubleWritable(prFraction));
            }
        }
    } 

    public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    	
    	
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
            throws IOException, InterruptedException {
            double sum = 0;
            int offset1, offset2, count = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            
            double pR = (1 - 0.85)/N  + 0.85 * sum;
            
            //modify HashMap
//            temp1 = new StringBuffer(key.toString());
            StringBuffer sBTemp = sHM.get(key.toString());
            	if(sBTemp != null){
            		c1++;
            	offset1 = sBTemp.indexOf("   _|_| ");
            	temp1 = new StringBuffer(sBTemp.substring(0, offset1));
//            	if(key.toString().equals(temp1.toString())){
            		offset2 = sBTemp.indexOf("    ");
            		sBTemp = sBTemp.replace(offset1, offset2, "    "+pR+"");
            		sHM.put(key.toString(), sBTemp);
            		context.write(key, new DoubleWritable(pR));
            	}
            	else{
            		System.out.println(key.toString());
            	}
//            context.write(key, new DoubleWritable(sum));
        }
    }

    public void call(String[] args, long pageCount, int iter) throws Exception {
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
        if(iter == 1 || iter == 8){
        if (dfs.exists(new Path("result/PageRank.iter" + iter + ".out"))) {
			dfs.delete(new Path("result/PageRank.iter" + iter + ".out"), true);
		}
        
        dfs.rename(new Path(outPath+"/part-r-00000"), new Path("result/PageRank.iter" + iter + ".out"));
        }
//        StringBuffer v;
        
        
        if (dfs.exists(new Path("result/PageRank.inlink.out"))) {
			dfs.delete(new Path("result/PageRank.inlink.out"), true);
		}
        
        FSDataOutputStream fsout = dfs.create(new Path("result/PageRank.inlink.out"));
        StringBuffer v;
        PrintWriter writer = new PrintWriter(fsout);
        int start, count = 1;
        for (String k : sHM.keySet())
        {
        	v = sHM.get(k);
            writer.append(v);
            if(count != sHM.size())
            	writer.append('\n');
            count++;
        }
        writer.close();
    }

}
