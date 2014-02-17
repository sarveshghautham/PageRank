package PageRank;
import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

class SortKeyComparator extends WritableComparator {
    
    protected SortKeyComparator() {
        super(DoubleWritable.class, true);
    }
 
    /**    * Compares in the descending order of the keys.     */
    @SuppressWarnings("rawtypes")
	@Override
    public int compare(WritableComparable a, WritableComparable b) {
        DoubleWritable o1 = (DoubleWritable) a;
        DoubleWritable o2 = (DoubleWritable) b;
        if(o1.get() < o2.get()) {
            return 1;
        }else if(o1.get() > o2.get()) {
            return -1;
        }else {
            return 0;
        }
    }
}

public class PageRankSort {
	
	public static long pageCount;

    public static class Map extends Mapper<LongWritable, Text, DoubleWritable, Text> {
        
    	@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    		
            String line = value.toString();
            String []keyValue = line.split("\t");
            System.out.println(line);
            context.write(new DoubleWritable(Double.parseDouble(keyValue[1].trim())), new Text(keyValue[0].trim()));
            
        }
    } 

    public static class Reduce extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

        @Override
		public void reduce(DoubleWritable key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
        	int N = context.getConfiguration().getInt("N", 1);
            for (Text val : values) {
            	if(key.get() >= 5.0/N){
            		context.write(val, key);
            	}
            }
        }
    }

    public void call(String[] args, int iter, int count) throws Exception {
        Configuration conf = new Configuration();
        conf.setInt("N", count);
        Job job = new Job(conf, "pagesort");
        job.setJarByClass(PageCountMR.class);

        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        job.setSortComparatorClass(SortKeyComparator.class);
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
        
        if (dfs.exists(new Path(temp+"/PageRank.iter"+iter+".out"))) {
			dfs.delete(new Path(temp+"/PageRank.iter"+iter+".out"), true);
		}
        dfs.close();
        
        FileSystem dfs1 = FileSystem.get(outPath.toUri(), new Configuration());
        
        dfs1.rename(new Path(outPath+"/part-r-00000"), new Path(temp+"/PageRank.iter"+iter+".out"));
        dfs1.close();
		   
    }

}
