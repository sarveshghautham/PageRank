import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.Reader;
import java.io.StringReader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

class parserXmlMap extends Mapper<LongWritable, Text, Text, Text> {
	
   @Override
   public void map(LongWritable key, Text value1,Context context) throws IOException, InterruptedException {

	   String xmlString = value1.toString();
       SAXBuilder builder = new SAXBuilder();
       Reader in = new StringReader(xmlString);
       
       try {
    	  
           Document doc = builder.build(in);
           Element root = doc.getRootElement();
           
           String link="";
           String pageTitle =root.getChild("title").getTextTrim() ;            
           String pageText =root.getChild("revision").getChild("text").getTextTrim();
           pageTitle = pageTitle.replace(' ', '_');
           
           /* Pattern to extract text inside "[[ ]]" */
   			//Pattern pattern = Pattern.compile("\\[\\[.*?\\]\\]");
            Pattern pattern = Pattern.compile("\\[\\[.*?\\]\\]");
   			Matcher matcher = pattern.matcher(pageText);
   			String[] tempArray;
   			while (matcher.find()) {
//   				link = matcher.group(0).replace('[', ' ').replace(']', ' ').trim();
   				link = matcher.group(0).substring(2, matcher.group(0).length() - 2);
   				
   				if(link.contains("[[")){
   					tempArray = link.split("\\[\\[");
   					link = tempArray[tempArray.length - 1]; 
   				}else if(link.contains("]]")){
   					tempArray = link.split("\\]\\]");
   					link = tempArray[0]; 
   				}
	   			/* Fetching the first page when a '|' occurs */
	   			int pipePos = link.indexOf('|');
	   			if(pipePos >= 0){
	   				link = (link.substring(0, pipePos));
	   			}
	   			/* Replacing spaces with '_' */
	   			link = link.replace(' ', '_');
	   			/* Excluding interwiki links, section linking and table row linking */
	   			if ( link.indexOf(':') < 0  && link.indexOf('#') < 0) {
	   				context.write(new Text(pageTitle), new Text(link));
	   			}
	   			link = "";
   		}
           
       } catch (JDOMException ex) {
           Logger.getLogger(parserXmlMap.class.getName()).log(Level.SEVERE, null, ex);
       } catch (IOException ex) {
           Logger.getLogger(parserXmlMap.class.getName()).log(Level.SEVERE, null, ex);
       }
   
   }

}

/* TODO: Reducer class coming */
class parserXmlRed extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) 
        throws IOException, InterruptedException {
        String output="";
        for (Text val : values) {
            output += val.toString()+"    ";
        }
        output += '\n';
        context.write(key, new Text(output));
    }
}

public class parseXMLMapRed {

	public static void main(String[] args) throws IOException {
	
		Configuration conf = new Configuration();
	
		conf.set("xmlinput.start", "<page>");
		conf.set("xmlinput.end", "</page>");
	
		Job job = new Job(conf, "xmlParsing");
		job.setJarByClass(parseXMLMapRed.class);
		job.setMapperClass(parserXmlMap.class);
		job.setReducerClass(parserXmlRed.class);
		job.setNumReduceTasks(1);
		job.setInputFormatClass(XmlInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		Path outPath = new Path(args[1]);
		FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileSystem dfs = FileSystem.get(outPath.toUri(), conf);
		if (dfs.exists(outPath)) {
			dfs.delete(outPath, true);
		}
	
		try {
			job.waitForCompletion(true);
		} 
		catch (InterruptedException ex) {
			Logger.getLogger(parseXMLMapRed.class.getName()).log(Level.SEVERE, null, ex);
		} 
		catch (ClassNotFoundException ex) {
			Logger.getLogger(parseXMLMapRed.class.getName()).log(Level.SEVERE, null, ex);
		}

	}
}
