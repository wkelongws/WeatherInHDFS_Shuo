/**
  *****************************************
  *****************************************
  * by Shuo Wang **
  *****************************************
  *****************************************
  */

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.awt.image.WritableRaster;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;




public class Seq2PNG extends Configured implements Tool {
	
	public static void main ( String[] args ) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new Seq2PNG(), args);
		System.exit(res); 
		
	} // End main
	
	public int run ( String[] args ) throws Exception {
		
		String input = args[0];    // Input
		String temp = "Shuo/output_Seq2PNG";       // Round one output
		
		int reduce_tasks = 1;  // The number of reduce tasks that will be assigned to the job
		
		Configuration conf = new Configuration();


		Job job_one = new Job(conf, "ShuoSeq2PNG"); 	

		job_one.setJarByClass(Seq2PNG.class); 

		job_one.setNumReduceTasks(reduce_tasks);			
		
		job_one.setMapOutputKeyClass(Text.class); 
		job_one.setMapOutputValueClass(BytesWritable.class); 
		job_one.setOutputKeyClass(Text.class);         
		job_one.setOutputValueClass(Text.class);

		job_one.setMapperClass(Map_One.class); 
		job_one.setReducerClass(Reduce_One.class);

		job_one.setInputFormatClass(SequenceFileInputFormat.class);  
		
		job_one.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job_one, new Path(input)); 
		// FileInputFormat.addInputPath(job_one, new Path(another_input_path)); // This is legal
		FileOutputFormat.setOutputPath(job_one, new Path(temp));
		// FileOutputFormat.setOutputPath(job_one, new Path(another_output_path)); // This is not allowed

		job_one.waitForCompletion(true); 

		return 0;
	
	} // End run

	public static class Map_One extends Mapper<Text, BytesWritable, Text, BytesWritable>  {		
		public void map(Text key, BytesWritable value, Context context) 
								throws IOException, InterruptedException  {
			
			String rawfilename = key.toString();

			String[] name = rawfilename.split("/");
			String filename = name[name.length-1];
			if (filename.equals("TEMP201607312135.png"))
			{
				context.write(new Text(filename),value);
			}
			
		} // End method "map"
		
	} // End Class Map_One

	public static class Reduce_One extends Reducer<Text, BytesWritable, Text, Text>  {		
		public void reduce(Text key, Iterable<BytesWritable> values, Context context) 
											throws IOException, InterruptedException  {
			
			Configuration conf = context.getConfiguration();
			for (BytesWritable val:values)
			{
				byte [] data = val.getBytes();
				InputStream in = new ByteArrayInputStream(data);
				BufferedImage bImageFromConvert = ImageIO.read(in);

				ImageIO.write(bImageFromConvert, "png", new File(
						"/hadoop/yarn/local/usercache/team/appcache/" + key.toString()));
				
				FileSystem hdfs =FileSystem.get(conf);
				hdfs.copyFromLocalFile(new Path("/hadoop/yarn/local/usercache/team/appcache/" + key.toString()),
				    		new Path("Shuo/images_query"));
				File f_TEMP = new File("/hadoop/yarn/local/usercache/team/appcache/" + key.toString());
				boolean bool_TEMP = f_TEMP.delete();
				System.out.println("File deleted: "+bool_TEMP);
			}
			
		} // End method "reduce" 
		
	} // End Class Reduce_One
 	
}
 	
 	
 	
	


