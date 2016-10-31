/**
  *****************************************
  *****************************************
  * by Shuo Wang **
  *****************************************
  *****************************************
  */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;




public class weatherJSON2CSV extends Configured implements Tool {
	
	public static void main ( String[] args ) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new weatherJSON2CSV(), args);
		System.exit(res); 
		
	} // End main
	
	public int run ( String[] args ) throws Exception {
		
		String input = args[0];    // Input
		String temp = "Shuo/output";       // Round one output
		//String temp1 = "/scr/shuowang/lab3/exp2/temp1/";     // Round two output
		//String output1 = "/scr/shuowang/lab3/exp2/output1/";   // Round three/final output
		//String output2 = "/scr/shuowang/lab3/exp2/output2/";   // Round three/final output
//		String matchtable = "Shuo/TMC_TT_TableToExcel.csv";
//		
		int reduce_tasks = 1;  // The number of reduce tasks that will be assigned to the job
//		
//		FileSystem fs = FileSystem.get(new Configuration());
//        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(new Path(matchtable))));
//        String line;
//        String gids = "";
//        while ((line = br.readLine()) != null) {
//        	gids = gids + "," + line.split(",")[4];            
//        }
//        br.close();
		
		
		Configuration conf = new Configuration();
//		conf.set("mapred.child.java.opts", "-Xmx4096m");
//		conf.set("mapreduce.map.memory.mb", "-Xmx4096m");
//		conf.set("mapreduce.reduce.memory.mb", "-Xmx4096m");
		conf.set("textinputformat.record.delimiter","}]}\n");
//		conf.set("gids", gids);

		Job job_one = new Job(conf, "ShuoJSONRecodReader"); 	

		job_one.setJarByClass(weatherJSON2CSV.class); 

		job_one.setNumReduceTasks(reduce_tasks);			
		
		//job_one.setMapOutputKeyClass(Text.class); 
		//job_one.setMapOutputValueClass(Text.class); 
		job_one.setOutputKeyClass(NullWritable.class);         
		job_one.setOutputValueClass(Text.class);

		job_one.setMapperClass(Map_One.class); 
		//job_one.setReducerClass(Reduce_One.class);

		job_one.setInputFormatClass(TextInputFormat.class);  
		
		job_one.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job_one, new Path(input)); 
		// FileInputFormat.addInputPath(job_one, new Path(another_input_path)); // This is legal
		FileOutputFormat.setOutputPath(job_one, new Path(temp));
		// FileOutputFormat.setOutputPath(job_one, new Path(another_output_path)); // This is not allowed

		job_one.waitForCompletion(true); 

		return 0;
	
	} // End run

	public static class Map_One extends Mapper<LongWritable, Text, NullWritable, Text>  {		
		public void map(LongWritable key, Text value, Context context) 
								throws IOException, InterruptedException  {
					
//			Configuration conf = context.getConfiguration();
//			String gid = conf.get("gids");
//			String[] gids = gid.split(",");
//			
//			List<String> Gids = Arrays.asList(gids);
			
			String[] lines = value.toString().split("\n");
			String timestamp = lines[0].split("\"")[3];
			String date = timestamp.split("T")[0];
			String time = timestamp.split("T")[1];
			String year = date.split("-")[0];
			String month = date.split("-")[1];
			String day = date.split("-")[2];
			String hour = time.split(":")[0];
			String minute = time.split(":")[1];
			
			for (int i=5;i<lines.length;i++)
			{
				String[] line = lines[i].trim().split(",");
				
				String id = line[0].split(": ")[1];
//				if(Gids.contains(id))
//				{
					if(line.length==12)
					{
						String tmpc = line[1].split(": ")[1];
						String wawa = line[2].split(": ")[1];
						String ptype = line[3].split(": ")[1];					
						String dwpc = line[4].split(": ")[1];
						String smps = line[5].split(": ")[1];
						String drct = line[6].split(": ")[1];
						String vsby = line[7].split(": ")[1];
						String roadtmpc = line[8].split(": ")[1];
						String srad = line[9].split(": ")[1];
						String snwd = line[10].split(": ")[1];
						String pcpn = line[11].split(": ")[1].split("}")[0];
						// aggregated by key
						context.write(NullWritable.get(), new Text(date+" "+hour+":"+minute+","+id+","+tmpc+","+wawa+","+ptype+","+dwpc
								+","+smps+","+drct+","+vsby+","+roadtmpc+","+srad+","+snwd+","+pcpn));
					}
					if(line.length>=13)
					{
						int l =line.length;
						
						String tmpc = line[1].split(": ")[1];
						
						String wawa_raw="";
						for(int j =2;j<l-10;j++){wawa_raw+=line[j]+";";}
						//String wawa = (line[2]+";"+line[3]).split(": ")[1];
						String wawa = (wawa_raw+line[l-10]).split(": ")[1];
						
						String ptype = line[l-9].split(": ")[1];					
						String dwpc = line[l-8].split(": ")[1];
						String smps = line[l-7].split(": ")[1];
						String drct = line[l-6].split(": ")[1];
						String vsby = line[l-5].split(": ")[1];
						String roadtmpc = line[l-4].split(": ")[1];
						String srad = line[l-3].split(": ")[1];
						String snwd = line[l-2].split(": ")[1];
						String pcpn = line[l-1].split(": ")[1].split("}")[0];
						// aggregated by key
						context.write(NullWritable.get(), new Text(date+" "+hour+":"+minute+","+id+","+tmpc+","+wawa+","+ptype+","+dwpc
								+","+smps+","+drct+","+vsby+","+roadtmpc+","+srad+","+snwd+","+pcpn));
					}
					
//				}
				
				
			}				
		} // End method "map"
		
	} // End Class Map_One

	public static class Reduce_One extends Reducer<Text, Text, NullWritable, Text>  {		
		public void reduce(Text key, Iterable<Text> values, Context context) 
											throws IOException, InterruptedException  {
			
			int counter = 0;
			double tmpc_sum = 0.0;
			double tmpc2_sum = 0.0;
			double dwpc_sum = 0.0;
			double dwpc2_sum = 0.0;
			double smps_sum = 0.0;
			double smps2_sum = 0.0;
			double vsby_sum = 0.0;
			double vsby2_sum = 0.0;
			double roadtmpc_sum = 0.0;
			double roadtmpc2_sum = 0.0;
			double srad_sum = 0.0;
			double srad2_sum = 0.0;
			double snwd_sum = 0.0;
			double snwd2_sum = 0.0;
			double pcpn_sum = 0.0;
			double pcpn2_sum = 0.0;
			
			
			for (Text val : values) {
				String[] data =val.toString().split(",");
				counter ++;
				tmpc_sum += Double.parseDouble(data[0]);
				tmpc2_sum +=  Math.pow(Double.parseDouble(data[0]), 2);
				dwpc_sum += Double.parseDouble(data[3]);
				dwpc2_sum +=  Math.pow(Double.parseDouble(data[3]), 2);
				smps_sum += Double.parseDouble(data[4]);
				smps2_sum +=  Math.pow(Double.parseDouble(data[4]), 2);
				vsby_sum += Double.parseDouble(data[6]);
				vsby2_sum +=  Math.pow(Double.parseDouble(data[6]), 2);
				roadtmpc_sum += Double.parseDouble(data[7]);
				roadtmpc2_sum +=  Math.pow(Double.parseDouble(data[7]), 2);
				srad_sum += Double.parseDouble(data[8]);
				srad2_sum +=  Math.pow(Double.parseDouble(data[8]), 2);
				snwd_sum += Double.parseDouble(data[9]);
				snwd2_sum +=  Math.pow(Double.parseDouble(data[9]), 2);
				pcpn_sum += Double.parseDouble(data[10].split("}")[0]);
				pcpn2_sum +=  Math.pow(Double.parseDouble(data[10].split("}")[0]), 2);
			}
			
			double tmpc_avg = tmpc_sum/counter;
			double tmpc_var = tmpc2_sum/counter-Math.pow(tmpc_avg, 2);
			double dwpc_avg = dwpc_sum/counter;
			double dwpc_var = dwpc2_sum/counter-Math.pow(dwpc_avg, 2);
			double smps_avg = smps_sum/counter;
			double smps_var = smps2_sum/counter-Math.pow(smps_avg, 2);
			double vsby_avg = vsby_sum/counter;
			double vsby_var = vsby2_sum/counter-Math.pow(vsby_avg, 2);
			double roadtmpc_avg = roadtmpc_sum/counter;
			double roadtmpc_var = roadtmpc2_sum/counter-Math.pow(roadtmpc_avg, 2);
			double srad_avg = srad_sum/counter;
			double srad_var = srad2_sum/counter-Math.pow(srad_avg, 2);
			double snwd_avg = snwd_sum/counter;
			double snwd_var = snwd2_sum/counter-Math.pow(snwd_avg, 2);
			double pcpn_avg = pcpn_sum/counter;
			double pcpn_var = pcpn2_sum/counter-Math.pow(pcpn_avg, 2);
			
			
			context.write(NullWritable.get(),new Text(key.toString()+","+tmpc_avg+","+tmpc_var+","+dwpc_avg+","+dwpc_var
					+","+smps_avg+","+smps_var+","+vsby_avg+","+vsby_var+","+roadtmpc_avg+","+roadtmpc_var
					+","+srad_avg+","+srad_var+","+snwd_avg+","+snwd_var+","+pcpn_avg+","+pcpn_var));
		} // End method "reduce" 
		
	} // End Class Reduce_One
 	
}
 	
 	
 	
	


