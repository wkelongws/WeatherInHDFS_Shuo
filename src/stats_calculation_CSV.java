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
import java.math.RoundingMode;
import java.text.DecimalFormat;
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




public class stats_calculation_CSV extends Configured implements Tool {
	
	public static void main ( String[] args ) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new stats_calculation_CSV(), args);
		System.exit(res); 
		
	} // End main
	
	public int run ( String[] args ) throws Exception {
		
		String input = args[0];    // Input
		String temp = "Shuo/output_spe";       // Round one output
		//String temp1 = "/scr/shuowang/lab3/exp2/temp1/";     // Round two output
		//String output1 = "/scr/shuowang/lab3/exp2/output1/";   // Round three/final output
		//String output2 = "/scr/shuowang/lab3/exp2/output2/";   // Round three/final output
		String matchtable = "Shuo/TMC_TT_TableToExcel.csv";
		
		int reduce_tasks = 16;  // The number of reduce tasks that will be assigned to the job
		
//		FileSystem fs = FileSystem.get(new Configuration());
//        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(new Path(matchtable))));
//        String line;
//        String gids = "";
//        while ((line = br.readLine()) != null) {
//        	gids = gids + "," + line.split(",")[4];            
//        }
//        br.close();
		String gids = "74041,74100,87527,86935,85030,87526";
		
		Configuration conf = new Configuration();
		conf.set("gids", gids);

		Job job_one = new Job(conf, "ShuoJSONRecodReader"); 	

		job_one.setJarByClass(stats_calculation_CSV.class); 

		job_one.setNumReduceTasks(reduce_tasks);			
		
		job_one.setMapOutputKeyClass(Text.class); 
		job_one.setMapOutputValueClass(Text.class); 
		job_one.setOutputKeyClass(NullWritable.class);         
		job_one.setOutputValueClass(Text.class);

		job_one.setMapperClass(Map_One.class); 
		job_one.setReducerClass(Reduce_One.class);

		job_one.setInputFormatClass(TextInputFormat.class);  
		
		job_one.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job_one, new Path(input)); 
		// FileInputFormat.addInputPath(job_one, new Path(another_input_path)); // This is legal
		FileOutputFormat.setOutputPath(job_one, new Path(temp));
		// FileOutputFormat.setOutputPath(job_one, new Path(another_output_path)); // This is not allowed

		job_one.waitForCompletion(true); 

		return 0;
	
	} // End run

	public static class Map_One extends Mapper<LongWritable, Text, Text, Text>  {		
		public void map(LongWritable key, Text value, Context context) 
								throws IOException, InterruptedException  {
					
			Configuration conf = context.getConfiguration();
			String gid = conf.get("gids");
			String[] gids = gid.split(",");
			
			List<String> Gids = Arrays.asList(gids);
			
			String[] lines = value.toString().split(",");
			
			String timestamp = lines[0];
			String date = timestamp.split(" ")[0];
			String time = timestamp.split(" ")[1];
			String year = date.split("-")[0];
			String month = date.split("-")[1];
			String day = date.split("-")[2];
			String hour = time.split(":")[0];
			String minute = time.split(":")[1];
			
			String id = lines[1];
			if(Gids.contains(id))
			{
				String tmpc = lines[2];
				String wawa = lines[3];
				String ptype = lines[4];
				String dwpc = lines[5];
				String smps = lines[6];
				String drct = lines[7];
				String vsby = lines[8];
				String roadtmpc = lines[9];
				String srad = lines[10];
				String snwd = lines[11];
				String pcpn = lines[12];
				// aggregated by key
						
				context.write(new Text(year+month+day+","+hour+","+id), new Text(tmpc+","+wawa+","+ptype+","+dwpc
								+","+smps+","+drct+","+vsby+","+roadtmpc+","+srad+","+snwd+","+pcpn));
				
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
			double drct_sum = 0.0;
			double drct2_sum=0.0;
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
			
			String[] wawa_table = 
				{
					"AS.Y",
					"EH.A",
					"EC.W",
					"FA.A",
					"EH.W",
					"HT.Y",
					"FZ.W",
					"FR.Y",
					"FW.A",
					"FW.W",
					"FZ.A",
					"HZ.W",
					"WS.A",
					"BZ.A",
					"SV.A",
					"TO.A",
					"FL.A",
					"FL.S",
					"WC.A",
					"FL.Y",
					"HW.A",
					"WC.W",
					"FL.W",
					"BS.Y",
					"WI.Y",
					"WC.Y",
					"FA.W",
					"FA.Y",
					"FF.A",
					"FF.W",
					"FG.Y",
					"HW.W",
					"SN.Y",
					"SB.Y",
					"WW.Y",
					"SV.W",
					"HS.W",
					"WS.W",
					"ZF.Y",
					"ZR.Y",
					"BZ.W",
					"TO.W",
					"IS.W"
				};
			String[] ptype_table = 
				{
					"-3",
					"0",
					"1",
					"2",
					"3",
					"4",
					"5",
					"6",
					"7",
					"8",
					"9",
					"10",
					"91",
					"96"
				};
			
			int[] wawa_counter = new int[wawa_table.length];
			int[] ptype_counter = new int[ptype_table.length];
			
			
			for (Text val : values) {
				String[] data =val.toString().split(",");
				counter ++;
				tmpc_sum += Double.parseDouble(data[0]);
				tmpc2_sum +=  Math.pow(Double.parseDouble(data[0]), 2);
				dwpc_sum += Double.parseDouble(data[3]);
				dwpc2_sum +=  Math.pow(Double.parseDouble(data[3]), 2);
				smps_sum += Double.parseDouble(data[4]);
				smps2_sum +=  Math.pow(Double.parseDouble(data[4]), 2);
				drct_sum += Double.parseDouble(data[5]);
				drct2_sum +=  Math.pow(Double.parseDouble(data[5]), 2);
				vsby_sum += Double.parseDouble(data[6]);
				vsby2_sum +=  Math.pow(Double.parseDouble(data[6]), 2);
				roadtmpc_sum += Double.parseDouble(data[7]);
				roadtmpc2_sum +=  Math.pow(Double.parseDouble(data[7]), 2);
				srad_sum += Double.parseDouble(data[8]);
				srad2_sum +=  Math.pow(Double.parseDouble(data[8]), 2);
				snwd_sum += Double.parseDouble(data[9]);
				snwd2_sum +=  Math.pow(Double.parseDouble(data[9]), 2);
				pcpn_sum += Math.max(0.0, Double.parseDouble(data[10]));
				pcpn2_sum +=  Math.pow(Math.max(0.0,Double.parseDouble(data[10])), 2);
				
				String[] wawa_raw=data[1].split(";");
				for (int i=0;i<wawa_raw.length;i++)
				{
					String wawa = wawa_raw[i].replaceAll("[\\[\\]\" ]", "");
					for (int j=0;j<wawa_table.length;j++)
					{
						if (wawa_table[j].equals(wawa))
						{
							wawa_counter[j]++;
						}
					}
					
				}
				String ptype = data[2];
				for (int j=0;j<ptype_table.length;j++)
				{
					if (ptype_table[j].equals(ptype))
					{
						ptype_counter[j]++;
					}
				}
				
			}
			
			double tmpc_avg = tmpc_sum/counter;
			double tmpc_var = tmpc2_sum/counter-Math.pow(tmpc_avg, 2);
			double dwpc_avg = dwpc_sum/counter;
			double dwpc_var = dwpc2_sum/counter-Math.pow(dwpc_avg, 2);
			double smps_avg = smps_sum/counter;
			double smps_var = smps2_sum/counter-Math.pow(smps_avg, 2);
			double drct_avg = drct_sum/counter;
			double drct_var = drct2_sum/counter-Math.pow(drct_avg, 2);
			double vsby_avg = vsby_sum/counter;
			double vsby_var = vsby2_sum/counter-Math.pow(vsby_avg, 2);
			double roadtmpc_avg = roadtmpc_sum/counter;
			double roadtmpc_var = roadtmpc2_sum/counter-Math.pow(roadtmpc_avg, 2);
			double srad_avg = srad_sum/counter;
			double srad_var = srad2_sum/counter-Math.pow(srad_avg, 2);
			double snwd_avg = snwd_sum/counter;
			double snwd_var = snwd2_sum/counter-Math.pow(snwd_avg, 2);
			double pcpn_avg = 12*pcpn_sum/counter;
			double pcpn_var = 12*(pcpn2_sum/counter-Math.pow(pcpn_avg/12, 2));
			
			String wawa_summary = "";
			String ptype_summary = "";
			
			for(int i=0;i<ptype_counter.length;i++)
			{
				ptype_summary+=ptype_counter[i]+",";
			}
			
			for(int i=0;i<wawa_counter.length-1;i++)
			{
				wawa_summary+=wawa_counter[i]+",";
			}
			wawa_summary+=wawa_counter[wawa_counter.length-1];
			
			DecimalFormat df = new DecimalFormat("#.##");
			df.setRoundingMode(RoundingMode.HALF_UP);
			
			context.write(NullWritable.get(),new Text(
							key.toString()+","+
							df.format(tmpc_avg)+","+
							df.format(tmpc_var)+","+
							df.format(dwpc_avg)+","+
							df.format(dwpc_var)+","+
							df.format(smps_avg)+","+
							df.format(smps_var)+","+
							df.format(drct_avg)+","+
							df.format(drct_var)+","+
							df.format(vsby_avg)+","+
							df.format(vsby_var)+","+
							df.format(roadtmpc_avg)+","+
							df.format(roadtmpc_var)+","+
							df.format(srad_avg)+","+
							df.format(srad_var)+","+
							df.format(snwd_avg)+","+
							df.format(snwd_var)+","+
							df.format(pcpn_avg)+","+
							df.format(pcpn_var)+","+
							ptype_summary+wawa_summary
							));
		} // End method "reduce" 
		
	} // End Class Reduce_One
 	
}
 	
 	
 	
	


