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
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
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
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;





public class weatherCSV2PNG2Seq extends Configured implements Tool {
	
	public static void main ( String[] args ) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new weatherCSV2PNG2Seq(), args);
		System.exit(res); 
		
	} // End main
	
	public int run ( String[] args ) throws Exception {
		
		String input1 = args[0];    // Input
		String input2 = "Shuo/images/";
		String temp = "Shuo/output_CSV2PNG";       // Round one output
		String out = "Shuo/output_PNG2Seq";

		String matchtable = "Shuo/weatherbins.csv";
		
		int reduce_tasks_1 = 16; 
		int reduce_tasks_2 = 1;
		
		FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(new Path(matchtable))));
        String line;
        String tmpc = "";
        String dwpc = "";
        String roadtmpc = "";
        String pcpn = "";
        String snwd = "";
        String smps = "";
        String drct = "";
        String vsby = "";
        while ((line = br.readLine()) != null) {
        	String firstcolumn = line.split(",")[0];
        	if(!firstcolumn.equals("999"))
        	{
        		String[] bins = line.split(",");
        		
        		if(firstcolumn.equals("0"))
        		{
        			tmpc = bins[1];
        			dwpc = bins[2];
        			roadtmpc = bins[3];
        			pcpn = bins[5];
        			snwd = bins[6];
        			smps = bins[7];
        			drct = bins[8];
        			vsby = bins[9];
        		}
        		else
        		{
        			tmpc = tmpc + "," + bins[1]; 
        			dwpc =  dwpc+ "," + bins[2];
        			roadtmpc =  roadtmpc+ "," + bins[3];
        			pcpn =  pcpn+ "," + bins[5];
        			snwd =  snwd+ "," + bins[6];
        			smps =  smps+ "," + bins[7];
        			drct =  drct+ "," + bins[8];
        			vsby =  vsby+ "," + bins[9];
        		}
        		
        	}
        	          
        }
        br.close();
//		String gids = "74041,74100,87527,86935,85030,87526";
		
		Configuration conf = new Configuration();
		conf.set("tmpc", tmpc);
		conf.set("dwpc", dwpc);
		conf.set("roadtmpc", roadtmpc);
		conf.set("pcpn", pcpn);
		conf.set("snwd", snwd);
		conf.set("smps", smps);
		conf.set("drct", drct);
		conf.set("vsby", vsby);


		Job job_one = new Job(conf, "ShuoweatherCSV2PNG2Seq:1.CSV2PNG"); 	
		job_one.setJarByClass(weatherCSV2PNG2Seq.class); 
		job_one.setNumReduceTasks(reduce_tasks_1);				
		job_one.setMapOutputKeyClass(Text.class); 
		job_one.setMapOutputValueClass(Text.class); 
		job_one.setOutputKeyClass(Text.class);         
		job_one.setOutputValueClass(Text.class);
		job_one.setMapperClass(Map_One.class); 
		job_one.setReducerClass(Reduce_One.class);
		job_one.setInputFormatClass(TextInputFormat.class);  
		job_one.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job_one, new Path(input1)); 
		FileOutputFormat.setOutputPath(job_one, new Path(temp));
		job_one.waitForCompletion(true); 
		
		Job job_two = new Job(conf, "ShuoweatherCSV2PNG2Seq:2.PNG2Seq"); 	
		job_two.setJarByClass(weatherCSV2PNG2Seq.class); 
		job_two.setNumReduceTasks(reduce_tasks_2);				
		//job_two.setMapOutputKeyClass(Text.class); 
		//job_two.setMapOutputValueClass(Text.class); 
		job_two.setOutputKeyClass(Text.class);         
		job_two.setOutputValueClass(BytesWritable.class);
		job_two.setMapperClass(SequenceFileMapper.class); 
		//job_two.setReducerClass(Reduce_Two.class);
		job_two.setInputFormatClass(FullFileInputFormat.class);  
		job_two.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job_two, new Path(input2)); 
		FileOutputFormat.setOutputPath(job_two, new Path(out));
		job_two.waitForCompletion(true);
		
		
		return 0;
	
	} // End run

	public static class Map_One extends Mapper<LongWritable, Text, Text, Text>  {		
		public void map(LongWritable key, Text value, Context context) 
								throws IOException, InterruptedException  {
					
			
			
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
				String pcpn = Double.toString(Math.max(0.0, Double.parseDouble(lines[12])));
				// aggregated by key
				
//				int a = ThreadLocalRandom.current().nextInt(1,  1001);
//				if (a==1) 		
//				{
				context.write(new Text(year+month+day+hour+minute), new Text(id+","+tmpc+","+wawa+","+ptype+","+dwpc
								+","+smps+","+drct+","+vsby+","+roadtmpc+","+srad+","+snwd+","+pcpn));
//				}
			
			
		} // End method "map"
		
	} // End Class Map_One

	public static class Reduce_One extends Reducer<Text, Text, Text, Text>  {		
		public void reduce(Text key, Iterable<Text> values, Context context) 
											throws IOException, InterruptedException  {
			
			System.out.println("Working Directory = " +
		              System.getProperty("user.dir"));
			
			Configuration conf = context.getConfiguration();
			String TMPC = conf.get("tmpc");
			String[] tmpcbins = TMPC.split(",");
			String DWPC = conf.get("dwpc");
			String[] dwpcbins = DWPC.split(",");
			String ROADTMPC = conf.get("roadtmpc");
			String[] roadtmpcbins = ROADTMPC.split(",");
			String PCPN = conf.get("pcpn");
			String[] pcpnbins = PCPN.split(",");
			String SNWD = conf.get("snwd");
			String[] snwdbins = SNWD.split(",");
			String SMPS = conf.get("smps");
			String[] smpsbins = SMPS.split(",");
			String DRCT = conf.get("drct");
			String[] drctbins = DRCT.split(",");
			String VSBY = conf.get("vsby");
			String[] vsbybins = VSBY.split(",");
			
			String time = key.toString();
			int[][] matrix_tmpc = new int[324][660];
			int[][] matrix_dwpc = new int[324][660];
			int[][] matrix_roadtmpc = new int[324][660];
			int[][] matrix_pcpn = new int[324][660];
			int[][] matrix_snwd = new int[324][660];
			int[][] matrix_smps = new int[324][660];
			int[][] matrix_drct = new int[324][660];
			int[][] matrix_vsby = new int[324][660];
			
			int[][] matrix_ptype = new int[324][660];
			
			for (Text val:values)
			{
				String[] line = val.toString().split(",");
				int gid = Integer.parseInt(line[0]);
				int y = 324-(gid-1)/660-1;
				int x = (gid-1)%660;
				
				try{
					double tmpc = Double.parseDouble(line[1]);
					double dwpc = Double.parseDouble(line[4]);
					double roadtmpc = Double.parseDouble(line[8]);
					
					int ptype = Integer.parseInt(line[3]);
					double pcpn = Double.parseDouble(line[11]);
					double snwd = Double.parseDouble(line[10]);
					
					double smps = Double.parseDouble(line[5]);
					int drct = Integer.parseInt(line[6]);
					double vsby = Double.parseDouble(line[7]);
					
					int[] flag = new int[8];
					for(int i=1;i<tmpcbins.length;i++)
					{
						//tmpc
						if (flag[0]==0 & tmpc<=Double.parseDouble(tmpcbins[i]))
						{
							flag[0]=i;
						}
						else if(flag[0]==0 & i==tmpcbins.length-1)
						{
							flag[0]=i;
						}
						//dmpc
						if (flag[1]==0 & dwpc<=Double.parseDouble(dwpcbins[i]))
						{
							flag[1]=i;
						}
						else if(flag[1]==0 & i==tmpcbins.length-1)
						{
							flag[1]=i;
						}
						//roadtmpc
						if (flag[2]==0 & roadtmpc<=Double.parseDouble(roadtmpcbins[i]))
						{
							flag[2]=i;
						}
						else if(flag[2]==0 & i==tmpcbins.length-1)
						{
							flag[2]=i;
						}
						//pcpn
						if (flag[3]==0 & pcpn<=Double.parseDouble(pcpnbins[i]))
						{
							flag[3]=i;
						}
						else if(flag[3]==0 & i==tmpcbins.length-1)
						{
							flag[3]=i;
						}
						//snwd
						if (flag[4]==0 & snwd<=Double.parseDouble(snwdbins[i]))
						{
							flag[4]=i;
						}
						else if(flag[4]==0 & i==tmpcbins.length-1)
						{
							flag[4]=i;
						}
						//smps
						if (flag[5]==0 & smps<=Double.parseDouble(smpsbins[i]))
						{
							flag[5]=i;
						}
						else if(flag[5]==0 & i==tmpcbins.length-1)
						{
							flag[5]=i;
						}
						//drct
						if (flag[6]==0 & drct<=Double.parseDouble(drctbins[i]))
						{
							flag[6]=i;
						}
						else if(flag[6]==0 & i==tmpcbins.length-1)
						{
							flag[6]=i;
						}
						//vsby
						if (flag[7]==0 & vsby<=Double.parseDouble(vsbybins[i]))
						{
							flag[7]=i;
						}
						else if(flag[7]==0 & i==tmpcbins.length-1)
						{
							flag[7]=i;
						}
					}
					
					matrix_tmpc[y][x] = flag[0];
					matrix_dwpc[y][x] = flag[1];
					matrix_roadtmpc[y][x] = flag[2];
					matrix_pcpn[y][x] = flag[3];
					matrix_snwd[y][x] = flag[4];
					matrix_smps[y][x] = flag[5];
					matrix_drct[y][x] = flag[6];
					matrix_vsby[y][x] = flag[7];
					
					if (ptype == -3){matrix_ptype[y][x] = 254;}
					else if (ptype == 0){matrix_ptype[y][x] = 255;}
					else {matrix_ptype[y][x] = ptype;}
					
				}
				catch (Exception e){e.printStackTrace();}
			}
			
			try {
			    BufferedImage image1 = new BufferedImage(660, 324, BufferedImage.TYPE_INT_RGB);
			    for(int i=0; i<matrix_tmpc.length; i++) {
			        for(int j=0; j<matrix_tmpc[i].length; j++) {
			            int R = matrix_tmpc[i][j];
			            //int G = matrix_tmpc[i][j];
			            //int B = matrix_tmpc[i][j];
			            int G = matrix_dwpc[i][j];
			            int B = matrix_roadtmpc[i][j];
			            Color newColor = new Color(R,G,B);
			            image1.setRGB(j,i,newColor.getRGB());
			        }
			    }
			    
			    BufferedImage image2 = new BufferedImage(660, 324, BufferedImage.TYPE_INT_RGB);
			    for(int i=0; i<matrix_tmpc.length; i++) {
			        for(int j=0; j<matrix_tmpc[i].length; j++) {
			            int R = matrix_pcpn[i][j];
			            //int G = matrix_pcpn[i][j];
			            //int B = matrix_pcpn[i][j];
			            int G = matrix_ptype[i][j];
			            int B = matrix_snwd[i][j];
			            Color newColor = new Color(R,G,B);
			            image2.setRGB(j,i,newColor.getRGB());
			        }
			    }
			    
			    BufferedImage image3 = new BufferedImage(660, 324, BufferedImage.TYPE_INT_RGB);
			    for(int i=0; i<matrix_tmpc.length; i++) {
			        for(int j=0; j<matrix_tmpc[i].length; j++) {
			            int R = matrix_vsby[i][j];
			            //int G = matrix_smps[i][j];
			            //int B = matrix_smps[i][j];
			            int G = matrix_smps[i][j];
			            int B = matrix_drct[i][j];
			            Color newColor = new Color(R,G,B);
			            image3.setRGB(j,i,newColor.getRGB());
			        }
			    }
			    
			    // get DataBufferBytes from Raster
//			    WritableRaster raster = image1.getRaster();
//			    DataBufferByte data   = (DataBufferByte) raster.getDataBuffer();
//			    context.write(new Text("TEMP" + time +".png"), new BytesWritable(data.getData()));
		    
			    File output1 = new File("/hadoop/yarn/local/usercache/team/appcache/TEMP" + time +".png");
			    File output2 = new File("/hadoop/yarn/local/usercache/team/appcache/PREC" + time +".png");
			    File output3 = new File("/hadoop/yarn/local/usercache/team/appcache/WIND" + time +".png");
			    //File output = new File("/home/team/" + time +".png");
			    
			    ImageIO.write(image1, "png", output1);
			    ImageIO.write(image2, "png", output2);
			    ImageIO.write(image3, "png", output3);
			    System.out.println("write png into /hadoop/yarn/local/usercache/team/appcache/");
			    //System.out.println("write png into /home/team/");
		
			    FileSystem hdfs =FileSystem.get(conf);
			    hdfs.copyFromLocalFile(new Path("/hadoop/yarn/local/usercache/team/appcache/TEMP" + time +".png"),
			    		new Path("Shuo/images"));
			    hdfs.copyFromLocalFile(new Path("/hadoop/yarn/local/usercache/team/appcache/PREC" + time +".png"),
			    		new Path("Shuo/images"));
			    hdfs.copyFromLocalFile(new Path("/hadoop/yarn/local/usercache/team/appcache/WIND" + time +".png"),
			    		new Path("Shuo/images"));
			    System.out.println("copy png into Shuo/images");
			    
//		        try{
//		        	byte[] data = Files.readAllBytes((java.nio.file.Path) new Path("/hadoop/yarn/local/usercache/team/appcache/TEMP" + time +".png"));
//
//		        	context.write(new Text("TEMP" + time +".png"), new BytesWritable(data));
//		        }catch (Exception e) {
//		            System.out.println("Exception MESSAGES = "+e.getMessage());
//		        }
			    
			    File f_TEMP = new File("/hadoop/yarn/local/usercache/team/appcache/TEMP" + time +".png");
			    boolean bool_TEMP = f_TEMP.delete();
			    System.out.println("File deleted: "+bool_TEMP);
			    File f_PREC = new File("/hadoop/yarn/local/usercache/team/appcache/PREC" + time +".png");
			    boolean bool_PREC = f_PREC.delete();
			    System.out.println("File deleted: "+bool_PREC);
			    File f_WIND = new File("/hadoop/yarn/local/usercache/team/appcache/WIND" + time +".png");
			    boolean bool_WIND = f_WIND.delete();
			    System.out.println("File deleted: "+bool_WIND);
			    
			    
			}

			catch(Exception e) {e.printStackTrace();}
			
			
		} // End method "reduce" 
		
	} // End Class Reduce_One
	
	
	
	public static class SequenceFileMapper extends
	Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
	private Text filename;
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		InputSplit split = context.getInputSplit();
		Path path = ((FileSplit) split).getPath();
		
		String rawfilename = path.toString();
		String[] file = rawfilename.split("/");
		String name = file[file.length-1];
		
		filename = new Text(name);
	}
	
	@Override
	protected void map(NullWritable key, BytesWritable value,
			Context context) throws IOException, InterruptedException {
		context.write(filename, value);
	}
	}
 	
	public static class FullFileInputFormat extends
	FileInputFormat<NullWritable, BytesWritable> {
	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		return false; 
	}
	
	@Override
	public RecordReader<NullWritable, BytesWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		FullFileRecordReader reader = new FullFileRecordReader();
		reader.initialize(split, context);
		return reader;
	}
	}

	
	public static class FullFileRecordReader extends RecordReader<NullWritable, BytesWritable> {
		private FileSplit fileSplit;
		private Configuration conf;
		private BytesWritable value = new BytesWritable();
		private boolean processed = false;

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			this.fileSplit = (FileSplit) split;
			this.conf = context.getConfiguration();
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (!processed) {
				byte[] contents = new byte[(int) fileSplit.getLength()];
				Path file = fileSplit.getPath();
				FileSystem fs = file.getFileSystem(conf);
				FSDataInputStream in = null;
				try {
					in = fs.open(file);
					IOUtils.readFully(in, contents, 0, contents.length);
					value.set(contents, 0, contents.length);
				} finally {
					IOUtils.closeStream(in);
				}
				processed = true;
				return true;
			}
			return false;
		}

		@Override
		public NullWritable getCurrentKey() throws IOException, InterruptedException {
			return NullWritable.get();
		}

		@Override
		public BytesWritable getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException {
			return processed ? 1.0f : 0.0f;
		}

		@Override
		public void close() throws IOException {
			// do nothing
		}
	}
	
}
 	
 	
 	
	


