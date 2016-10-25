import java.io.*;
import java.lang.*;
import java.util.*;
import java.net.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class testJSON {

	public static void main(String[] args) throws IOException {
		String line = "    {\"gid\": 1, \"tmpc\": 5.40, \"wawa\": [], \"ptype\": 0, \"dwpc\": -3.10, \"smps\": 9.3, "
				+ "\"drct\": 340, \"vsby\": 16.093, \"roadtmpc\": 11.80,\"srad\": 206.60, \"snwd\": 0.00, \"pcpn\": -12.00},";
		
		System.out.println(line);
		
		
		//line = "{\"time\": \"2015\",";
		
		String[] lines = line.split(",");
		System.out.println(lines.length);
		for (int i=0;i<lines.length;i++)
		{
			System.out.println(lines[i]);
		}
		
		line = " \"wawa\": []";
		
		lines = line.split(": ");
		System.out.println(lines.length);
		for (int i=0;i<lines.length;i++)
		{
			System.out.println(lines[i]);
		}
		

	}

}
