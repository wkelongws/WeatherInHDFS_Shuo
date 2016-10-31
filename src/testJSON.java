import java.io.*;
import java.lang.*;
import java.lang.reflect.Array;
import java.math.RoundingMode;
import java.util.*;
import java.net.*;
import java.text.DecimalFormat;

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
		
		String xx = "10";
		String x = xx.split("}")[0];
		
		
		System.out.println(x);
		
	}

}
