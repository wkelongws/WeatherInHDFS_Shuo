import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;
import javax.imageio.ImageIO;
import javax.swing.JFrame;
import java.lang.*;
import java.lang.reflect.Array;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
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
		
		
//		int[][] yourmatrix = new int[][]{
//			  { 0, 255, 133 },
//			  { 139, 67, 200 },
//			  { 200, 170, 100 },
//			  { 50, 50, 230 },
//			  { 70, 110, 0 }
//			};
			int gid = 213840;
		int y = 324-(gid-1)/660;
		int x = (gid-1)%660+1;
		System.out.println(y+","+x);
			
//		try {
//		    BufferedImage image = new BufferedImage(3, 5, BufferedImage.TYPE_INT_RGB);
//		    for(int i=0; i<yourmatrix.length; i++) {
//		        for(int j=0; j<yourmatrix[i].length; j++) {
//		            int a = yourmatrix[i][j];
//		            Color newColor = new Color(a,a,a);
//		            image.setRGB(j,i,newColor.getRGB());
//		        }
//		    }
//		    File output = new File("/Users/Shuo/Desktop/GrayScale.png");
//		    ImageIO.write(image, "png", output);
//		}
//
//		catch(Exception e) {e.printStackTrace();}
		
		
//		BufferedImage image = new BufferedImage(100, 100, BufferedImage.TYPE_INT_RGB); 
//		 Graphics g = image.getGraphics(); 
//		 g.drawString("Hello World!!!", 10, 20); 
//		 try {  
//		   ImageIO.write(image, "jpg", new File("c:/CustomImage.jpg")); 
//		 } catch (IOException e) {  
//		  e.printStackTrace(); 
//		 }
	}

}
