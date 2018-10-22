/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Modified by Jingqwen Shi to demonstrate functionality for Homework 2
// April-May 2015
import java.util.*;
import java.lang.String;
import java.lang.Double;
import java.io.IOException;
import java.util.StringTokenizer;
import java.math.RoundingMode;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Hw2Part1{

	public static class NewArrayWritable extends ArrayWritable{  
    			public NewArrayWritable() {   
         		super(DoubleWritable.class);   
       			}   
			public NewArrayWritable(DoubleWritable[] doubleArr) {   
         		super(DoubleWritable.class);

			set(doubleArr);   
       			}  

	}  

	public static class CountAvgMapper
		extends Mapper<Object, Text, Text, NewArrayWritable>{
		private final static DoubleWritable one = new DoubleWritable(1);
		private Text word = new Text();

		public void map(Object key,Text value,Context context)
			throws IOException, InterruptedException{
                        String[] arrLines = value.toString().split("/n");

			for (int i =0;i<arrLines.length;i++){
                                String[] singleLine = arrLines[i].split(" ");
				String reg = "^[0-9]+(.[0-9]+)?$";

                                if(singleLine.length!=3){}  
				else if(!singleLine[2].matches(reg)){}//test uncorrect line
				else if (Double.valueOf(singleLine[2]) < 0){}
				else{ 
					DoubleWritable time = new DoubleWritable(Double.valueOf(singleLine[2]));
					String strKey = singleLine[0]+" "+singleLine[1];
					word.set(strKey);    
					DoubleWritable[] doubleArr = new DoubleWritable[2];
					doubleArr[0] = one;
					doubleArr[1] = time; 
			                NewArrayWritable arrValue = new NewArrayWritable(doubleArr);			
					context.write(word,arrValue);		
				}
				
			}	
		}
	}
	
	public static class CountAvgCombiner
		extends Reducer<Text, NewArrayWritable, Text, NewArrayWritable>{
		private NewArrayWritable result = new NewArrayWritable();
			
		public void reduce(Text key,Iterable<NewArrayWritable> values,Context context)
			throws IOException,InterruptedException{
                        double resCount = Double.valueOf(0);
			double resAvg = Double.valueOf(0);			
 
			for (NewArrayWritable val : values){
				resCount += Double.valueOf(val.get()[0].toString()); 
				resAvg += Double.valueOf(val.get()[1].toString());
			}
			DoubleWritable[] doubleArr = {new DoubleWritable(resCount), new DoubleWritable(resAvg)};
			NewArrayWritable arrValue = new NewArrayWritable(doubleArr);		

			context.write(key, arrValue);
		} 		
	}

	public static class CountAvgReducer extends Reducer<Text, NewArrayWritable, Text, Text>{
		private Text result_key = new Text();
		private Text result_value = new Text();


		public void reduce(Text key, Iterable<NewArrayWritable> values, Context context) 
			throws IOException, InterruptedException {			

			double [] res = {Double.valueOf(0), Double.valueOf(0)}; 
			for (NewArrayWritable val : values){
				res[0] += Double.valueOf(val.get()[0].toString()); 
				res[1] += Double.valueOf(val.get()[1].toString());
			}
		        DecimalFormat df0 = new DecimalFormat("#");
		 	String strCount = String.valueOf(df0.format(res[0]));


			DecimalFormat df1 = new DecimalFormat("###0.000");
		 	df1.setRoundingMode(RoundingMode.HALF_UP);
		 	String strAvg = String.valueOf(df1.format( res[1]/res[0]));

                        String strRes = " "+strCount+" "+strAvg;

			//generate result valse
			result_value.set(strRes);
			
			context.write(key, result_value);
		}
	}

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        	if(otherArgs.length < 2){
			System.err.println("Usage:Hw2Part1 <in> [<in>...]<out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf,"Hw2Part1");

   		job.setJarByClass(Hw2Part1.class);

		job.setMapperClass(CountAvgMapper.class);
		job.setCombinerClass(CountAvgCombiner.class);
		job.setReducerClass(CountAvgReducer.class);

		job.setMapOutputKeyClass(Text.class);
  		job.setMapOutputValueClass(NewArrayWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//append the input paths as given by command line
  		for (int i = 0;i < otherArgs.length - 1; ++i){
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}

		//append the output path as given by command line
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		
		System.exit(job.waitForCompletion(true)? 0 : 1);
	}
}


