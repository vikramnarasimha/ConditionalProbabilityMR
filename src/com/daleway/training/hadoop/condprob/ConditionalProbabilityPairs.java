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
/**
 * I spent a lot of time having problems with divide / zero errors
 * 
 * I think this went away with the conversion to Writable rather than using 
 * primitives as the instance variables ? Or was it the conversion from IntWritable to Double. 
 * 
 * But i still had problems with all the answers coming out of the reducer
 * as 0 all the time.And I was seeing an extra pass at the end. 
 * 
 * THen I found out that it was actually the Combiner (the extra pass) 
 * that was causing the zeros. So eventually I worked out that my reducer as a combiner was not working.
 * 
 * (I wrongly thought that the issue was being caused by the fact that i was 
 * not explicitly writing a compareto method for the * text.But I wrote a 
 * small test that proved that the sorting was not the issue.)  
 * 
 * THe other problem i am still seeing is that the aggregation i'm not seeing 
 * yet because i am testing on a single node with 1 mapper / reducer ? 
 * Increasing the number of reduce tasks doesnt seem to invoke the aggregation either.
 * 
 * Is conditional probability an accurate measure - count/countN ?? Where there is only 1 word and 1 pair .. the conditional prob will be 1, which is misleading
 * Should we consider case sensitivity ?  
 * What about all double quotes, quotes,exclamation,hyphen, etc - do we need to do some cleansing ?
 * 
 */

package com.daleway.training.hadoop.condprob;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ConditionalProbabilityPairs {
	
	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);		
		private Text pair = new Text();
		private Text orderInversionPair = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			List<String> pairs = getPairs(value.toString());
			for (String aPair : pairs) {
				pair.set(aPair);
				context.write(pair, one);
				StringTokenizer p = new StringTokenizer(aPair, ",");
				if(p.hasMoreTokens()){
					orderInversionPair.set(p.nextToken() + ",*");
					context.write(orderInversionPair, one);					
				}
			}
		}

		private List<String> getPairs(String string) {
			StringTokenizer tokenizer = new StringTokenizer(string);
			List<String> tokens = new ArrayList<String>();
			while (tokenizer.hasMoreTokens()) {
				tokens.add(tokenizer.nextToken());
			}
			List<String> pairs = new ArrayList<String>();
			int index = 0;
			for (String token : tokens) {
				if (index < tokens.size() - 1) {
					pairs.add(clean(token) + "," + clean(tokens.get(index + 1)));
				}
				index += 1;
			}
			return pairs;
		}
		
		private String clean(String token){
			String lower = token.toLowerCase();
			return lower.replaceAll("[^0-9a-zA-Z]", "");
		}		
	}
	


	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, Text> {
		private Text result = new Text();
		private DoubleWritable sumW = new DoubleWritable();
		private Text PFW = new Text();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}

			String x = new String(key.getBytes());
			if (x.contains(",*")) {
				Text thisFW = new Text(x.substring(0, x.indexOf(",*")));
				//System.out.println(thisFW + ", PFW = " + PFW);
				if (thisFW.equals(PFW)) {
					sumW.set(sumW.get() + sum);
					//Doesn't ever seem to get here. 
					//System.out.println("agg = "+ sumW.get());
				} else {
					PFW.set(thisFW);
					sumW.set(sum);
					//System.out.println(sumW.get());
				}
			} else {
				//System.out.println("key = "+ key + " Else PFW = " + PFW + " " + sumW.get());
				result.set((double)sum/sumW.get() +"\t" + sum+ "/"+sumW.get());	
				context.write(key, result);					
			}			
		}
	}
	

	
	public static class ProbDistPartitioner extends
			Partitioner<Text, IntWritable> {

		@Override
		public int getPartition(Text arg0, IntWritable arg1, int arg2) {
			String x= arg0.toString().substring(0, arg0.toString().indexOf(","));
			String t[] = x.split(":");			
			System.out.println("going to reducer : "+ t[0].hashCode() % arg2);
			return Math.abs(t[0].hashCode()) % arg2;
		}

	}
	
	public static Job createJob(Configuration conf, String inputPath, String outputPath) throws IOException{
		Job job = new Job(conf, "pair wise count");
		job.setJarByClass(ConditionalProbabilityPairs.class);
		job.setMapperClass(TokenizerMapper.class);
		//job.setCombinerClass(IntSumReducer.class);
		job.setPartitionerClass(ProbDistPartitioner.class);
		job.setReducerClass(IntSumReducer.class);		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(5);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));		
		return job;
		
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: pairwise <in> <out>");
			System.exit(2);
		}
		System.exit(createJob(conf, otherArgs[0], otherArgs[1]).waitForCompletion(true) ? 0 : 1);
	}
}
