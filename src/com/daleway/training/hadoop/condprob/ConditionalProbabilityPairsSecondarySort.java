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
/*
 * I initially thought I had to do the Secondary Sort within the same MapReduce. 
 * The only reason we need the reducer here is for stripping out the composite key. 
 * (Don't think this can be achieved in the mapper)
 */
package com.daleway.training.hadoop.condprob;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * THis Map Reduce implements the secondary sort to sort by the Value, as Map Reduce sorts only the keys
 * @author vikram
 *
 */
public class ConditionalProbabilityPairsSecondarySort {
	
	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer s = new StringTokenizer(value.toString());
			String newKey = s.nextToken();
			String newValue = s.nextToken();
			String log = s.nextToken();
			double val = 0;
			try{
				val = Double.parseDouble(newValue.toString());	
			}catch(NumberFormatException e){}
			context.write(new Text(newKey+":"+val), new Text(val+ "\t" + log));
		}
	}


	public static class IntSumReducer extends
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			for (Text text : values) {
				context.write(new Text(new String(key.getBytes()).split(":")[0]), text);									
			}
		}
	}
	
	public static class KeyComparator extends WritableComparator {
		
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
				Text t1 = (Text) w1;
				Text t2 = (Text) w2;						
				String[] s1 = t1.toString().split(":");
				String[] s2 = t2.toString().split(":");
				String s1key = s1[0].toString().substring(0, s1[0].toString().indexOf(","));					
				String s2key = s2[0].toString().substring(0, s2[0].toString().indexOf(","));					
				int comparison = s1key.compareTo(s2key);
				if(comparison == 0){
					return new Double(Double.parseDouble(s2[1])).compareTo(new Double(Double.parseDouble(s1[1])));
				}
				return comparison;
		}

		protected KeyComparator(){
			super(Text.class, true);
		}
	}
	
	public static class GroupComparator extends WritableComparator {
		
		protected GroupComparator(){
			super(Text.class, true);
		}
		
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			Text t1 = (Text) w1;
			Text t2 = (Text) w2;			
			String s1 = t1.toString().substring(0, t1.toString().indexOf(","));
			String s2 = t2.toString().substring(0, t2.toString().indexOf(","));
			return s1.compareTo(s2);			
		}
	}
	
	public static class ProbDistPartitioner extends
			Partitioner<Text, Text> {

		@Override
		public int getPartition(Text arg0, Text arg1, int arg2) {
			String x= arg0.toString().substring(0, arg0.toString().indexOf(","));
			System.out.println("going to reducer : "+ x.hashCode() % arg2);
			return Math.abs(x.hashCode()) % arg2;
		}

	}

	public static Job createJob(Configuration conf, String inputPath, String outputPath) throws IOException {
		Job job = new Job(conf, "pair wise count");
		job.setJarByClass(ConditionalProbabilityPairsSecondarySort.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setSortComparatorClass(KeyComparator.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		//job.setCombinerClass(IntSumReducer.class);
		job.setPartitionerClass(ProbDistPartitioner.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		//Is the output value class for Map or Reduce ?
		job.setOutputValueClass(Text.class);
		//job.setNumReduceTasks(5);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		return job;		
	}

}
