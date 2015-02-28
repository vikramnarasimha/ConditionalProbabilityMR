/**
 *  * Licensed to the Apache Software Foundation (ASF) under one
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


package com.daleway.training.hadoop.condprob;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ConditionalProbabilityStripes {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, Text> {

		private final static IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			Map<String, Map<String,Integer>> stripes = getStripes((value.toString()));
			Iterator keys = stripes.keySet().iterator();
			while(keys.hasNext()){
				String stripeKey= (String) keys.next();	
				Map<String, Integer> stripe = stripes.get(stripeKey);
				Iterator valuekeys = stripe.keySet().iterator();
				String values = "";
				while(valuekeys.hasNext()){
					String k = (String) valuekeys.next();
					String kv =  k + ":" + stripe.get(k);
					values += (values.equals("")?"":",") + kv; 									
				}
				context.write(new Text(stripeKey), new Text(values));	
			}
		}

		private Map<String,Map<String,Integer>> getStripes(String string) {
			Map<String, Map<String,Integer>> stripes = new HashMap<String, Map<String, Integer>>(); 
			List<String> pairs = getPairs(string);			
			for (String aPair : pairs) {
				if(aPair.split(",").length > 1){
					String key = aPair.split(",")[0];
					String value = aPair.split(",")[1];
					if (stripes.get(key) == null) {
						stripes.put(key, new HashMap<String, Integer>());
					}
					HashMap<String, Integer> stripe = (HashMap<String, Integer>) stripes.get(key);
					if(stripe.get(value) == null){
						stripe.put(value, new Integer(1));
					}else{
						stripe.put(value, new Integer(stripe.get(value)+ 1));
					}					
				}
			}
			return stripes;
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

		private String clean(String token) {
			String lower = token.toLowerCase();
			return lower.replaceAll("[^0-9a-zA-Z]", "");
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, Text, Text, Text> {

		private Text PFW = new Text();
		private IntWritable sumW = new IntWritable(0);
		private Map<String, Integer> map = new HashMap<String, Integer>();
	
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			
			Text thisFW = new Text(key.toString());			
			if(!thisFW.toString().equals(PFW.toString())){
				map = new HashMap<String,Integer>();
			}			

			int sum = 0;
			for (Text val : values) {				
				String[] t = val.toString().split(",");
				for (int i = 0; i < t.length; i++){ 
					if(t[i].contains(":")){
						String[] v = t[i].split(":");						
						sum += Integer.parseInt(v[1]);
						if(map.get(v[0]) != null){
							Integer x = (Integer) map.get(v[0]);
							map.put(v[0], x + Integer.parseInt(v[1]));
						}else{
							map.put(v[0], new Integer(Integer.parseInt(v[1])));
						}
					}						
				}
			}
			
			if(!thisFW.toString().equals(PFW.toString())){
				PFW.set(thisFW);
				sumW.set(sum);
			}else{
				sumW.set(sumW.get() + sum);
			}
			
			Set<String> keys = map.keySet();				
			for (String val : keys) {
				System.out.println("val = " + map.get(val) + " sum = " + sumW.get());
				double condprob = 0;
				if(sumW.get() > 0){					
					condprob = (double)map.get(val)/sumW.get();
					context.write(new Text(key.toString() + "," + val), new Text(""+condprob + "\t" + map.get(val)+ "/"+sumW.get()));
				}else{
					context.write(new Text(key.toString() + "," + val), new Text("NA" + "\t" + map.get(val)+ "/"+sumW.get()));
				}
			}			
		}
	}

	public static class ProbDistPartitioner extends
			Partitioner<Text, Text> {

		@Override
		public int getPartition(Text arg0, Text arg1, int arg2) {
			return Math.abs(arg0.hashCode())  % arg2;
		}

	}

	public static Job createJob(Configuration conf, String inputPath, String outputPath) throws IOException {
		Job job = new Job(conf, "pair wise count");
		job.setJarByClass(ConditionalProbabilityStripes.class);
		job.setMapperClass(TokenizerMapper.class);
		// job.setCombinerClass(IntSumReducer.class);
		job.setPartitionerClass(ProbDistPartitioner.class);
		job.setReducerClass(IntSumReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);		
		
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