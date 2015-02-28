package com.daleway.training.hadoop.condprob;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

public class Main {
	
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf1 = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf1, args)
				.getRemainingArgs();
		
		if(otherArgs.length < 4){
			System.err.println("Usage: Main <type=pairs|stripes> <in> <temp> <out>");
			System.exit(2);			
		}
		
		Job job1 = null; 
		if(otherArgs[0].equalsIgnoreCase("pairs")){
			job1 = ConditionalProbabilityPairs.createJob(conf1, otherArgs[1], otherArgs[2]);
		}else if(otherArgs[0].equalsIgnoreCase("stripes")){
			job1 = ConditionalProbabilityStripes.createJob(conf1, otherArgs[1], otherArgs[2]);
		}else{
			System.err.println("Usage: Main <type=pairs|stripes> <in> <temp> <out>");
			System.exit(2);			
		}
		
		Job job2 = null;
		if(job1.waitForCompletion(true)){
			Configuration conf2 = new Configuration();
			job2 = ConditionalProbabilityPairsSecondarySort.createJob(conf2, otherArgs[2], otherArgs[3]);
		}else{
			System.exit(1);
		}
		
		Job job3 = null;
		if(job2.waitForCompletion(true)){
			Configuration conf2 = new Configuration();
			job3 = ConditionalProbabilityPairWordExtractor.createJob(conf2, otherArgs[3], otherArgs[3] +"-for");
		}else{
			System.exit(1);
		}
		
		if(!job3.waitForCompletion(true)){
			System.exit(1);
		}
		
		Job job4 = null;
		if(job3.waitForCompletion(true)){
			Configuration conf2 = new Configuration();
			job4 = ConditionalProbabilityPairsSecondarySort.createJob(conf2, otherArgs[3] +"-for",otherArgs[3] +"-for-ss");
		}else{
			System.exit(1);
		}
		
		if(!job4.waitForCompletion(true)){
			System.exit(1);
		}		
	}

}