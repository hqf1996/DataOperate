package com.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobFactor {

	public static Job createJob(String jobName, Configuration conf,Class<?> jarClass, Class<? extends Mapper<?, ?, ?, ?>> mapClass,
			Class<? extends Reducer<?, ?, ?, ?>> combinerClass, Class<? extends Reducer<?, ?, ?, ?>> reduceClass, Class<?> mapOutputKeyClass,
			Class<?> mapOutputValueClass, Class<?> outputKeyClass, Class<?> outputValueClass,  String... pathArgs) throws IOException{
		Job job = createJob(jobName, pathArgs, conf, jarClass);
		job.setMapperClass(mapClass);
		if(combinerClass != null){
			job.setCombinerClass(combinerClass);
		}
		if(reduceClass != null){
			job.setReducerClass(reduceClass);
		}
		if(mapOutputKeyClass != null && mapOutputValueClass != null){
			job.setMapOutputKeyClass(mapOutputKeyClass);
			job.setMapOutputValueClass(mapOutputValueClass);
		}
		job.setOutputKeyClass(outputKeyClass);
		job.setOutputValueClass(outputValueClass);
		for(int i = 0;i<pathArgs.length-1;i++){
			FileInputFormat.addInputPath(job, new Path(pathArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(pathArgs[pathArgs.length-1]));
		return job;
	} 
	
	public static Job createJob(String jobName, Configuration conf,Class<?> jarClass, 
			Class<? extends Mapper<?, ?, ?, ?>> mapClass, Class<? extends Reducer<?, ?, ?, ?>> reduceClass, 
			Class<?> outputKeyClass, Class<?> outputValueClass, String... pathArgs) throws IOException{
		return createJob(jobName, conf, jarClass, mapClass, null, reduceClass, null, null, outputKeyClass, outputValueClass, pathArgs);
	}
	
	public static Job createJob(String jobName, Configuration conf,Class<?> jarClass,
			Class<? extends Mapper<?, ?, ?, ?>> mapClass,Class<? extends Reducer<?, ?, ?, ?>> combinerClass, 
			Class<? extends Reducer<?, ?, ?, ?>> reduceClass, Class<?> outputKeyClass, Class<?> outputValueClass,  String... pathArgs) throws IOException{
		return createJob(jobName, conf, jarClass, mapClass, combinerClass, reduceClass, null, null, outputKeyClass, outputValueClass, pathArgs);
	}
	
	public static Job createJob(String jobName, Configuration conf,Class<?> jarClass,
			Class<? extends Mapper<?, ?, ?, ?>> mapClass,Class<? extends Reducer<?, ?, ?, ?>> reduceClass,
			Class<?> mapOutputKeyClass,Class<?> mapOutputValueClass, Class<?> outputKeyClass, Class<?> outputValueClass,  String... pathArgs) throws IOException{
		return createJob(jobName, conf, jarClass, mapClass, null, reduceClass, mapOutputKeyClass, mapOutputValueClass, outputKeyClass, outputValueClass, pathArgs);
	}
	
	public static Job createJob(String jobName, Configuration conf, Class<?> jarClass, Class<? extends Mapper<?, ?, ?, ?>> mapClass,
			Class<?> outputKeyClass,Class<?> outputValueClass,  String... pathArgs) throws IOException{
		return createJob(jobName, conf, jarClass, mapClass, null, null, null, null, outputKeyClass, outputValueClass, pathArgs);
	}
	
	private static Job createJob(String jobName, String[] pathArgs, Configuration conf,Class<?> jarClass) throws IOException{
		if (pathArgs.length < 2){
			System.err.println("Usage: Data Deduplication <in> <out>");
			return null;
		}
		Job job  = Job.getInstance(conf, jobName);
		job.setJarByClass(jarClass);
		return job;
	}
}
