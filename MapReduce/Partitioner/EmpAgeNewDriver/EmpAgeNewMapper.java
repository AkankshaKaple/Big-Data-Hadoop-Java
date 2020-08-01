package com.learn.partitioner.wc;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EmpAgeNewMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
	
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		
		System.out.println("Mapper : " + value);
		
		
		context.write(value, NullWritable.get());
		
	}
	
	

}