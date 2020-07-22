package com.learn.assignment.GetDeptWiseSummary;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class GetDeptWiseSummaryMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		
		String[] fields = line.split(":");
		System.out.println("Mapper output");
		System.out.println("key == " + fields[3] + "  value ==" + value.toString());
		
		context.write(new Text(fields[3]), new Text(value));
	}

}