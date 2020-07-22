package com.learn.emp.OrderBySalary;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OerderySalaryMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	protected void map(LongWritable key, Text value, Context context) throws java.io.IOException ,InterruptedException {
		
		String line = value.toString();
		String[] fields = line.split(":");
			
		context.write(new LongWritable(Long.valueOf(fields[4])),new Text(line));
		
	}
}
