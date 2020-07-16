package com.learn.emp.GenderCount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GenderCountMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

	protected void map(LongWritable key, Text value, Context context) throws java.io.IOException ,InterruptedException {
		String line = value.toString();
		String[] fields = line.split(":");
		context.write(new Text(fields[5]), new LongWritable(1));
	}
}
