package com.learn.emp.SortByDept;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortByDeptMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		
		String[] fields = line.split(":");
		System.out.println("key == " + key + "  value ==" + value);
		context.write(new Text(fields[3]), new Text(value));
	}

}
