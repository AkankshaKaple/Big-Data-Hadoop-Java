// Gender grouping based on age

package com.learn.partitioner.GenderBasedOnAge;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GenderBasedOnAgeMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	protected void map(LongWritable key, Text value, Context context) throws java.io.IOException ,InterruptedException {
		
		String[] fields = value.toString().split(":");
		System.out.println("Mapper ===== " + fields[5] + " == " + value);
		context.write(new Text(fields[5]), value);
		
	}

}
