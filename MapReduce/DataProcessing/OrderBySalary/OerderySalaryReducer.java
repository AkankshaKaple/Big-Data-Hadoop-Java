package com.learn.emp.OrderBySalary;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OerderySalaryReducer extends Reducer<LongWritable, Text, Text, NullWritable> {
	
	@Override
	protected void reduce(LongWritable salary, Iterable<Text> records, Context context)
			throws IOException, InterruptedException {
		
		for (Text record : records) {
				
			context.write(record, NullWritable.get());
		}
		
		
	}
	
}
