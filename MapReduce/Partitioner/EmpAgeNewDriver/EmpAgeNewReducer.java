package com.learn.partitioner.wc;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EmpAgeNewReducer extends Reducer<Text, NullWritable, Text, NullWritable>{
	
	@Override
	protected void reduce(Text key, Iterable<NullWritable> values, Context context)
			throws IOException, InterruptedException {		
		System.out.println("Reducer ============= " + key);
		context.write(key, NullWritable.get());
	}

}