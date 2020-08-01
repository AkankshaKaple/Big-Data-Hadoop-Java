package com.learn.partitioner.GenderBasedOnAge;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GenderBasedOnAgeReducer extends Reducer<Text, Text, Text, NullWritable>{
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {		
		System.out.println("Reducer ============= " + key);
		
		for (Text text : values) {
			System.out.println("Text ::: " + text);
			context.write(text, NullWritable.get());
		}
		
	}
}
