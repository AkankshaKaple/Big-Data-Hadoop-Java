package com.learn.emp.SortByDept;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortByDeptReducer extends Reducer<Text, Text, Text, NullWritable>{
	

	@Override
	protected void reduce(Text key, Iterable<Text> records, Context context) throws IOException, InterruptedException {
		
		System.out.println("key == " + key + "  value ==" + records);
		
		 for (Text record : records) {
			 context.write(record, NullWritable.get());
		 }
		

	}
	

}
