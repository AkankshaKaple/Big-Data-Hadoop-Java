package com.learn.assignment.GetDeptWiseSummary;

import java.io.IOException;


import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class GetDeptWiseSummaryReducer extends Reducer<Text, Text, Text, NullWritable>{
	@Override
	protected void reduce(Text key, Iterable<Text> records, Context context) throws IOException, InterruptedException {
		
		long total_emp = 0;
		long male_count = 0;
		long female_count = 0;
		long max_sal = 0;
		long min_sal = 0;
		long avg_sal = 0;
		long CTC = 0;
		
	
		 for (Text record : records) { 
			 
			 System.out.println(record);
			 String[] fields = record.toString().split(":");
			 
			 total_emp ++;
			 
			 CTC = CTC + Long.parseLong(fields[4]);
			 		 
			 if(fields[5].trim().equals("female")) {				 
				 female_count++;
			 } 
			 else {
				 male_count++;
			 }
			 	 
			 		 
		 }
		 
		 avg_sal = CTC / total_emp;
		 
		 String result = key + ":" + total_emp + ":" + male_count + ":" + female_count + ":" + CTC + ":" + avg_sal; 
		 context.write(new Text(result), NullWritable.get());
		 
		 System.out.println("Done");
	}

}
