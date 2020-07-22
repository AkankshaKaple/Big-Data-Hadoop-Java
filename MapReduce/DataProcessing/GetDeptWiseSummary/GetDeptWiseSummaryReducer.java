package com.learn.assignment.GetDeptWiseSummary;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GetDeptWiseSummaryReducer extends Reducer<Text, Text, Text, NullWritable>{
	@Override
	protected void reduce(Text key, Iterable<Text> records, Context context) throws IOException, InterruptedException {
		
		long total_emp = 0;
		long male_count = 0;
		long female_count = 0;
		Long max_sal = 0L;
		Long min_sal = 0L;
		long CTC = 0;
		
		
		Iterator<Text> record = records.iterator();
		
		int i = 0;
		while(record.hasNext()) {
			
			Text temp = record.next();

			String[] fields = temp.toString().split(":");
			
			if (fields.length >= 4) {
			long sal = Long.parseLong(fields[4]);
			
			if (i == 0) {
				max_sal = sal;
				min_sal = sal;
				i++;
			}			
			if(sal < min_sal) {
				min_sal = sal;				
			}else if(sal > max_sal) {
				max_sal = sal;
				
			}
			
			total_emp ++;
			 
			 CTC = CTC + Long.parseLong(fields[4]);
			 		 
			 if(fields[5].trim().equals("female")) {				 
				 female_count++;
			 } 
			 else {
				 male_count++;
			 }
						
		}
			
	}
		long avg_sal = CTC / total_emp;
		
		String result =  key + ":" + total_emp + ":" + male_count + ":" + female_count + ":" + max_sal + ":" + min_sal + ":" + + avg_sal + ":" + CTC;	
		context.write(new Text(result), NullWritable.get());
	}

}
