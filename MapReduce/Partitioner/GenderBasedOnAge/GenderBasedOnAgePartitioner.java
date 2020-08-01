package com.learn.partitioner.GenderBasedOnAge;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class GenderBasedOnAgePartitioner extends Partitioner<Text, Text>{

	@Override
	public int getPartition(Text key, Text value, int noOfReducers) {
		
		System.out.println("Partitioner :: " + key + "  " + value);
		
		String[] fields = value.toString().split(":");
		int age = Integer.parseInt(fields[6]);
		
		if(noOfReducers == 0) {
			return -1;
		}
		
		else if(age < 20) {
			return 0%noOfReducers;
		}
		
		else if(age < 30) {
			return 1%noOfReducers;
		}
		
		else if(age < 40) {
			System.out.println("Return 2");
			return 2%noOfReducers;
		}
		
		else if(age < 50) {
			return 3%noOfReducers;
		}
		else {
			return 4%noOfReducers;
		}
		
	}
	
	

}
