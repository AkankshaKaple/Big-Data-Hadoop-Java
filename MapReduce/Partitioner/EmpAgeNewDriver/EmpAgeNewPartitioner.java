package com.learn.partitioner.wc;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class EmpAgeNewPartitioner extends Partitioner<Text, NullWritable>{

	@Override
	public int getPartition(Text key, NullWritable value, int noOfReducers) {
		// TODO Auto-generated method stub						5
		
		System.out.println("Partitioner  " + key );
				
		String empRecord =	key.toString();
		
		String[] fields  = empRecord.split(":");		
		
		int age = Integer.parseInt(fields[6]);
		
		System.out.println("age :::: " + age);
		
		
		if(noOfReducers==0)
		{
			return -1;
		}
		
		if(age<20 ){
			
			System.out.println("P : " + 0);
			
			return 0%noOfReducers;
		}
		else if (age<30){
			System.out.println("P : " + 1);
			
			return 1%noOfReducers;
		}
		else if (age<40){
			System.out.println("P : " + 2);
			
			
			return 2%noOfReducers;
		}
		else if (age<50){
			
			System.out.println("P : " + 3);
			
			
			return 3%noOfReducers;
		}
		else{
			
			System.out.println("P : " + 4);
			
			
			return 4%noOfReducers;
		}
		
	}
	
}