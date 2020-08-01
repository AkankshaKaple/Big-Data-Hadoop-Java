package com.learn.partitioner.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class EmpAgeNewDriver implements Tool{
	
	private Configuration conf=null;

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		// TODO Auto-generated method stub
		this.conf=conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		
		Job  empAgePartitionJob = new Job(getConf());
		
		empAgePartitionJob.setJarByClass(this.getClass());
		
		
		empAgePartitionJob.setMapperClass(EmpAgeNewMapper.class);
		
		empAgePartitionJob.setMapOutputKeyClass(Text.class);
		
		empAgePartitionJob.setMapOutputValueClass(NullWritable.class);
		
		
		empAgePartitionJob.setPartitionerClass(EmpAgeNewPartitioner.class);
		
		
		empAgePartitionJob.setReducerClass(EmpAgeNewReducer.class);
		
		empAgePartitionJob.setOutputKeyClass(Text.class);
		
		empAgePartitionJob.setOutputValueClass(NullWritable.class);
		
		empAgePartitionJob.setNumReduceTasks(5);
		
		
		empAgePartitionJob.setInputFormatClass(TextInputFormat.class);
		
		empAgePartitionJob.setOutputFormatClass(TextOutputFormat.class);
		
		
		Path inputPath = new Path(args[0]);
		
		Path outputPath = new Path(args[1]);
		
		FileInputFormat.addInputPath(empAgePartitionJob, inputPath);
		
		FileOutputFormat.setOutputPath(empAgePartitionJob, outputPath);
		
		
		FileSystem fileSystem = outputPath.getFileSystem(getConf());
		
		fileSystem.delete(outputPath,true);				
		
		int status = empAgePartitionJob.waitForCompletion(true) ? 0 : -1;		
		
		return status;
	}
	
	public static void main(String[] args) throws Exception {		
		
		int status = ToolRunner.run(new Configuration(), new EmpAgeNewDriver(),args);	
		
		System.out.println("EmpAge Partitioning Job status "+status);
		
	}

}