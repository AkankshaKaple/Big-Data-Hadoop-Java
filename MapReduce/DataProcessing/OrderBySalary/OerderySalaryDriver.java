package com.learn.emp.OrderBySalary;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class OerderySalaryDriver implements Tool{
	
	private Configuration conf = null;

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		
		Job orderByJob = new Job(getConf());
		orderByJob.setJobName("Order by salary");
		
		orderByJob.setJarByClass(this.getClass());
		
		
		
		orderByJob.setMapperClass(OerderySalaryMapper.class);
		
		orderByJob.setMapOutputKeyClass(LongWritable.class);
		
		orderByJob.setMapOutputValueClass(Text.class);
		
		
		
		orderByJob.setReducerClass(OerderySalaryReducer.class);
		
		orderByJob.setOutputKeyClass(Text.class);
		
		orderByJob.setOutputValueClass(NullWritable.class);
		
	
			
		
		orderByJob.setInputFormatClass(TextInputFormat.class);
		
		orderByJob.setOutputFormatClass(TextOutputFormat.class);
		
		
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		FileInputFormat.addInputPath(orderByJob, inputPath);
		FileOutputFormat.setOutputPath(orderByJob, outputPath);
		
		FileSystem fileSystem = outputPath.getFileSystem(getConf());
		fileSystem.delete(outputPath, true);
		
		int status = orderByJob.waitForCompletion(true) ? 0 : -1;
		return status;
		
		
		
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int status = ToolRunner.run(conf, new OerderySalaryDriver(), args);
		System.out.println("Status of Order by salay :: " + status);
	}
	

}
