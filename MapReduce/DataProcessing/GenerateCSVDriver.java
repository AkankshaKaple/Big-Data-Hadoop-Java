package com.learn.emp.toCSV;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileSystem;


public class GenerateCSVDriver implements Tool {
	
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
		Job generateCSVJob = new Job(getConf());
		generateCSVJob.setJobName("Data Processing");
		
		generateCSVJob.setJarByClass(GenerateCSVDriver.class);
		
		generateCSVJob.setMapperClass(GenerateCSVMapper.class);
		generateCSVJob.setOutputKeyClass(Text.class);
		generateCSVJob.setOutputValueClass(NullWritable.class);
		
		generateCSVJob.setNumReduceTasks(0);
		
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		FileInputFormat.addInputPath(generateCSVJob, inputPath);
		FileOutputFormat.setOutputPath(generateCSVJob, outputPath);	
		
		
		FileSystem fileSystem = outputPath.getFileSystem(getConf());
		
		fileSystem.delete(outputPath,true);				
		
		int status = generateCSVJob.waitForCompletion(true) ? 0 : -1;		
		
		return status;		
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int status = ToolRunner.run(conf, new GenerateCSVDriver(), args);
		System.out.println("Program for converting data to CSV file : " + status);
		
	}
}
