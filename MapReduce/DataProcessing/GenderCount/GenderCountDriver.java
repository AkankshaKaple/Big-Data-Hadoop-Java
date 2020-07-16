package com.learn.emp.GenderCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class GenderCountDriver implements Tool{
	
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
		
		Job genderCountJob = new Job(getConf());
		genderCountJob.setJobName("Gender Count");
		
		genderCountJob.setMapperClass(GenderCountMapper.class);
		genderCountJob.setOutputKeyClass(Text.class);
		genderCountJob.setOutputValueClass(LongWritable.class);
		
		genderCountJob.setReducerClass(GenderCountReducer.class);
		genderCountJob.setOutputKeyClass(Text.class);
		genderCountJob.setOutputValueClass(LongWritable.class);
		
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		FileInputFormat.addInputPath(genderCountJob, inputPath);
		FileOutputFormat.setOutputPath(genderCountJob, outputPath);
		
		FileSystem fileSystem = outputPath.getFileSystem(getConf());
		fileSystem.delete(outputPath, true);
		
		int status = genderCountJob.waitForCompletion(true) ? 0 : 1;
		return status;
		
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		GenderCountDriver genderCountDriver = new GenderCountDriver();
		int status = ToolRunner.run(conf, genderCountDriver, args);
		System.out.println("Gender count job status : "+ status);
	}

}
