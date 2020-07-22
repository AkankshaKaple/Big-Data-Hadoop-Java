package com.learn.assignment.GetDeptWiseSummary;


	

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class GetDeptWiseSummaryDriver implements Tool {
		
	private Configuration conf = null;
	@Override
	public Configuration getConf() {

		return conf;
}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}
	
	@Override
	public int run(String[] args) throws Exception {
		
		
		Job  groupByDeptJob = new Job(getConf());
		
		groupByDeptJob.setJobName("Group By Dept");
		
		
		groupByDeptJob.setJarByClass(this.getClass());
		
		
		
		groupByDeptJob.setMapperClass(GetDeptWiseSummaryMapper.class);
		
		groupByDeptJob.setMapOutputKeyClass(Text.class);
		
		groupByDeptJob.setMapOutputValueClass(Text.class);
		
		
		
		groupByDeptJob.setReducerClass(GetDeptWiseSummaryReducer.class);
		
		groupByDeptJob.setOutputKeyClass(Text.class);
		
		groupByDeptJob.setOutputValueClass(NullWritable.class);
		
	
			
		
		groupByDeptJob.setInputFormatClass(TextInputFormat.class);
		
		groupByDeptJob.setOutputFormatClass(TextOutputFormat.class);
		
		
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		FileInputFormat.addInputPath(groupByDeptJob, inputPath);
		FileOutputFormat.setOutputPath(groupByDeptJob, outputPath);
		
		FileSystem fileSystem = outputPath.getFileSystem(getConf());
		fileSystem.delete(outputPath, true);
		
		int status = groupByDeptJob.waitForCompletion(true) ? 0 : -1;
		return status;
		
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		GetDeptWiseSummaryDriver genderCountDriver = new GetDeptWiseSummaryDriver();
		int status = ToolRunner.run(conf, genderCountDriver, args);
		System.out.println("Gender count job status : "+ status);
		}

}

	
	
	


