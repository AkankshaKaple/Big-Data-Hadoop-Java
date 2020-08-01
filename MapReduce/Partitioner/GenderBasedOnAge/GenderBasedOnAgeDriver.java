// Gender grouping based on age

package com.learn.partitioner.GenderBasedOnAge;

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

import com.learn.partitioner.wc.EmpAgeNewMapper;
import com.learn.partitioner.wc.EmpAgeNewPartitioner;
import com.learn.partitioner.wc.EmpAgeNewReducer;


public class GenderBasedOnAgeDriver implements Tool{
	
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
		Job  genderBasedOnAge = new Job(getConf());
		
		genderBasedOnAge.setJarByClass(this.getClass());
		
		
		genderBasedOnAge.setMapperClass(GenderBasedOnAgeMapper.class);
		
		genderBasedOnAge.setMapOutputKeyClass(Text.class);
		
		genderBasedOnAge.setMapOutputValueClass(Text.class);
		
		
		genderBasedOnAge.setPartitionerClass(GenderBasedOnAgePartitioner.class);
		
		
		genderBasedOnAge.setReducerClass(GenderBasedOnAgeReducer.class);
		
		genderBasedOnAge.setOutputKeyClass(Text.class);
		
		genderBasedOnAge.setOutputValueClass(NullWritable.class);
		
		genderBasedOnAge.setNumReduceTasks(5);
		
		
		genderBasedOnAge.setInputFormatClass(TextInputFormat.class);
		
		genderBasedOnAge.setOutputFormatClass(TextOutputFormat.class);
		
		
		Path inputPath = new Path(args[0]);
		
		Path outputPath = new Path(args[1]);
		
		FileInputFormat.addInputPath(genderBasedOnAge, inputPath);
		
		FileOutputFormat.setOutputPath(genderBasedOnAge, outputPath);
		
		
		FileSystem fileSystem = outputPath.getFileSystem(getConf());
		
		fileSystem.delete(outputPath,true);				
		
		int status = genderBasedOnAge.waitForCompletion(true) ? 0 : -1;		
		
		return status;
	}
	
	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(new Configuration(), new GenderBasedOnAgeDriver(), args);
		System.out.println("GenderBasedOnAge Partitioning Job status "+status);
		
	}
}
