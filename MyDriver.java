package PokerCards;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MyDriver {
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		//Configuration class pointing to default configuration
		Configuration conf = new Configuration();

		//Preparing Job Object
		Job job = new Job(conf, "PokerCardJob");
		
		//Link driver class with the Job
		job.setJarByClass(MyDriver.class);
		
		//Link mapper with job
		job.setMapperClass(MyMapper.class);
		
		//Link reducer with job
		job.setReducerClass(MyReducer.class);
		
		//Set Final Output Key
		job.setMapOutputKeyClass(Text.class);
		
		//Set Final Output Value
		job.setMapOutputValueClass(IntWritable.class);
		
		//Input and Output Path
		Path input_dir = new Path(args[0]);
		FileInputFormat.addInputPath(job, input_dir);
		
		
		
		//Add output path to Job
		Path output_dir = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, output_dir);
		
		
		
		System.exit(job.waitForCompletion(true) ? 0:1);
		
	}

}
