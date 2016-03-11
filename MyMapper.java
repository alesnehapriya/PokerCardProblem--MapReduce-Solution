package PokerCards;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {


	@Override
	protected void map(LongWritable offset, Text line,Context context)
			throws IOException, InterruptedException {
		
		//Input Data format Clubs C,Heart H,Spade S,Diamond D  
		// CA,C2,HA,H2
		
		String currentLine = line.toString();
		System.out.println("MyMapper.map():offset "+offset+" :: CurrentLine= "+currentLine );
		
		String words[] = currentLine.split(",");

		
		for(String word:words){
			context.write(new Text(word), new IntWritable(1));
		}
	
	}
	

	

}
