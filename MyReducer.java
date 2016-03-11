package PokerCards;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text,Iterable<IntWritable>,Text,IntWritable> {

//	c1[1]
	
@Override
protected void reduce(Text card, Iterable<Iterable<IntWritable>> value,
		Context ctx)
				throws IOException, InterruptedException {
	// TODO Auto-generated method stub
		Iterator it = 	value.iterator();
		int count=0;
		int set=100;
		while(it.hasNext()){
			IntWritable i= (IntWritable)it.next();
			count=count+i.get();
		}
		
			ctx.write(new Text(card), new IntWritable(100-count));
	
	
}	
	
}
