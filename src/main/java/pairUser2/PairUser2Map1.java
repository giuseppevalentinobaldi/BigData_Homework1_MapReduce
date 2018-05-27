package pairUser2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class PairUser2Map1 extends Mapper<LongWritable, Text, Text, Text> {
	
	private static final int scoreMin = 4;
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] tokens = value.toString().split("\t");
		
		if(Integer.parseInt(tokens[6]) >= scoreMin){
			context.write(new Text(tokens[1]), new Text(tokens[2]));
		}
		
	}

}
