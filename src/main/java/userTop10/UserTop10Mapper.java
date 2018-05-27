package userTop10;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class UserTop10Mapper extends Mapper<LongWritable, Text, Text, Text>{

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] tokens = value.toString().split("\t");
		
		context.write(new Text(tokens[2]), new Text(tokens[1] + "," + tokens[6]));
	}
	
}
