package top5;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Top5Mapper2 extends Mapper<Text, Text, Text, Text> {

	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = key.toString().split(",");
		context.write(new Text(tokens[0]), new Text(tokens[1] + "," + value.toString()));
	}
}