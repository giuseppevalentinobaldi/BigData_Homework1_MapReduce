package top5;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class Top5Mapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] tokens = value.toString().split("\t");
		
		// trasformazione dell'Unix Time
		long timestamp = Long.parseLong(tokens[7]);
		Date date = new Date(timestamp * 1000L);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM");
		String formattedDate = sdf.format(date);
		
		context.write(new Text(formattedDate), new Text(tokens[1] + "," + tokens[6]));
	}
}