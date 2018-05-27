package top5;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Top5Mapper1 extends Mapper<Object, Text, Text, IntWritable> {

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().split("\t");
		long timestamp = Long.parseLong(tokens[7]);
		Date date = new Date(timestamp * 1000L);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM");
		String formattedDate = sdf.format(date);
		String dataProd=formattedDate+","+tokens[1];
		context.write(new Text(dataProd), new IntWritable(Integer.parseInt(tokens[6])));
	}
}