package pairUser2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PairUser2 {

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Usage: Top5Cascata <in> <out>");
			System.exit(2);
		}
		Path temp = new Path("/temp");
		Configuration cf= new Configuration();
		Job job1 = new Job(cf, "top-5-step-1");
		Job job2 = new Job(cf, "top-5-step-2");
		
	    job1.setJarByClass(PairUser2.class);
	    job2.setJarByClass(PairUser2.class);
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, temp);
		
		FileInputFormat.setInputPaths(job2, temp);
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		
		
		job1.setMapperClass(PairUser2Map1.class);
		job1.setReducerClass(PairUser2Reduce1.class);
		// Operazioni di formattazione dell'output Map
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		// Operazioni di formattazione dell'output Reducer
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		boolean succ = job1.waitForCompletion(true);
		if (!succ) {
			System.out.println("Job1 failed, exiting");
		}

		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		
		job2.setMapperClass(PairUser2Map2.class);
		job2.setReducerClass(PairUser2Reduce2.class);
		// Operazioni di formattazione dell'output Map
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		// Operazioni di formattazione dell'output Reducer
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		long startTime = System.currentTimeMillis();
		succ = job2.waitForCompletion(true);
		if (!succ) {
			System.out.println("Job2 failed, exiting");
		}
		long stopTime = System.currentTimeMillis();
		System.out.println("Job Finished in " + (stopTime - startTime) / 1000.0 + " seconds");
	}

}
