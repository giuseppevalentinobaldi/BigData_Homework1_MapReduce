package top5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Top5 {

	public static void main(String[] args) throws Exception{
		
		// creazione del job
		Job job = new Job(new Configuration(), "Top5");
		// settaggio della classe jar (la classe "Main")
		job.setJarByClass(Top5.class);

		// settaggio delle classi Mapper e Reducer (se necessario anche la
		// classe Combiner)
		job.setMapperClass(Top5Mapper.class);
		job.setReducerClass(Top5Reducer.class);

		// settaggio dei file di input e di output
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// settaggio output del Mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// settaggio output del Reducer
		job.setOutputKeyClass(Text.class);
		job.setOutputKeyClass(Text.class);

		long startTime = System.currentTimeMillis();
		
		// lancio il job
		job.waitForCompletion(true);
		
		long stopTime = System.currentTimeMillis();
		
		System.out.println("Job Finished in " + (stopTime - startTime) / 1000.0 + " seconds");

	}

}
