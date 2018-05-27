package pairUser2;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PairUser2Reduce2 extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Set<String> products = new HashSet<String>();
		values.forEach(p -> products.add(p.toString()));
		if (products.size() > 2)
			context.write(key, new Text(products.toString()));
	}
}
