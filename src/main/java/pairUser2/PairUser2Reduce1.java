package pairUser2;

import java.io.IOException;
import java.util.TreeSet;
import java.util.Set;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PairUser2Reduce1 extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String k = key.toString();
		Set<String> users = new TreeSet<String>();
		values.forEach(u -> users.add(u.toString()));
		Iterator<String> i1 = users.iterator();
		while (i1.hasNext()) {
			String user1 = i1.next().toString();
			Iterator<String> i2 = users.iterator();
			while (i2.hasNext()) {
				String user2 = i2.next().toString();
				if (!user1.equals(user2)) {
					if (user1.compareTo(user2) < 0) {
						context.write(new Text(user1 + "," + user2), new Text(k));
					}
				}
			}
		}
	}
}
