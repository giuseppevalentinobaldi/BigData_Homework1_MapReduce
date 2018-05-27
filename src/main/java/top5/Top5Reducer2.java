package top5;

import java.io.IOException;
import java.util.Stack;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Top5Reducer2 extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Stack<Product> s1 = new Stack<Product>();
		String[] tokens;
		for (Text value : values) {
			tokens = value.toString().split(",");
			s1.push((new Product(tokens[0], Float.parseFloat(tokens[1]))));
			
		}
		sort(s1);
		int len=s1.size();
		for (int j = 0; j < len && j < 5; j++) {
			context.write(key, new Text(s1.pop().toString()));
		}
	}
	
	public static Stack<Product> sort(Stack<Product> s) {

		if (s.isEmpty()) {
			return s;
		}
		Product pivot = s.pop();

		// partition
		Stack<Product> left = new Stack<Product>();
		Stack<Product> right = new Stack<Product>();
		while (!s.isEmpty()) {
			Product y = s.pop();
			if (y.getAverage() < pivot.getAverage()) {
				left.push(y);
			} else {
				right.push(y);
			}
		}
		sort(left);
		sort(right);

		// merge
		Stack<Product> tmp = new Stack<Product>();
		while (!right.isEmpty()) {
			tmp.push(right.pop());
		}
		tmp.push(pivot);
		while (!left.isEmpty()) {
			tmp.push(left.pop());
		}
		while (!tmp.isEmpty()) {
			s.push(tmp.pop());
		}
		return s;
	}
}