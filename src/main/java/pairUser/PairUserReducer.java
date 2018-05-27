package pairUser;

import java.io.IOException;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PairUserReducer extends Reducer<Text, Text, Text, Text> {

	private static final int numberProdMin = 3;
	private TreeMap<String, Set<String>> userProducts;
	private Map<String, Set<String>> productUsers;

	@Override
	protected void setup(Context ctx) {
		this.userProducts = new TreeMap<String, Set<String>>();
		this.productUsers = new HashMap<String, Set<String>>();
	}

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		Set<String> set = new HashSet<String>();

		for (Text value : values) {
			set.add(value.toString());
		}
		String temp;
		Iterator<String> is = set.iterator();
		while (is.hasNext()) {
			temp = is.next();
			if (productUsers.containsKey(temp)) {
				productUsers.get(temp).add(key.toString());
			} else {
				Set<String> setTemp = new HashSet<String>();
				setTemp.add(key.toString());
				productUsers.put(temp, setTemp);
			}

		}
		userProducts.put(key.toString(), set);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {

		Entry<String, Set<String>> entryMapUserProducts;
		Comparator<? super Pair<String, String>> secondCharComparator = new Comparator<Pair<String, String>>() {
			@Override
			public int compare(Pair<String, String> o1, Pair<String, String> o2) {
				return new Integer(o1.hashCode()).compareTo(new Integer(o2.hashCode()));
			}
		};

		TreeMap<Pair<String, String>, Set<String>> output = new TreeMap<Pair<String, String>, Set<String>>(
				secondCharComparator);

		while ((entryMapUserProducts = userProducts.firstEntry()) != null) {
			String user = entryMapUserProducts.getKey();
			Set<String> setUserProducts = entryMapUserProducts.getValue();
			Iterator<String> iProduct = setUserProducts.iterator();
			while (iProduct.hasNext()) {
				String temp = iProduct.next();
				productUsers.get(temp).forEach(u -> {
					if (!u.equals(user)) {
						Pair<String, String> pairTemp = new Pair<String, String>(user, u);
						if (output.containsKey(pairTemp))
							output.get(pairTemp).add(temp);
						else {
							Set<String> setTemp = new HashSet<String>();
							setTemp.add(temp);
							output.put(pairTemp, setTemp);
						}

					}
				});
			}
			userProducts.remove(user);
		}
		productUsers = null;
		Set<Entry<Pair<String, String>, Set<String>>> entryOutput = entriesSortedByValues(output);

		entryOutput.forEach(o -> {
			if (o.getValue().size() >= numberProdMin) {
				String prod;
				Iterator<String> it = o.getValue().iterator();
				prod = it.next();
				while (it.hasNext()) {
					prod += "\t" + it.next();
				}
				try {
					context.write(new Text(o.getKey().toString()), new Text(prod));
				} catch (IOException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
	}

	public SortedSet<Map.Entry<Pair<String, String>, Set<String>>> entriesSortedByValues(
			Map<Pair<String, String>, Set<String>> map) {
		SortedSet<Map.Entry<Pair<String, String>, Set<String>>> sortedEntries = new TreeSet<Map.Entry<Pair<String, String>, Set<String>>>(
				new Comparator<Map.Entry<Pair<String, String>, Set<String>>>() {
					@Override
					public int compare(Map.Entry<Pair<String, String>, Set<String>> e1,
							Map.Entry<Pair<String, String>, Set<String>> e2) {
						int res = e1.getKey().compareTo(e2.getKey());
						return res != 0 ? res : 1;
					}
				});
		sortedEntries.addAll(map.entrySet());
		return sortedEntries;
	}

}
