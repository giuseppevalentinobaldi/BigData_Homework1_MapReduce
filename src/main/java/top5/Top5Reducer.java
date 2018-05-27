package top5;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Top5Reducer extends Reducer<Text, Text, Text, Text> {

	private TreeMap<String, Map<String, Average>> map;

	@Override
	protected void setup(Context ctx) {
		this.map = new TreeMap<String, Map<String, Average>>();
	}

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		String stringKey = key.toString();
		
		for (Text value : values) {
			
			String[] tokens = value.toString().split(",");
			
			if(map.containsKey(stringKey)){
				if(map.get(stringKey).containsKey(tokens[0])){
					map.get(stringKey).get(tokens[0]).updateAverage(Integer.parseInt(tokens[1]));
				}
				else{
					map.get(stringKey).put(tokens[0], new Average(Integer.parseInt(tokens[1])));
				}
			}
			else{
				Map<String, Average> prodMap = new HashMap<String, Average>();
				prodMap.put(tokens[0], new Average(Integer.parseInt(tokens[1])));
				map.put(stringKey, prodMap);
			}
			
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		int count = 0;
		
		for(Entry<String, Map<String, Average>> treeMapEntry : map.entrySet()){
			
			Map<String, Average> sortValueMap = sortByValues(treeMapEntry.getValue());
			
			for(Entry<String, Average> sortMapEntry : sortValueMap.entrySet()){
				
				if(count == 5) break;
				
				context.write(new Text(treeMapEntry.getKey()), new Text(sortMapEntry.getKey()+"\t"+(float)sortMapEntry.getValue().getAverage()));
				count++;
			}
			
			count = 0;
			
		}

	}

	@SuppressWarnings("rawtypes")
	private static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {

		List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());

		Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {
			
			@SuppressWarnings("unchecked")
			public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
				return o2.getValue().compareTo(o1.getValue());
			}

		});

		Map<K, V> sortedMap = new LinkedHashMap<K, V>();

		for (Map.Entry<K, V> entry : entries) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}

		return sortedMap;
	}
}