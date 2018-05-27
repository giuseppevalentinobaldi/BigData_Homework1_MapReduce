package userTop10;

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

public class UserTop10Reducer extends Reducer<Text, Text, Text, Text> {
	
	private TreeMap<String, Map<String, Integer>> map;

	@Override
	protected void setup(Context ctx) {
		this.map = new TreeMap<String, Map<String, Integer>>();
	}

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		String stringKey = key.toString();
		
		for (Text value : values) {
			
			String[] tokens = value.toString().split(",");
			
			if(map.containsKey(stringKey)){
				
				Map<String, Integer> keyMap = map.get(stringKey);
				
				if(keyMap.containsKey(tokens[0])){
					
					int s = Integer.parseInt(tokens[1]);
					
					if(keyMap.get(tokens[0]) < s){
						keyMap.put(tokens[0], s);
					}
					
				}
				else{
					keyMap.put(tokens[0], Integer.parseInt(tokens[1]));
				}
				
			}
			else{
				Map<String, Integer> prodMap = new HashMap<String, Integer>();
				prodMap.put(tokens[0], Integer.parseInt(tokens[1]));
				map.put(stringKey, prodMap);
			}
			
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		int count = 0;
		
		for(Entry<String, Map<String, Integer>> treeMapEntry : map.entrySet()){
			
			Map<String, Integer> sortValueMap = sortByValues(treeMapEntry.getValue());
			
			for(Entry<String, Integer> sortMapEntry : sortValueMap.entrySet()){
				
				if(count == 10) break;
				
				context.write(new Text(treeMapEntry.getKey()), new Text(sortMapEntry.getKey()+"\t"+sortMapEntry.getValue()));
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
