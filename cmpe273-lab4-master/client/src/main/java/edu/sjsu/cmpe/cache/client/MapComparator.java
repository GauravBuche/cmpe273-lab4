package edu.sjsu.cmpe.cache.client;

import java.util.Comparator;
import java.util.HashMap;

public class MapComparator implements Comparator<String>{

	HashMap<String, Integer> valueMap;
	
	public MapComparator(HashMap<String, Integer> valueMap) {
		this.valueMap = valueMap;
	}
	
	
	@Override
	public int compare(String val1, String val2) {
		if(valueMap.get(val1) >= valueMap.get(val2)){
			return -1;
		}else{
			return 1;
		}
	}

}
