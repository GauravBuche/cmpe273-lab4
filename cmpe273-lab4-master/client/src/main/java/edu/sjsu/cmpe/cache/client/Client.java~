package edu.sjsu.cmpe.cache.client;

import java.util.ArrayList;
import java.io.IOException;
import com.mashape.unirest.http.Unirest;

public class Client {

    public static void main(String[] args) throws Exception {
        
    	
    	ArrayList<CacheServiceInterface> serverList = new ArrayList<CacheServiceInterface>();
    	
	/*********************************************************************************************/
	    CacheServiceInterface cacheOne = new DistributedCacheService("http://localhost:3000");
	    CacheServiceInterface cacheTwo = new DistributedCacheService("http://localhost:3001");
	    CacheServiceInterface cacheThree = new DistributedCacheService("http://localhost:3002");
	    serverList.add(cacheOne);
	    serverList.add(cacheTwo);
	    serverList.add(cacheThree);
    	
	/*********************************************************************************************/
	    System.out.println("Step 1: Writing value 1 => a - On Instance 1, 2 and 3");

	    CRDTClient client = new CRDTClient(serverList);
	    client.writeToCache(1, "a");

	    System.out.println("Step 2: Writing Value 1 => b - On Instance 2 and 3 (turn down instance 1)");
	    Thread.sleep(30000);

	    client = new CRDTClient(serverList);
	    client.writeToCache(1, "b");

	    System.out.println("Step 3: Read Value from Instance 1, 2 and 3 (bring up instance 1)");
	    Thread.sleep(30000);

	    client.readFromCache(1);
	    
	/*********************************************************************************************/    
	   
	    
    }

}
