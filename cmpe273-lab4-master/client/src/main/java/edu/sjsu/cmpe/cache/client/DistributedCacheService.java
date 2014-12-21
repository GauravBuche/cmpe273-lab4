package edu.sjsu.cmpe.cache.client;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

/**
 * Distributed cache service
 * 
 */
public class DistributedCacheService implements CacheServiceInterface {
    private final String cacheServerUrl;

    public DistributedCacheService(String serverUrl) {
        this.cacheServerUrl = serverUrl;
    }

    /**
     * @see edu.sjsu.cmpe.cache.client.CacheServiceInterface#get(long)
     */
    @Override
    public String get(long key) {
        HttpResponse<JsonNode> res = null;
        try {
            res = Unirest.get(this.cacheServerUrl + "/cache/{key}")
                    .header("accept", "application/json")
                    .routeParam("key", Long.toString(key)).asJson();
        } catch (UnirestException e) {
            System.err.println(e);
        }
        String value = res.getBody().getObject().getString("value");

        return value;
    }

    /**
     * @see edu.sjsu.cmpe.cache.client.CacheServiceInterface#put(long,
     *      java.lang.String)
     */
    @Override
    public void put(long key, String value) {
        HttpResponse<JsonNode> res = null;
        try {
            res = Unirest
                    .put(this.cacheServerUrl + "/cache/{key}/{value}")
                    .header("accept", "application/json")
                    .routeParam("key", Long.toString(key))
                    .routeParam("value", value).asJson();
        } catch (UnirestException e) {
            System.err.println(e);
        }

        if (res.getCode() != 200) {
            System.out.println("Failed to add to the cache.");
        }
    }
    
    
    /* Added Delete */
    @Override
    public boolean delete(long key) {
        HttpResponse<JsonNode> res = null;
        boolean delSuccess = false;
        try {
            res = Unirest.get(this.cacheServerUrl + "/cache/{key}")
                    .header("accept", "application/json")
                    .routeParam("key", Long.toString(key)).asJson();
            System.out.println(res.getCode());
            if(res.getCode() == 204){
            	delSuccess = true;
            }
            
        } catch (UnirestException e) {
            System.err.println(e);
        }

        return delSuccess;
    }
    
    public String getCacheServerUrl(){
    	return this.cacheServerUrl;
    }
    
    @Override
    public String toString(){
    	return this.cacheServerUrl;
    }
}
