package edu.sjsu.cmpe.cache.client;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.async.Callback;
import com.mashape.unirest.http.exceptions.UnirestException;
import javax.print.attribute.standard.Severity;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.Future;

public class CRDTClient {

	public int successCnt = 0;
	public int failureCnt = 0;
	public int writeCnt = 0;
	public int addKey;
	public int delSuccessFlag = -1;
	public String oldVal;

	
	public ArrayList<CacheServiceInterface> serverList;
	public ArrayList<CacheServiceInterface> writeFailedServers;
	public ArrayList<CacheServiceInterface> successServerList = new ArrayList<CacheServiceInterface>();	
	public static HashMap<String, Integer> savedReadValue = new HashMap<String, Integer>();
	public HashMap<String, String> valuesByServers = new HashMap<String, String>();

	CRDTClient(ArrayList<CacheServiceInterface> serverList) 
	
	{
		this.serverList = serverList;
	}

/*************** Read from Cache ********************/

	public void readFromCache(int key) throws IOException 
	{   

		boolean writeSuccess = false;
		addKey = key;

		this.successCnt = 0;
		this.failureCnt = 0;
		this.writeCnt = serverList.size();

		for (final CacheServiceInterface server : serverList) 
		{
			Future<HttpResponse<JsonNode>> res = null;
			String tempServer = server.toString();
			try {
				res = Unirest
						.get(server.toString() + "/cache/{key}")
						.header("accept", "application/json")
						.routeParam("key", Long.toString(key))
						.asJsonAsync(new Callback<JsonNode>() {

							@Override
							public void failed(UnirestException e) {
								writeCnt--;
								failureCnt++;
								callbackReadOp();

							}

							@Override
							public void completed(HttpResponse<JsonNode> res) 
							{
								writeCnt--;
								successCnt++;
								String value = "";
								if(res.getBody() != null)
								{
									value = res.getBody().getObject().getString("value");
								}
								valuesByServers.put(server.toString(), value);
								Integer getExistingCounter = savedReadValue.get(value);
								if(getExistingCounter == null)
								{
									savedReadValue.put(value, 1);
								}else
								{
									savedReadValue.put(value, getExistingCounter+1);
								}
								callbackReadOp();
							}

							@Override
							public void cancelled() 
							{
								writeCnt--;
								failureCnt++;
								callbackReadOp();
							}
						});
			} catch (Exception e) {	e.printStackTrace(); }


		}
	}

                   


/********* Write to Cache *******/

	public void writeToCache(int key, String value) throws IOException 
	{

		boolean writeSuccess = false;
		addKey = key;

		this.successCnt = 0;
		this.failureCnt = 0;
		this.writeCnt = serverList.size();

		savedIntermediateStateOp();

		this.successCnt = 0;
		this.failureCnt = 0;

		this.writeCnt = serverList.size();

		this.successServerList = new ArrayList<CacheServiceInterface>();

		writeFailedServers = new ArrayList<CacheServiceInterface>();

		for (final CacheServiceInterface server : serverList) 
		{
			HttpResponse<JsonNode> res = null;
			try {
				res = Unirest
						.put(server.toString() + "/cache/{key}/{value}")
						.header("accept", "application/json")
						.routeParam("key", Long.toString(key))
						.routeParam("value", value)
						.asJsonAsync(new Callback<JsonNode>(){

							@Override
							public void failed(UnirestException e) 
							{
								writeCnt--;
								failureCnt++;
								callbackWriteOp();
								writeFailedServers.add(server);
							}

							@Override
							public void completed(HttpResponse<JsonNode> res) 
							{
								if (res.getCode() != 200) 
								{
									writeCnt--;
									failureCnt++;
								} 
								else 
								{
									writeCnt--;
									successCnt++;
									successServerList.add(server);
								}
								callbackWriteOp();
							}

							@Override
							public void cancelled() 
							{
								writeCnt--;
								failureCnt++;
								callbackWriteOp();

							}
						}).get();
			} catch (Exception e) {	}

			if (res == null || res.getCode() != 200) { }

		}


	}





/*************** Delete from Cache ******************/


	public void deleteFromCache(int key) throws IOException{ 

		boolean deleteSuccess = false;


		this.successCnt = 0;
		this.failureCnt = 0;
		this.writeCnt = successServerList.size();

		for (CacheServiceInterface server : successServerList) 
		{
			HttpResponse<JsonNode> res = null;

			try {
				Unirest
				.delete(server.toString() + "/cache/{key}")
				.header("accept", "application/json")
				.routeParam("key", Long.toString(key))
				.asJsonAsync(new Callback<JsonNode>() {

					@Override
					public void failed(UnirestException e) {
						writeCnt--;
						failureCnt++;
						callbackDeleteOp();

					}

					@Override
					public void completed(HttpResponse<JsonNode> res) 
					{
						if (res.getCode() != 204) 
						{
							writeCnt--;
							failureCnt++;
						} 
						else 
						{
							writeCnt--;
							successCnt++;
						}
						callbackDeleteOp();

					}

					@Override
					public void cancelled() 
					{
						writeCnt--;
						failureCnt++;
						callbackDeleteOp();

					}
				});

			} catch (Exception e) {	System.err.println(e); }

		}

	}



/***************** Call Back Delete ********************/

	public void callbackDeleteOp() {

		if (writeCnt == 0 && failureCnt == 0) {
			delSuccessFlag++;
			writeFailedRollbackOp();
		}else{
			try {
				deleteFromCache(addKey);
			} catch (IOException e) { e.printStackTrace(); }
		}

	}

/****************** Call Back Read  ********************/

	public void callbackReadOp() {

		if (writeCnt == 0 && successCnt == 3) 
		{
			String repairVal = getRepairValOp();
			repairCache(repairVal);

		}
	
	}



/**************** Get Repair Value *****************/


	private String getRepairValOp(){

		MapComparator mapComparator = new MapComparator(savedReadValue);
		SortedMap<String, Integer> sortedMap = new TreeMap<String, Integer>(mapComparator);
		sortedMap.putAll(savedReadValue);
		return sortedMap.firstKey();

	}



/***************** Save Intermediate State **********************/

	private void savedIntermediateStateOp(){
		for(CacheServiceInterface server:serverList){
			try {
				HttpResponse<JsonNode> res = Unirest.get(server + "/cache/{key}")
						.header("accept", "application/json")
						.routeParam("key", Long.toString(addKey)).asJson();
				oldVal = res.getBody().getObject().getString("value");
				if(oldVal == null) { continue; }
				else { break; }
			} catch (Exception e) {	continue; }
		}
	}

/*********************  Write Failed RollBack **********************/

	private void writeFailedRollbackOp(){

		for(CacheServiceInterface successServerList : this.successServerList)
		{
			String prevValue = oldVal;

			try {
				HttpResponse<JsonNode> res = Unirest.put(successServerList.toString() + "/cache/{key}/{value}")
						.header("accept", "application/json")
						.routeParam("key", Long.toString(addKey))
						.routeParam("value", prevValue)
						.asJson();
			     } catch (Exception e) {	}
		}

	}


/****************** Call Back Write ***********************/

	public void callbackWriteOp() 
	{

		if (writeCnt == 0 && failureCnt >= 2) 
		{

			try {
				System.out.println("Write operation failed! Rolling back values.. "+addKey+" => "+oldVal);
				deleteFromCache(addKey);

			} catch (IOException e) { e.printStackTrace(); }
		}

	}



/******************** Read Repair *********************/

	
	public void repairCache(String repairVal){

		boolean deleteSuccess = false;

		this.successCnt = 0;
		this.failureCnt = 0;
		this.writeCnt = successServerList.size();

		ArrayList<CacheServiceInterface> updateServerList = new ArrayList<CacheServiceInterface>();

		for (Entry<String, String> server : valuesByServers.entrySet()) {

			if(server.getValue() != null && !server.getValue().equals(repairVal)){

				updateServerList.add(new DistributedCacheService(server.getKey().toString()));
			}

		}

		if(updateServerList.size() >  0){

			for(CacheServiceInterface server : updateServerList){
				System.out.println("Repairing on "+ server.toString());
				try {
					HttpResponse<JsonNode> res = Unirest.put(server.toString() + "/cache/{key}/{value}")
							.header("accept", "application/json")
							.routeParam("key", Long.toString(addKey))
							.routeParam("value", repairVal)
							.asJson();
				} catch (UnirestException e) {	e.printStackTrace(); }
			}

		}else{

		}
		try {
			Unirest.shutdown();
		} catch (IOException e) { e.printStackTrace(); }
	}




}
