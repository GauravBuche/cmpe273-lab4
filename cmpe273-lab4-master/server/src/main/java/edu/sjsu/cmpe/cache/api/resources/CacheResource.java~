package edu.sjsu.cmpe.cache.api.resources;

import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.yammer.dropwizard.jersey.params.LongParam;
import com.yammer.metrics.annotation.Timed;

import edu.sjsu.cmpe.cache.domain.Entry;
import edu.sjsu.cmpe.cache.repository.CacheInterface;

@Path("/")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class CacheResource {
    private final CacheInterface cache;

    /**
     * CacheResource constructor
     * 
     * @param cache
     *            a InMemoryCache instance
     */
    public CacheResource(CacheInterface cache) {
        this.cache = cache;
    }

    @GET
    @Path("{key}")
    @Timed(name = "get-entry")
    public Entry get(@PathParam("key") LongParam key) {
        return cache.get(key.get());
    }

    @GET
    @Timed(name = "view-all-entries")
    public List<Entry> getAll() {
        return cache.getAll();
    }

    @PUT
    @Path("{key}/{value}")
    @Timed(name = "add-entry")
    public Response put(@PathParam("key") LongParam key,
            @PathParam("value") String value) {
    	System.out.println("PUT");
        Entry entry = new Entry();
        entry.setKey(key.get());
        entry.setValue(value);

        cache.save(entry);

        return Response.status(200).build();
    }

/***********************************************************************************************/    

    /* Server Side Delete Operation*/

    @DELETE
    @Path("{key}")
    @Timed(name = "DeleteEntry")
    public Response delete(@PathParam("key") LongParam key) {
    	System.out.println("delete");
    	Entry entry = cache.delete(key.get());
    	System.out.println("entry "+entry);
    	if(entry == null)
	{
    		System.out.println("400");
	 	return Response.status(400).build();
    	}
	else
	{
		System.out.println("204");
    		return Response.status(204).build();
    	}

 }

/***********************************************************************************************/    
}
