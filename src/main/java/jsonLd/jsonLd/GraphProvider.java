package jsonLd.jsonLd;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;

public class GraphProvider {
	
	private Client client;
	
	public GraphProvider()
	{
		Cluster cluster;
		try 
		{
			cluster = Cluster.build(new File("src/cluster.yaml")).create();
		} 
		catch (FileNotFoundException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
		
		this.client = cluster.connect();
	}
	
	public List<Result> read(String query)
	{
		System.out.println("\nSubmitting this Gremlin query: " + query);

        // Submitting remote query to the server.
        ResultSet results = client.submit(query);

        CompletableFuture<List<Result>> completableFutureResults;
        CompletableFuture<Map<String, Object>> completableFutureStatusAttributes;
        List<Result> resultList = new ArrayList<Result>();
        Map<String, Object> statusAttributes = new HashMap<String, Object>();
        
        try
        {
            completableFutureResults = results.all();
            completableFutureStatusAttributes = results.statusAttributes();
            resultList = completableFutureResults.get();
            statusAttributes = completableFutureStatusAttributes.get();            
        }
        catch(ExecutionException | InterruptedException e)
        {
            e.printStackTrace();
        }
        catch(Exception e)
        {
            ResponseException re = (ResponseException) e.getCause();
            /*
            // Response status codes. You can catch the 429 status code response and work on retry logic.
            System.out.println("Status code: " + re.getStatusAttributes().get().get("x-ms-status-code")); 
            System.out.println("Substatus code: " + re.getStatusAttributes().get().get("x-ms-substatus-code")); 
            
            // If error code is 429, this value will inform how many milliseconds you need to wait before retrying.
            System.out.println("Retry after (ms): " + re.getStatusAttributes().get().get("x-ms-retry-after"));

            // Total Request Units (RUs) charged for the operation, upon failure.
            System.out.println("Request charge: " + re.getStatusAttributes().get().get("x-ms-total-request-charge"));
            
            // ActivityId for server-side debugging*/
            System.out.println("ActivityId: " + re.getStatusAttributes().get().get("x-ms-activity-id"));
            
            throw(e);
        }
        
        // Status code for successful query. Usually HTTP 200.
        System.out.println("Status: " + statusAttributes.get("x-ms-status-code").toString());

        // Total Request Units (RUs) charged for the operation, after a successful run.
        System.out.println("Total charge: " + statusAttributes.get("x-ms-total-request-charge").toString());
        
        return resultList;
	}
	
	public void insert(List<String> queries)
	{
		/**
	     * This method iterates through a string array with queries and prints the result of the execution.
	     * */   
		for (String query : queries) 
		{
            System.out.println("\nSubmitting this Gremlin query: " + query);

            // Submitting remote query to the server.
            ResultSet results = this.client.submit(query);

            CompletableFuture<Map<String, Object>> completableFutureStatusAttributes;
            Map<String, Object> statusAttributes;

            try
            {
                completableFutureStatusAttributes = results.statusAttributes();
                statusAttributes = completableFutureStatusAttributes.get();            
            }
            catch(ExecutionException | InterruptedException e)
            {
                e.printStackTrace();
                break;
            }
            catch(Exception e)
            {
                ResponseException re = (ResponseException) e.getCause();
                /*
                // Response status codes. You can catch the 429 status code response and work on retry logic.
                System.out.println("Status code: " + re.getStatusAttributes().get().get("x-ms-status-code")); 
                System.out.println("Substatus code: " + re.getStatusAttributes().get().get("x-ms-substatus-code")); 
                
                // If error code is 429, this value will inform how many milliseconds you need to wait before retrying.
                System.out.println("Retry after (ms): " + re.getStatusAttributes().get().get("x-ms-retry-after"));

                // Total Request Units (RUs) charged for the operation, upon failure.
                System.out.println("Request charge: " + re.getStatusAttributes().get().get("x-ms-total-request-charge"));
                
                // ActivityId for server-side debugging
                System.out.println("ActivityId: " + re.getStatusAttributes().get().get("x-ms-activity-id"));*/
                System.out.println("ActivityId: " + re.getStatusAttributes().get().get("x-ms-activity-id"));
                throw(e);
               
            }

            // Status code for successful query. Usually HTTP 200.
            System.out.println("Status: " + statusAttributes.get("x-ms-status-code").toString());

            // Total Request Units (RUs) charged for the operation, after a successful run.
            System.out.println("Total charge: " + statusAttributes.get("x-ms-total-request-charge").toString());
        }
	}
}
