package jsonLd.jsonLd;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONObject;

public class Graph {

	public ArrayList<String> edges;
	public ArrayList<String> vertices;
	private ArrayList<String> contextEdges;
	
	public Graph()
	{
		this.edges = new ArrayList<String>();
		this.vertices = new ArrayList<String>();
		this.contextEdges = new ArrayList<String>();
	}
	
	public void buildGraphFromJsonLd(String filePath)
	{
		 try (InputStream input = new FileInputStream(filePath))
	        {
	            //Parse file to JSON Object
	            JSONObject jsonObject = new JSONObject(IOUtils.toString(input.readAllBytes(), "UTF-8"));

		       /**
		        * If we were successful reading the file, now we can create the gremlin queries to insert nodes and edges into Cosmos Db
		        * 
		        *      1. createContextNode: recieves the context inside the json ld object
		        *      		First, we create the query to insert a context node and store the query in context Array List.
		        *      		This method also adds into contextEdges the list of properties that are edges
		        *      2. parseJsonLdGraph: receives the json ld file
		        *      		Create the queries to insert the nodes and store the queries in nodes Array List
		        *      3. parseJsonLdEdges: receives the json ld file
		        *      		Create the queries to insert the edges and store the queries in edges Array List
		        */
		        
		        //Store the gremlin queries to create nodes in an Array List by calling the parse JsonLdObject method
		        parseJsonLdGraph(jsonObject);
		        
		        //Store the gremlin queries to create edges in an Array List by calling the parse JsonLdObject method
		        parseJsonLdEdges(jsonObject);
	        } 
	        catch (FileNotFoundException e) 
	        {
	            e.printStackTrace();
	        } 
	        catch (IOException e) 
	        {
	            e.printStackTrace();
	        } 
	}
	
	private void parseJsonLdGraph(JSONObject jsonObject)
    {    
    	/**
    	 * This method iterates through the json objects inside the json file and calls the createVertex method which assigns the query to an Array List
    	 * */
    	//Extract the array inside @graph json ld
        JSONArray graphlist = (JSONArray) jsonObject.get("@graph");              
        
        //Iterate over graph array
        for (int i = 0; i < graphlist.length(); i++) 
        {        	
        	createVertex((JSONObject)graphlist.get(i));
        }            	     
    }
    
    private void parseJsonLdEdges(JSONObject jsonObject)
    {    
    	/**
    	 * This method iterates through the json objects inside the json file and calls the createEdges method which adds the query to an Array List
    	 * */
    	//Extract the array inside @context json ld
        JSONArray graphlist = (JSONArray) jsonObject.get("@graph");      
        
        //Iterate over graph array
        for (int i = 0; i < graphlist.length(); i++) 
        {        	
        	createEdges((JSONObject)graphlist.get(i));
        }          	     
    }
    
	private void createVertex(JSONObject jsonld) {
		/**
		 * This method receives a json object. 
		 * 1. First, we assiggn the node label with the @type property in the json object
		 * 2. We assign the node property 'jsonldid' with the @id property in the json object
		 * 3. We iterate through the rest of json properties to assign it to the current node
		 * */
		
		//Assign the node label with the @type property in the json object
		String label = "";
		if (jsonld.get("@type").getClass() == String.class) 
		{
			label = (String) jsonld.get("@type");	
		} 
		else if(jsonld.get("@type").getClass() == JSONArray.class) 
		{ 	
			label = (String)((JSONArray) jsonld.get("@type")).get(0);		    
		} 
		else 
		{
			label = "undefined";
		}

		//Assign the node property 'jsonldid' with the @id property in the json object
		String id = (String) jsonld.get("@id"); 
		
		if(Utils.isValid(id))
		{
			id = Utils.encodeURL(id);
		}
		
		String insertion = "g.addV('"+label+"').property('id', '"+id+"').property('partitionkey','"+id+"')";
		
    	for (String key : jsonld.keySet()) 
    	{    		
    		if(!key.equalsIgnoreCase("@id") && !this.contextEdges.contains(key)) 
    		{
    			if (jsonld.get(key).getClass() == JSONArray.class) 
    			{    
    				for (int j = 0; j< ((JSONArray) jsonld.get(key)).length(); j++) 
    				{
    					insertion += (".property(list, '"+this.getKeyToBeInserted(key)+"', '"+ ((JSONArray) jsonld.get(key)).get(j).toString().replace("'", "") +"')"); 					
    				}
    			}
    			else if(jsonld.get(key).getClass() == JSONObject.class) 
    			{
    				for (String key2 : ((JSONObject) jsonld.get(key)).keySet()) 
    				{   					
    					String value = (String) ((JSONObject) jsonld.get(key)).get(key2);
    					
    					insertion += (".property(list, '"+this.getKeyToBeInserted(key)+"', '"+  value +"')"); 					
    				}
    			}
    			else 
    			{    				
    				insertion += (".property('"+this.getKeyToBeInserted(key)+"' , '"+  jsonld.get(key).toString().replace("'", "") +"')"); 
    			}   			 		   			 
    		}    		    		
    	}	
    
    	//	Add the query string to nodes Array List
    	vertices.add(insertion);
	}
	
	private String getKeyToBeInserted(String key)
	{
		String keyToBeInserted = key;
		if(key.equalsIgnoreCase("@type"))
		{
			keyToBeInserted = "type";
		}
		return keyToBeInserted;
	}
	
	private void createEdges(JSONObject jsonld) {
		/**
		 * This method receives a json object. 
		 * 1. First, we assign the edge 'jsonldid' property with @id
		 * 2. We iterate through the properties inside the json object
		 * 3. We check if the property is inside the list of edges contained in the context
		 * 4. If it is inside the list, we check to see if it is a valid url
		 * 5. If it is compliant with both conditions, we assign the edge or edges. Depending on how many came inside a json object. 
		 * 		(one if it's a string, or many if those are contained inside an array or a nested json)
		 * */	

		//Assign the edge 'jsonldid' property with @id
    	String id = (String) jsonld.get("@id");
    	if(Utils.isValid(id))
    	{
    		id = Utils.encodeURL(id);
    	}
    	
    	//Iterate through the properties inside the json object
    	for (String key : jsonld.keySet()) 
    	{
    		//Check if the property is inside the list of edges contained in the context   
    		if(contextEdges.contains(key))
    		{
    			if (jsonld.get(key).getClass() == JSONArray.class) 
    			{        				
    				for (int j = 0; j< ((JSONArray) jsonld.get(key)).length(); j++) 
    				{
    					//if the value contains an url, it has an edge       					
        				String value = (String) ((JSONArray) jsonld.get(key)).get(j);	
            			if(Utils.isValid(value)) 
            			{        			
            				value = Utils.encodeURL(value);
            		    	edges.add("g.V().has('id','"+id+"').addE('"+key+"').to(g.V().has('id','"+value+"')).property('partitionkey', '"+id+"')");
            			}
    				}
    			}
    			else if(jsonld.get(key).getClass() == JSONObject.class) 
    			{
    				for (String key2: ((JSONObject) jsonld.get(key)).keySet()) 
    				{
    					//if the value contains an url, it has an edge
        				String value = (String) ((JSONObject) jsonld.get(key)).get(key2);            				
            			if(Utils.isValid(value)) 
            			{
            				value = Utils.encodeURL(value);
            				edges.add("g.V().has('id', '"+id+"').addE('"+key+"').to(g.V().has('id', '"+value+"')).property('partitionkey', '"+id+"').property('"+key2+"', '"+value+"')");
            			}                			            							
    				}        				
    			}
    			else 
    			{
    				//if the value contains an url, it has an edge
    				String value = (String) jsonld.get(key);	            			
        			if(Utils.isValid(value)) 
        			{
        				value = Utils.encodeURL(value);
        				edges.add("g.V().has('id', '"+id+"').addE('"+key+"').to(g.V().has('id', '"+value+"')).property('partitionkey', '"+id+"')");
        			}
    			}
    		}
    	}
	}
}
