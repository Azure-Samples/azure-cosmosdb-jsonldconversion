package jsonLd.jsonLd;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.json.JSONArray;
import org.json.JSONObject;
import com.google.gson.Gson;

public class GraphParser {

	private GraphProvider provider;
	private Map<String, Object> context;
	
	public GraphParser(GraphProvider provider)
	{
		this.provider = provider;
		this.context = new HashMap<String, Object>();
	}
	
	public JSONObject parse(List<Result> input)
	{
        JSONArray vertices = new JSONArray();
        
		for (Result result : input) 
        {
            Map<String, Object> vertex = new HashMap<String, Object>();
            
            String vertexId = "";
            try 
            {
            	Gson gson = new Gson();
            	
            	String json = gson.toJson(result.getObject());
				
            	JSONObject jsonObject = new JSONObject(json);
				
				for(String key : jsonObject.keySet())
				{
					if(key.equalsIgnoreCase("id"))
					{
						vertexId = jsonObject.getString(key);
						
						String decodedVertexId = Utils.isValid(vertexId) ? Utils.decodeURL(vertexId) : vertexId;
						
						key = getJsonLDKey(key);
						
						vertex.put(key, decodedVertexId);
					}
					else if(key.equalsIgnoreCase("properties"))
					{
						JSONObject properties = (JSONObject) jsonObject.get(key);
						
						for(String propkey : properties.keySet())
						{
							if(propkey.equalsIgnoreCase("partitionkey"))
							{
								continue;
							}
							JSONArray propertyArray = (JSONArray) properties.get(propkey);
							
							propkey = getJsonLDKey(propkey);
							
							for(Object element : propertyArray)
							{
								JSONObject propObject = (JSONObject) element;
								
								if(vertex.containsKey(propkey))
								{
									if(vertex.get(propkey) instanceof JSONArray)
									{
										((JSONArray) vertex.get(propkey)).put(propObject.get("value"));
									}
									else
									{
										JSONArray propArray = new JSONArray();
										propArray.put(vertex.get(propkey));
										propArray.put(propObject.get("value"));
										vertex.put(propkey, propArray);
									}
								}
								else
								{
									vertex.put(propkey, propObject.getString("value"));
								}
							}
						}
					}
				}
			} 
            catch (Exception e) 
            {
				e.printStackTrace();
				throw(e);
			}
            
            if(!vertex.isEmpty())
            {
            	Map<String, Object> contextProperties = this.getContextProperties(vertexId);
            	vertex.putAll(contextProperties);
            	vertices.put(new JSONObject(vertex));
            }
        }
        
        JSONObject graph = new JSONObject();
        
        graph.put("@graph", vertices);
        graph.put("@context", this.context);
        
        return graph;
	}
	
	private Map<String, Object> getContextProperties(String vertexId)
	{
		String query = "g.V('id', '"+vertexId+"').OutE()";
		
		List<Result> edges = this.provider.read(query);
		
		Map<String, Object> contextProperties = new HashMap<String, Object>();
		
		try 
        {
	        for(Result edge : edges)
	        {
	        	Gson gson = new Gson();
	        	
	        	String json = gson.toJson(edge.getObject());
	        	
	        	JSONObject jsonObject = new JSONObject(json);
	        	
	        	String contextKey = jsonObject.getString("label");
	        	
	        	this.addContextProperty(contextKey);
	        	
	        	String contextValue = jsonObject.getString("inV");
	        	
	        	contextValue = Utils.isValid(contextValue) ? Utils.decodeURL(contextValue) : contextValue;	        	
	        	
	        	if(contextProperties.containsKey(contextKey))
	        	{
	        		if(contextProperties.get(contextKey) instanceof JSONArray)
					{
						((JSONArray) contextProperties.get(contextKey)).put(contextValue);
					}
					else
					{
						JSONArray contextValArray = new JSONArray();
						contextValArray.put(contextProperties.get(contextKey));
						contextValArray.put(contextValue);
						contextProperties.put(contextKey, contextValArray);
					}
	        	}
	        	else
	        	{
	        		contextProperties.put(contextKey, contextValue);
	        	}
			}
        }
        catch (Exception e) 
        {
			e.printStackTrace();
			throw(e);
		}
		
		return contextProperties;
	}
	
	private void addContextProperty(String key)
	{
		if(!this.context.containsKey(key)) 
		{
			JSONObject jsonObj = new JSONObject();
			jsonObj.put("@type", "@id");
			this.context.put(key, jsonObj);
		}
	}
	
	private String getJsonLDKey(String key)
    {
    	switch(key)
    	{
	    	case "type":
	    		return "@type";
	    	case "id":
	    		return "@id";
    		default:
    			return key;
    	}
    }
}
