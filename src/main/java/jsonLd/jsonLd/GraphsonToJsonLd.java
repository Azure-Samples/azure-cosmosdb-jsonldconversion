package jsonLd.jsonLd;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.json.JSONObject;

public class GraphsonToJsonLd {

	public void generateGraph()
	{
		// TODO Auto-generated method stub
		
		GraphProvider dbProvider = new GraphProvider();
        
        GraphParser parser = new GraphParser(dbProvider);
        
        String vertexquery = "g.V()";
        
        List<Result> vertices = dbProvider.read(vertexquery);
        
		JSONObject graph = parser.parse(vertices);
        
        try (FileWriter file = new FileWriter("JsonLD.json"))
        {

            file.write(graph.toString());
            file.flush();
            System.out.println("Succesfully converted to JsonLd.");
        } 
        catch (IOException e) 
        {
            e.printStackTrace();
        }
	}
}
