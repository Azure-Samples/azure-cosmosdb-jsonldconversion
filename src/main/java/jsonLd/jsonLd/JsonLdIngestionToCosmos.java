package jsonLd.jsonLd;

import java.io.IOException;
import java.util.Properties;

public class JsonLdIngestionToCosmos {
	
	public void insertData() throws IOException 
	{
		// TODO Auto-generated method stub
         
        /**This logic is to read a json ld file from a path
         *The path is specified inside src/main/resources/pom.properties file*/
        
        Properties properties = new Properties();
        properties.load(JsonLdIngestionToCosmos.class.getResourceAsStream("/pom.properties"));
        
        String filePath = properties.getProperty("file");       
        
        Graph newGraph = new Graph();
        newGraph.buildGraphFromJsonLd(filePath);
        
        /**
         * After connection is successful, execute the nodes creation and then the edges
         * We write the nodes first, and the edges at the end.
         */
        
        GraphProvider dbProvider = new GraphProvider();
        dbProvider.insert(newGraph.vertices);
        dbProvider.insert(newGraph.edges);

        System.out.println("Successfully inserted the graph");
    }
}
