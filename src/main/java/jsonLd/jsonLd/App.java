package jsonLd.jsonLd;

public class App {

	public static void main(String[] args) 
	{
		try
		{   
//			System.out.println("Inserting Json LD data into ComosDB");
//			JsonLdIngestionToCosmos jsonldIngestion = new JsonLdIngestionToCosmos();
//			jsonldIngestion.insertData();
			
			System.out.println("Converting CosmosDB data to Json LD");
			GraphsonToJsonLd jsonldGeneration = new GraphsonToJsonLd();
			jsonldGeneration.generateGraph();
			
			System.exit(0);
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(-1);
		}
	}
}
