package jsonLd.jsonLd;

import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;

public class Utils {
	
	/* Returns true if url is valid */
    public static boolean isValid(String url) 
    { 
        /* Try creating a valid URL */
        try 
        {
        	url = decodeURL(url);
            new URL(url).toURI(); 
            return true; 
        } 
          
        // If there was an Exception 
        // while creating URL object 
        catch (Exception e) 
        { 
            return false; 
        } 
    }
    
    public static String encodeURL(String url)
    {
    	String encodedURI = "";
    	try
    	{
    		encodedURI = URLEncoder.encode(url, "UTF-8");
    	}
    	catch(UnsupportedEncodingException ex)
    	{
    		ex.printStackTrace();
    	}
    	return encodedURI;
    }
    
    public static String decodeURL(String encodedURL)
    {
    	String decodedURI = "";
    	try
    	{
    		decodedURI = URLDecoder.decode(encodedURL, "UTF-8");
    	}
    	catch(UnsupportedEncodingException ex)
    	{
    		ex.printStackTrace();
    	}
    	return decodedURI;
    }
}
