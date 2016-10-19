import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;

import org.junit.Before;
import org.junit.Test;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;

public class TestREST {
	
	@Before
	public void init(){
		
	}
	
	@Test
	public void testPost() throws IOException{
		//URL url = new URL("http://localhost:8080/DSGatewayWebService/api/Gateway/queue");
		URL url = new URL("http://ec2-52-38-19-146.us-west-2.compute.amazonaws.com:80/DSGatewayWebService/api/Gateway/queue");
		URLConnection connection = url.openConnection();
		connection.setDoOutput(true);
		//connection.setRequestProperty("Content-Type", "application/json");
		connection.setRequestProperty("Content-Type", "text/plain");
		connection.setConnectTimeout(50000);
		connection.setReadTimeout(50000);
		OutputStreamWriter out = new OutputStreamWriter(connection.getOutputStream());
		out.write("kaniu's message");
		out.close();
				
		//response
		BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
		String inStr = null;
		while (( inStr = in.readLine()) != null) {
			System.out.println(inStr);
		}
		System.out.println("\n DSGatewayWebService Gateway REST Service Invoked Successfully..");
		in.close();
	}
	
	@Test
	public void testGet(){
		
		ClientConfig config = new DefaultClientConfig();
		Client client = Client.create(config);
		WebResource webResource = client.resource(UriBuilder.fromUri("http://ec2-52-41-124-186.us-west-2.compute.amazonaws.com:8080").build());
		  
		/*
		MultivaluedMap formData = new MultivaluedMapImpl();
		
		  formData.add('name1', 'val1');
		
		  formData.add('name2', 'val2');
		
		 ClientResponse response = webResource.type(MediaType.APPLICATION_FORM_URLENCODED_TYPE).post(ClientResponse.class, formData);
		*/
		
		ClientResponse response = webResource.
				path("owners/143").
				//path("143").
				type(MediaType.APPLICATION_JSON).
				accept(MediaType.APPLICATION_JSON).
				header("API-KEY", "1").
				get(ClientResponse.class);
		
		System.out.println("Response " + response.getEntity(String.class));
				  

	}

}
