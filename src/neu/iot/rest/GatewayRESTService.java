package neu.iot.rest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import io.client.kafka.KafkaProducerClient;

/**
 * Is a REST service example Responses 1** = informational 2** = success 3** =
 * redirect 4** = client error 5** = server error
 */
@Path("/test")
public class GatewayRESTService {

	@GET
	@Path("/about")
	@Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
	public Response get() {
		String result = "This is a REST api http service for json messages";
		return Response.status(200).entity(result).build();
	}
	
	@POST
	@Path("/test/{message}/{topic}")
	@Produces({MediaType.TEXT_PLAIN, MediaType.TEXT_HTML})
	public Response test1(@PathParam("message")String message, @PathParam("topic")String topic) {
		
		String result = "This is a REST resource  http service:   message="+message+" topic="+topic;
		
		if(message==null || topic ==null){
			result = "Error:   message="+message+" topic="+topic;
			return Response.status(417).entity(result).build();
		}
		return Response.status(200).entity(result).build();
	}
	
	@POST
	@Path("/test2")
	@Produces({MediaType.TEXT_PLAIN, MediaType.TEXT_HTML})
	public Response test2(@QueryParam("message")String message, @QueryParam("topic")String topic) {
		
		String result = "This is a REST resource  http service:   message="+message+" topic="+topic;
		
		if(message==null || topic ==null){
			result = "Error:   message="+message+" topic="+topic;
			return Response.status(417).entity(result).build();
		}
		return Response.status(200).entity(result).build();
	}
	
	@POST
	@Path("/verify")
	@Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
	@Consumes(MediaType.TEXT_PLAIN)
	public Response verifyRESTService(InputStream incomingData) {
		String result = "CrunchifyRESTService Successfully started..";

		// return HTTP response 200 in case of success
		return Response.status(200).entity(result).build();
	}

	@POST
	@Path("/queueString")
	@Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
	public Response post(String message) {
		try {
			System.out.println("Recieved message ->" + message);
			KafkaProducerClient kafka =  KafkaProducerClient.singleton();
			kafka.send(message);
		} catch (IOException e) {

			String result = "Unable to POST -> message =" + message + " because of " + e.getLocalizedMessage();
			return Response.status(500).entity(result).build();
		}
		String result = "SUCCESS http service POST -> message =" + message;
		return Response.status(200).entity(result).build();
	}
	
	
	@POST
	@Path("/queueStringToTopic/{message}/{topic}")
	@Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
	public Response post(@PathParam("message")String message, @PathParam("topic")String topic) {
		String result = null;
		try {
			System.out.println("Recieved message ->" + message);
			KafkaProducerClient kafka =  KafkaProducerClient.singleton();
			result = kafka.send(message,topic);
		} catch (IOException e) {

			result = "Unable to POST -> message =" + message + " because of " + e.getLocalizedMessage();
			return Response.status(500).entity(result).build();
		}
		result = "SUCCESS http service POST -> message =" + message+": kafaka -> "+result;
		return Response.status(200).entity(result).build();
	}

	@POST
	@Path("/queueJson/topic/{topic}")
	@Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
	public Response post(@PathParam("topic")String topic,InputStream jsonStream) {
		System.out.println("Recieved json message");
		StringBuilder crunchifyBuilder = new StringBuilder();
		String result = "";
		try {
			BufferedReader in = new BufferedReader(new InputStreamReader(jsonStream));
			String line = null;
			while ((line = in.readLine()) != null) {
				crunchifyBuilder.append(line);
			}

			System.out.println("Data Received: " + crunchifyBuilder.toString());

			KafkaProducerClient kafka = KafkaProducerClient.singleton();;
			
			if(topic!=null){
				 result = kafka.send(crunchifyBuilder.toString(),topic);
			}else{
				kafka.send(crunchifyBuilder.toString());
			}
		} catch (IOException e) {

			 result = "Unable to POST -> message =" + crunchifyBuilder.toString() + " because of "
					+ e.getLocalizedMessage();
			return Response.status(500).entity(result).build();
		}

		return Response.status(200).entity(result).build();
	}
	
	@POST
	@Path("/queueJson")
	@Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
	public Response post(InputStream jsonStream) {
		System.out.println("Recieved json message");
		StringBuilder crunchifyBuilder = new StringBuilder();

		try {
			BufferedReader in = new BufferedReader(new InputStreamReader(jsonStream));
			String line = null;
			while ((line = in.readLine()) != null) {
				crunchifyBuilder.append(line);
			}

			System.out.println("Data Received: " + crunchifyBuilder.toString());

			KafkaProducerClient kafka =  KafkaProducerClient.singleton();
			kafka.send(crunchifyBuilder.toString());
		} catch (IOException e) {

			String result = "Unable to POST -> message =" + crunchifyBuilder.toString() + " because of "
					+ e.getLocalizedMessage();
			return Response.status(500).entity(result).build();
		}
		String result = "SUCCESS http service POST -> message =" + crunchifyBuilder.toString();
		return Response.status(200).entity(result).build();
	}

}