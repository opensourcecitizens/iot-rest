package neu.iot.rest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.Consumes;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import io.client.kafka.KafkaProducerClient;

/**
 * This service consumes messages to be transfered to a kafka topic. 
 * It is therefore a web interface for kakfa topics or a cloud gateway for the data pipeline. 
 * This service has been set aside for OneId as a customer for the stream processing API.
 * Example Responses 1** = informational 2** = success 3** =
 * redirect 4** = client error 5** = server error
 * 
 * @author kndungu
 */
@Path("/OneIdRESTService")
public class GatewayRESTOneIdJsonService {
	Logger log = Logger.getLogger(GatewayRESTOneIdJsonService.class);
	
	/**
	 * Get function that shows available methods.
	 * @return Response
	 */
	@GET
	@Path("/about")
	@Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
	public Response get() {
		String result = "This is a REST api http service for json messages. Supported URIs:"
				+ "\n '/queue/json?message={json_message}' "
				+ "\n '/queue/json/stream?message={json_message}'"
				+ "\n SECURITY: Required header params : 'Authentication'";
		return Response.status(200).entity(result).build();
	}
	
	private String topic = "in.oneid.topic";
	private String userid = "oneid";
	
	/**
	 * POST function that shows available methods.
	 * '/queue/json?message={json_message}' -h 'Authentication:<jwt token>'
	 * 
	 * @param authentication
	 * @param message
	 * @return Response
	 */
	@POST
	@Path("/queue/json")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces({MediaType.APPLICATION_JSON})
	public String postParams(@HeaderParam("Authentication")String authentication,@QueryParam("message")String message) {
		
		return sendToKafka(authentication,message, topic, userid);
	}
	
	
	/**
	 * POST function that shows available methods.
	 * '/queue/json/stream'  -d {json message} -h 'Authentication:<jwt token>'
	 * 
	 * @param authentication
	 * @param jsonStream
	 * @return Response
	 */	
	@POST
	@Path("/queue/json/stream")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces({MediaType.TEXT_PLAIN})
	public String postParamInputStream(@HeaderParam("Authentication")String authentication,InputStream jsonStream) {
		System.out.println("Recieved json message");
		StringBuilder crunchifyBuilder = new StringBuilder();
		String result = "";
		
			BufferedReader in = new BufferedReader(new InputStreamReader(jsonStream));
			String line = null;
			try{
				while ((line = in.readLine()) != null) {
					crunchifyBuilder.append(line);
				}
			
			}catch(Exception e){

				 result = "Unable to POST "
						+ e.getLocalizedMessage();
				 log.error(e, e);
				try {
					return new ObjectMapper().writeValueAsString(Response.status(500).entity(result).build());
				} catch (Throwable e1) {
					
					log.error(e,e);
				}
			
			}
			
			return sendToKafka(authentication,crunchifyBuilder.toString(), topic, userid);	
	}

	
	/**
	 * This method sends a message to kafka. It creates a JSON wrapper message with payload, sourceid,authentication and messagetype attributes.
	 * 
	 * @param authentication
	 * @param message
	 * @param topic
	 * @param userid
	 * @return
	 */
	private String sendToKafka(String authentication, String message,  String topic, String userid) {
		
		String result = "Successufully queued message of length="+message.length()+" on topic="+topic;
		Map<String, Object> data = new HashMap<String,Object>();
		data.put("payload", message);
		data.put("sourceid", userid);
		data.put("authentication", authentication);
		data.put("messagetype", "TELEMETRY");
		
		ObjectMapper mapper = new ObjectMapper();
		
		try{
			
			String json = mapper.writeValueAsString(data);
			@SuppressWarnings("unchecked")
			KafkaProducerClient<String> kafka = (KafkaProducerClient<String>) KafkaProducerClient.singleton();
			
			if(topic!=null){
				result = kafka.send(json,topic);
			}else{
				Future<RecordMetadata> res = kafka.send(json);
			}
			
		} catch (IOException e) {

			 result = "Unable to queue message of length =" + message.length() + " because of "
					+ e.getLocalizedMessage();
			 log.error(e,e);
			try {
				return mapper.writeValueAsString(Response.status(500).entity(result).build());
			} catch (Throwable e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				log.error(e,e);
			}
		}

		try {
			return mapper.writeValueAsString(Response.status(200).entity(result).build());
		} catch (Throwable e) {
			log.error(e,e);
		} 
		return "";
	}

}