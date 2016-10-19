package neu.iot.rest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import io.client.kafka.KafkaProducerClient;
import io.parser.avro.AvroUtils;

/**
 * Is a REST service example Responses 1** = informational 2** = success 3** =
 * redirect 4** = client error 5** = server error
 */
@Path("/")
public class GatewayRESTJsonService {
	Logger log = Logger.getLogger(GatewayRESTJsonService.class);
	
	@GET
	@Path("/about")
	@Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
	public Response get() {
		String result = "This is a REST api http service for json messages. Supported URIs:"
				+ "\n '/queue/json?topic={topic}&userid={userid}&message={json_message}' "
				+ "\n '/queue/json/topic/{topic}?userid={userid}&message={json_message}'"
				+ "\n '/queue/json/stream/topic/{topic}?userid={userid}&message={json_message}'"
				+ "\n SECURITY: Required header params : 'Authentication'";
		return Response.status(200).entity(result).build();
	}
	
	@GET
	@Path("/queue")
	@Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
	public Response verify() {
		String result = "This is a REST api http service: supports POST for '/queue/json' ";
		return Response.status(200).entity(result).build();
	}
	

	@POST
	@Path("/queue/json")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
	public String postQuery(@HeaderParam("Authentication")String authentication,@QueryParam("message")String message, @QueryParam("topic")String topic, @QueryParam("userid")String userid) {
		
		return sendToKafka(authentication,message, topic, userid);
	}
	

	
	@POST
	@Path("/queue/json/topic/{topic}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
	public String postParams(@HeaderParam("Authentication")String authentication,@QueryParam("message")String message, @PathParam("topic")String topic, @QueryParam("userid")String userid) {
		
		return sendToKafka(authentication,message, topic, userid);
	}
	

	@POST
	@Path("/queue/json/stream/topic/{topic}/")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
	public String postParamInputStream(@HeaderParam("Authentication")String authentication,@PathParam("topic")String topic,InputStream jsonStream, @QueryParam("userid")String userid) {
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
	
	
	@POST
	@Path("/queue/avro/stream/topic/{topic}/")
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
	@Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
	public String postParamInputByteStream(@HeaderParam("Authentication")String authentication,@PathParam("topic")String topic,InputStream jsonStream, @QueryParam("userid")String userid) {
		System.out.println("Recieved json message");
		//StringBuilder crunchifyBuilder = new StringBuilder();
		String result = "";
			BufferedReader is = new BufferedReader(new InputStreamReader(jsonStream));
			//InputStreamReader is = new InputStreamReader(in.);
			byte[] buf = null;
			try{
				
				 buf = IOUtils.toByteArray(is);

			
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
			

			
			return sendUnwrapped2Kafka(authentication,buf, topic, userid);	
	}
	
	
	private String sendUnwrapped2Kafka(String authentication, byte[] message,  String topic, String userid) {
		
		String result = "Successufully queued message of length="+message.length+" on topic="+topic;

		System.out.println( "Successufully queued message of length="+message.length );
		ObjectMapper mapper = new ObjectMapper();
		
		try{
			
			Schema schema = new Schema.Parser().parse(new URL("https://s3-us-west-2.amazonaws.com/iot-dev-avroschema/registry-to-spark/versions/current/remoterequest.avsc").openStream());

			GenericRecord record = AvroUtils.avroToJava(message, schema);
			System.out.println(record);
			message = AvroUtils.serializeJava(record, schema);
			
			KafkaProducerClient<byte[]> kafka = new KafkaProducerClient<byte[]>();
			
			if(topic!=null){
				result = kafka.send(message,topic);
			}else{
				Future<RecordMetadata> res = kafka.send(message);
			}
			
		} catch (IOException e) {

			 result = "Unable to queue message of length =" + message.length + " because of "
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
	
	
	private <T> String sendToKafka(String authentication, T message,  String topic, String userid) {
		
		String result = "Successufully queued message ="+message+" on topic="+topic;
		Map<String, Object> data = new HashMap<String,Object>();
		data.put("payload", message);
		data.put("sourceid", userid);
		data.put("authentication", authentication);
		data.put("messagetype", "TELEMETRY");
		
		ObjectMapper mapper = new ObjectMapper();
		
		try{
			
			String json = mapper.writeValueAsString(data);
			KafkaProducerClient kafka = KafkaProducerClient.singleton();
			
			if(topic!=null){
				result = kafka.send(json,topic);
			}else{
				Future<RecordMetadata> res = kafka.send(json);
			}
			
		} catch (IOException e) {

			 result = "Unable to queue message =" + message + " because of "
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