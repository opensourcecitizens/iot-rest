package neu.iot.rest;



import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
//import org.glassfish.jersey.jetty.JettyHttpContainerFactory;
//import org.glassfish.jersey.server.ResourceConfig;

import com.google.common.io.Resources;


/**
 * Main class for this package. main method of this class starts up a jetty instance.
 * 
 * @author kndungu
 */
public class JettyServerRun
{
	private Properties properties = null;
	
	/**
	 * Constructor
	 * expects a properties file in package, jettyserver.props that has two properties:
	 * 		context.path
	 * 		service.port
	 * 
	 * @throws IOException
	 */
	public JettyServerRun() throws IOException{
		InputStream props = Resources.getResource("jettyserver.props").openStream();
		properties = new Properties();
		properties.load(props);
	}
	
    /**
     * Constructor
	 * 
     * @param propsfile that has two properties:
     * 		context.path
	 * 		service.port
     * @throws IOException
     */
    public JettyServerRun(File propsfile) throws IOException {
    	InputStream props = new FileInputStream(propsfile);
		properties = new Properties();
		properties.load(props);
	}

	/**
	 * main method
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String args[]) throws Exception 
    {
       if(args!=null && args.length > 0 ){
    	   File propsfile  = new File(args[0]);
    	   if(propsfile.exists()){
    		   new JettyServerRun(propsfile).start();
    	   }else{
    		   System.err.println("File "+args[0]+" does not exist or is not reachable");
    	   }
    	   
       }else
    	new JettyServerRun().start();
    }
    
    /**
     * Service initialization method
     * 
     * @throws Exception
     */
    public void start() throws Exception{
    	    	
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath(properties.getProperty("context.path"));
        
        ServletHolder servlet = context.addServlet(com.sun.jersey.spi.container.servlet.ServletContainer.class, "/*");
        servlet.setInitOrder(1);
        servlet.setInitParameter( "com.sun.jersey.config.property.packages",GatewayRESTOneIdJsonService.class.getPackage().getName());
        Server server = new Server(Integer.parseInt(properties.getProperty("service.port")));
        server.setHandler(context);       
        
        server.start();
        server.join();
    }


}
