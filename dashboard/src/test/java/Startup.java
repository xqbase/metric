import javax.servlet.ServletException;

import org.apache.catalina.LifecycleException;
import org.apache.catalina.connector.Connector;
import org.apache.catalina.startup.Tomcat;

import com.xqbase.util.Conf;
import com.xqbase.util.Log;

public class Startup {
	public static void main(String[] args) {
		Connector connector = new Connector();
		connector.setPort(80);
		Tomcat tomcat = new Tomcat();
		tomcat.setPort(80);
		tomcat.getService().addConnector(connector);
		tomcat.setConnector(connector);
		try {
			tomcat.addWebapp("", Conf.locate("../src/main/webapp"));
			tomcat.start();
			Thread.currentThread().join();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		} catch (ServletException | LifecycleException e) {
			Log.e(e);
		}
	}
}