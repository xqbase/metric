import javax.servlet.ServletException;

import org.apache.catalina.Context;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.Server;
import org.apache.catalina.WebResourceRoot;
import org.apache.catalina.connector.Connector;
import org.apache.catalina.loader.WebappLoader;
import org.apache.catalina.startup.Tomcat;
import org.apache.catalina.webresources.DirResourceSet;
import org.apache.catalina.webresources.StandardRoot;

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
			Context ctx = tomcat.addWebapp("", Conf.getAbsolutePath("../src/main/webapp"));
			// Ensure to Load All Classes in the Same Class Loader
			ctx.setLoader(new WebappLoader(Startup.class.getClassLoader()) {
				@Override
				public ClassLoader getClassLoader() {
					return Startup.class.getClassLoader();
				}
			});
			WebResourceRoot resources = new StandardRoot(ctx);
			resources.addPreResources(new DirResourceSet(resources,
					"/WEB-INF/classes", Conf.getAbsolutePath("classes"), "/"));
			ctx.setResources(resources);

			Server server = tomcat.getServer();
			server.start();
			server.setPort(8005);
			server.await();
			server.stop();
		} catch (ServletException | LifecycleException e) {
			Log.e(e);
		}
	}
}