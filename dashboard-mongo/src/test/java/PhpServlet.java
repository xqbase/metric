import javax.servlet.annotation.WebInitParam;
import javax.servlet.annotation.WebServlet;

import com.xqbase.util.servlet.FastCGIServlet;

@WebServlet(urlPatterns="*.php", initParams={
	@WebInitParam(name="command", value="D:\\PHP\\PHP-CGI.EXE -b"),
	@WebInitParam(name="addresses", value="localhost:9000,localhost:9001,localhost:9002"),
})
public class PhpServlet extends FastCGIServlet {
	private static final long serialVersionUID = 1L;
}