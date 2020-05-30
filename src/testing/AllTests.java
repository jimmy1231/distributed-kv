package testing;

import java.io.IOException;

import org.apache.log4j.Level;

import app_kvServer.Server;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;


public class AllTests {

	static {
		try {
			new LogSetup("logs/testing/test.log", Level.ERROR);
			new Server(50000, 10, "FIFO");
		} catch (IOException e) {
			e.printStackTrace();
		}
		catch (Exception e){
			// null for now to avoid compile error
		}
	}
	
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		clientSuite.addTestSuite(ConnectionTest.class);
		clientSuite.addTestSuite(InteractionTest.class); 
		clientSuite.addTestSuite(AdditionalTest.class);
		return clientSuite;
	}
	
}
