package com.sas.viya.nifi;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.flowfile.attributes.*;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;
import org.omg.CORBA_2_3.portable.InputStream;

@SuppressWarnings("unused")
public class TestPushToCasProcessor {

	final String TEST_INPUT_DATA = "col1,col2,col3\r\n1,brian,smith\r\n2,ellena,smith\r\n";
	
	@Test
	public void testProcess() throws IOException {
		final TestRunner runner = TestRunners.newTestRunner(new PushToCASProcessor());
		
		runner.setProperty(PushToCASProcessor.CAS_SERVER, "cascontroller.hls-viya.sashq-d.openstack.sas.com");
		runner.setProperty(PushToCASProcessor.CAS_SERVER_PORT, "5570");
		runner.setProperty(PushToCASProcessor.CAS_USER, "brwsmi");
		runner.setProperty(PushToCASProcessor.CAS_PASSWORD, "demopw");
		runner.setProperty(PushToCASProcessor.CASLIB, "Public");
		runner.setProperty(PushToCASProcessor.CAS_TABLE, "brwsmi_code_test");
		runner.setProperty(PushToCASProcessor.APPEND_TABLE, "false");
		runner.setProperty(PushToCASProcessor.GUESS_ROWS, "25");
		
		java.io.InputStream stream = new FileInputStream("c:\\temp\\bank_customers.txt");
		
		String fileText = IOUtils.toString(stream);
		
		runner.enqueue(fileText);
		
		runner.run();
		runner.assertTransferCount(PushToCASProcessor.SUCCESS_RELATIONSHIP, 1);
	}
}
