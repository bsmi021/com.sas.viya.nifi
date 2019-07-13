package com.sas.viya.nifi;

import java.io.IOException;
import java.io.InputStream;
import java.text.MessageFormat;
import java.util.Properties;

import com.sas.cas.CASClient;
import com.sas.cas.CASClientInterface;
import com.sas.cas.CASException;
import com.sas.cas.actions.sessionProp.SetSessOptOptions;

@SuppressWarnings("unused")
public abstract class BaseProcessor {

	private CASClientInterface client;
	private Properties properties = new Properties();
	
	
	public abstract String doWork() throws Exception;
	
	
	public Properties getProperties() {
		return this.properties;
	}
	
	public void setClient(CASClientInterface client) {
		this.client = client;
	}
	
	public CASClientInterface getClient() {
		return this.client;
	}
	
	
	public String invokeProcessor() throws Exception{
		createTarget();
		return doWork();
	}
	
	public String invokeProcessor(String[] args) throws Exception{
		
		
		argsToProperties(args,properties);
		createTarget();
		return doWork();
	}
	
	public String getRequiredProperty(String name) throws CASException {
		if (name == null) {
			throw new IllegalArgumentException();
		}
		String value = properties.getProperty(name.toLowerCase());
		if (value == null) {
			String msg = "Missing property";
			Object[] args = new String[] { name };
			throw new CASException(MessageFormat.format(msg, args));
		}
		return value;
	}
	
	public String getProperty(String name) {
		if (name == null) {
			throw new IllegalArgumentException();
		}
		String value = null;
				
		name = name.toLowerCase();
		if (properties.containsKey(name)) {
			value = properties.getProperty(name);
		}
		return value;
	}
	
	public CASClientInterface createCASClient() throws CASException, IOException {
		String host = getRequiredProperty("host");//"cascontroller.hls-viya.sashq-d.openstack.sas.com";

		String port = getProperty("port");
		
		String username = getProperty("username");
		String password = getProperty("password");
		String caslib = getProperty("caslib");
		
		final CASClient client = new CASClient();
		
		client.setHost(host);
		client.setPort(Integer.parseInt(port));
		client.setUserName(username);
		client.setPassword(password);
		
		SetSessOptOptions options = new SetSessOptOptions();
		options.clear();
		options.setCaslib(caslib);
		
		client.invoke(options);
		
		CASClientInterface c = client;
		
		return c;
	}
	
	public void createTarget() throws Exception {
		if (client == null) {
			client = createCASClient();
		}
	}
	
	public void argsToProperties(String[] args, Properties properties) {
		if (args != null) {
			for (int i = 0; i < args.length; i++) {
				String name = args[i];
				String value = null;
				int pos = name.indexOf('='); /*I18nOK:EMC*/
				if (pos >= 0) {
					value = name.substring(pos + 1);
					name = name.substring(1, pos);
				}
				if (name.equals("?") || name.equals("-help")) {
					value = "?";
				}
				if ((value == null) || (value.trim().length() == 0)) {
					properties.remove(name);
				}
				else {
					properties.setProperty(name.toLowerCase(), value);
				}
			}
		}
	}
	
}
