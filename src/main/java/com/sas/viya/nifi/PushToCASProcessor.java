package com.sas.viya.nifi;

import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


@SuppressWarnings("unused")
public class PushToCASProcessor extends AbstractProcessor {

	  public static final PropertyDescriptor CAS_SERVER = new PropertyDescriptor
				.Builder().name("CAS_SERVER")
				.displayName("CAS Server")
				.description("Host for CAS Controller")
				.required(true)
				.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
				.build();

		    public static final PropertyDescriptor CAS_SERVER_PORT = new PropertyDescriptor
				.Builder().name("CAS_SERVER_PORT")
				.displayName("CAS Server Port")
				.description("Host Port for CAS Controller")
				.required(true)
				.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
				.build();
		    
		    public static final PropertyDescriptor CAS_USER = new PropertyDescriptor
				.Builder().name("CAS_USER")
				.displayName("CAS Username")
				.description("User for executing against CAS")
				.required(true)
				.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
				.build();
		    
		    public static final PropertyDescriptor CAS_PASSWORD = new PropertyDescriptor
				.Builder().name("CAS_PASSWORD")
				.displayName("CAS User Password")
				.sensitive(true)
				.description("Password for user executing against CAS")
				.required(true)
				.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
				.build();
		    
		    public static final PropertyDescriptor CASLIB = new PropertyDescriptor
				.Builder().name("CASLIB")
				.displayName("CAS Library")
				.description("CAS Library to write to")
				.required(true)
				.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
				.build();

		    public static final PropertyDescriptor CAS_TABLE = new PropertyDescriptor
				.Builder().name("CAS_TABLE")
				.displayName("CAS Table")
				.description("CAS Table to be written to")
				.required(true)
				.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
				.build();
		    
		    public static final PropertyDescriptor APPEND_TABLE = new PropertyDescriptor
				.Builder().name("APPEND_TABLE")
				.displayName("Append to table")
				.description("True/False, false will overwrite table in CAS")
				.required(true)
				.defaultValue("true")
				.addValidator(StandardValidators.BOOLEAN_VALIDATOR)
				.build();
		    
		    public static final PropertyDescriptor GUESS_ROWS = new PropertyDescriptor
				.Builder().name("GUESS_ROWS")
				.displayName("Guess rows")
				.description("Provides a count of rows to review for data type mapping")
				.required(false)
				.addValidator(StandardValidators.INTEGER_VALIDATOR)
				.build();
		    
		    public static final Relationship SUCCESS_RELATIONSHIP = new Relationship.Builder()
		            .name("Success")
		            .description("Example relationship")
		            .build();

		    private List<PropertyDescriptor> descriptors;

		    private Set<Relationship> relationships;

		    @Override
		    protected void init(final ProcessorInitializationContext context) {
		        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		        
			descriptors.add(CAS_SERVER);
			descriptors.add(CAS_SERVER_PORT);
			descriptors.add(CAS_USER);
			descriptors.add(CAS_PASSWORD);
			descriptors.add(CASLIB);
			descriptors.add(CAS_TABLE);
			descriptors.add(APPEND_TABLE);
			descriptors.add(GUESS_ROWS);
			
		        this.descriptors = Collections.unmodifiableList(descriptors);

		        final Set<Relationship> relationships = new HashSet<Relationship>();
		        relationships.add(SUCCESS_RELATIONSHIP);
		        this.relationships = Collections.unmodifiableSet(relationships);
		    }

		    @Override
		    public Set<Relationship> getRelationships() {
		        return this.relationships;
		    }

		    @Override
		    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		        return descriptors;
		    }

		    @OnScheduled
		    public void onScheduled(final ProcessContext context) {

		    }

		    @Override
		    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		        
		    	FlowFile flowFile = session.get();
		        
		        if ( flowFile == null ) {
		            return;
		        }
		        
		        final AddCSVFile proc = new AddCSVFile();
		        
		        String host = context.getProperty(CAS_SERVER).getValue();
		        String port = context.getProperty(CAS_SERVER_PORT).getValue();
		        String username = context.getProperty(CAS_USER).getValue();
		        String password = context.getProperty(CAS_PASSWORD).getValue();
		        String caslib = context.getProperty(CASLIB).getValue();
		        String table = context.getProperty(CAS_TABLE).getValue();
		        String append = context.getProperty(APPEND_TABLE).getValue();
		        String guess = context.getProperty(GUESS_ROWS).getValue();
		        
		        proc.getProperties().setProperty("host", host);
		        proc.getProperties().setProperty("port", port);
		        proc.getProperties().setProperty("username", username);
		        proc.getProperties().setProperty("password", password);
		        proc.getProperties().setProperty("caslib", caslib);
		        proc.getProperties().setProperty("table", table);
		        proc.getProperties().setProperty("append", append);
		        proc.getProperties().setProperty("guess", guess);
			        
		        session.read(flowFile, new InputStreamCallback() {
		        	
		        		public void process(InputStream in) throws IOException {
		        		try {
		        			
		        			String msg = proc.executeProcessor(in);
		        			
		        			getLogger().info(msg);
		        		}
		        		catch(Exception ex) {
		        			ex.printStackTrace();
		        			getLogger().error("Failed to send to CAS: " + ex.toString());
		        		}
		        	}
		        });
		        
		        session.transfer(flowFile, SUCCESS_RELATIONSHIP);
		        		
		        
		        // TODO implement
		    }

}
