package com.sas.viya.nifi;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;

import com.sas.cas.CASConstants;
import com.sas.cas.CASException;
import com.sas.cas.actions.sessionProp.SetSessOptOptions;
import com.sas.cas.actions.table.AddTableOptions;
import com.sas.cas.actions.table.Addtablevariable;
import com.sas.cas.actions.table.DropTableOptions;
import com.sas.cas.actions.table.TableExistsOptions;
import com.sas.cas.events.CASEventListener;
import com.sas.cas.io.CSVDataTagHandler;
import com.sas.cas.messages.CASMessageHeader;
import com.sas.cas.CASActionResults;
import com.sas.cas.CASAuthenticatedUserInfo;
import com.sas.cas.CASClient;
import com.sas.cas.CASClientInterface;
import com.sas.cas.CASException;
import com.sas.cas.CASTable;
import com.sas.cas.CASTable.OutputType;
import com.sas.cas.CASValue;

@SuppressWarnings("unused")
public class AddCSVFile extends BaseProcessor {

	
	private CASClientInterface client;
	private InputStream inputStream = null;
	
	public void setStream (InputStream stream) {
		this.inputStream = stream;
	}
	
	public String executeProcessor (InputStream stream) throws Exception {
		
		this.setStream(stream);
		
		try {
			return this.invokeProcessor();
		}
		catch (Exception ex) {
			throw ex;
		}
		finally {
			this.close();
		}
	}
	
	public String doWork() throws CASException, IOException {
					
			String tableName = getProperty("table");
						
			InputStream is1 = null;
			
			// copy the input stream in order to have it available for loading to CAS
			if (this.inputStream != null ) {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				IOUtils.copy(this.inputStream,baos);
				byte[] content = baos.toByteArray();
				is1 = new ByteArrayInputStream(content);
			}
			
			boolean header = true;
			boolean promote = true;
			
			CSVDataTagHandler h;
			
			if(this.inputStream == null)
				return "Stream not available";
			else
				h = new CSVDataTagHandler(is1, CSVDataTagHandler.getDelimiter(","), header, true);
			
			
			String append = this.getProperty("append");
			
			// If append = false then delete the table, you cannot overwrite a promoted table
			if (append.equalsIgnoreCase("false")) {
				DropTableOptions doptions = new DropTableOptions();
				doptions.setCaslib(getProperty("caslib"));
				doptions.setName(tableName);
				doptions.setQuiet(true);
								
				try {
					getClient().invoke(doptions);
				}
				catch (CASException ex) {
					ex.printStackTrace();
					ex.printLogEvents();
					
					return "Unable to delete table: " + tableName;
				}
			}
			
			// set the variables for the CAS table
			Addtablevariable[] vars = h.guessCSVVars(25);
								
			int offset = 0;
			for (int i = 0; i < vars.length; i++) {
				Addtablevariable var = vars[i];
				var.setOffset(offset);
				offset += var.getLength();
			}
						
			/* Determine if table exists, if exists do not promote else promote */
			TableExistsOptions teoptions = new TableExistsOptions();
			teoptions.clear();
			teoptions.setName(tableName);
			
			CASActionResults<CASValue> results = getClient().invoke(teoptions);
			
			CASValue doesExist = results.getResult(0);
			
			AddTableOptions options = new AddTableOptions();
			options.setTable(tableName);
			options.setVars(vars);
			options.setRecLen(offset);
			
			if (doesExist.getValueAsInteger() < 1)
				options.setPromote(promote);
			options.setReplace(false);
			options.setAppend(true);
			options.setMessageTagHandler(CASMessageHeader.TAG_DATA, h);
			
			try {
				getClient().invoke(options);
			}
			catch (CASException ex) {
				ex.printStackTrace();
				ex.printLogEvents();
			}
			catch (Exception ex) {
				ex.printStackTrace();
				return ex.getMessage();
			}
			
			String msg = "Processed " + h.getFileLineCount() + " line(s) and " + h.getLineCount() + " row(s)";
			System.out.println(msg);
			return msg;
	}
	
	public void close() {
		if (client != null) {
			try {client.close(true);}
			catch (Exception ex) {
				//ignore
			}
		}
	}
}
