package eu.stratosphere.sopremo.base;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.template.GenericInputSplit;
import eu.stratosphere.pact.common.contract.GenericDataSink;
import eu.stratosphere.pact.common.contract.GenericDataSource;
import eu.stratosphere.pact.common.io.statistics.BaseStatistics;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.generic.io.OutputFormat;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.SopremoEnvironment;
import eu.stratosphere.sopremo.base.DataMarketAccess.DataMarketInputFormat;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.io.JsonParseException;
import eu.stratosphere.sopremo.io.JsonParser;
import eu.stratosphere.sopremo.operator.ElementaryOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.Property;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.serialization.SopremoRecord;
import eu.stratosphere.sopremo.serialization.SopremoRecordLayout;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.BigIntegerNode;
import eu.stratosphere.sopremo.type.DecimalNode;
import eu.stratosphere.sopremo.type.DoubleNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.LongNode;
import eu.stratosphere.sopremo.type.NullNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * The operator converts a json input file into a csv format
 * and generates a meta data as well, at last saves the data in Datamarket.
 * @author shan
 */
@Name(verb = "datamarketimport")
@InputCardinality(0)
public class DataMarketImport  extends ElementaryOperator<DataMarketImport>{

	   protected static final String DM_URL_PARAMETER = "ser_dm_export_parameter";
	   protected static final String DM_API_KEY_PARAMETER = "ser_api_key_parameter";
	   
	    private IJsonNode scriptNode = null;
	    private String  scriptString=null;
	    private String dmApiKeyString = null;
	    
	    
	 public static class DataMarketOutputFormat implements OutputFormat<SopremoRecord> {

			private EvaluationContext context;
	        private String incomingString;
	        private String apiKey;
	        int teskNumber;
	        
//	        Iterator<IJsonNode> nodeIterator;

			@Override
			public void configure(Configuration parameters) {
				this.context = SopremoUtil.getEvaluationContext(parameters);
	            SopremoEnvironment.getInstance().setEvaluationContext(context);
	            incomingString = parameters.getString(DM_URL_PARAMETER, null);
				apiKey = parameters.getString(DM_API_KEY_PARAMETER, null);
	          
			}

			public void setTaskNumber(){
				if (incomingString.isEmpty()){
					 this.teskNumber=0;
				}else{
					this.teskNumber=1;
				}
			}
			@Override
			public void open(int taskNumber) throws IOException {
				
				if (taskNumber==0) {
					System.out.println("There is no job in DataMarketImport currently");					
					
				} else {
					// convert record format
					DataConverter dc=new DataConverter();
					String meta = dc.convertInputMeta(this.incomingString);
					String csv=dc.convertInputCsv(incomingString);
					
					//post to DataMarket
					postFile(apiKey, meta, "dataPackage", ".json");
					postFile(apiKey, csv, "dataCsv", ".csv");
				}
				
			}

			/**
			 * Adds a record to the output.			 * 
			 * @param record:The records to add to the output.
			 */
			@Override
			public void writeRecord(SopremoRecord record) throws IOException {
				// TODO Auto-generated method stub				
			}

			/**
			 * Method that marks the end of the life-cycle of parallel output instance. Should be used to close
			 * channels and streams and release resources.
			 * After this method returns without an error, the output is assumed to be correct.
			 * <p>
			 * When this method is called, the output format it guaranteed to be opened.
			 *  
			 * @throws IOException Thrown, if the input could not be closed properly.
			 */
			@Override
			public void close() throws IOException {
				// TODO Auto-generated method stub
				
			}
			
			@SuppressWarnings("deprecation")
			public static void postFile(String apiKey, String content, String fileName, String fileType)  {
				HttpPost httppost = new HttpPost("https://datamarket.com/import/job/");
				httppost.addHeader("X-DataMarket-Secret-Key", apiKey);
				
				// add the content to a file, which is allowed to approach DM
				File file = null ;
				try {
				    // Create temp file.
					file = File.createTempFile(fileName, fileType);   //".json"

				    // Delete temp file when program exits.
					file.deleteOnExit();

				    // Write to temp file
				    BufferedWriter out = new BufferedWriter(new FileWriter(file));
				    out.write(content);
				    out.close();
				} catch (IOException e) {
				}
				   
		    //SSL decode
			    SSLContextBuilder builder = new SSLContextBuilder();
			    try {
					builder.loadTrustMaterial(null, new TrustStrategy() {				
						@Override
						public boolean isTrusted(X509Certificate[] chain, String authType)
								throws CertificateException {
							// TODO Auto-generated method stub
							return true;
						}
					});
				} catch (NoSuchAlgorithmException e1) {
					e1.printStackTrace();
				} catch (KeyStoreException e1) {
					
					e1.printStackTrace();
				}
			    
			    SSLConnectionSocketFactory fac; 	     
				try {
					fac = new SSLConnectionSocketFactory(builder.build());
					CloseableHttpClient client = HttpClients.custom().setSSLSocketFactory(fac).build();
				    httppost.setEntity(new FileEntity(file, "text/"+fileType+" , application/"+fileType)); //"text/json, application/json"
				//  CloseableHttpResponse response = client.execute(httppost); //post String directly				
				    HttpResponse httpResponse = client.execute(httppost);
					HttpEntity resEntity = httpResponse.getEntity();
							 				 							
					// Get the HTTP Status Code					  
					int status = httpResponse.getStatusLine().getStatusCode();					    
					System.out.println("HTTP Status : "+status);				    
							    
					// Get the contents of the response					    
					InputStream input;									
					input = resEntity.getContent();		
					String responseBody = IOUtils.toString(input);					    
					input.close();
							 
					// Print the response code and message body					    
					System.out.println(responseBody);						    
						
				} catch (UnsupportedEncodingException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();						
				}catch (IllegalStateException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();						
				} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();						
				}catch (KeyManagementException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();						
				} catch (NoSuchAlgorithmException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();						
				}		       
			  }
			
			
			protected class DataConverter{
				
				public String convertInputMeta(String dscontent) {
				    
					String oneMetaField="";
										
					// write a meta data
					JsonParser parser = new JsonParser(dscontent);
					
					IArrayNode<IJsonNode>data= null;
				    try {
				    	data =(IArrayNode<IJsonNode>) parser.readValueAsTree();
					} catch (JsonParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				    //check the types of input by first row
				    ObjectNode da=(ObjectNode) data.get(0);		    
		        	Iterator<Entry<String, IJsonNode>> iterator=da.iterator();
		        	
		        	String meta="";
		        	String tmpmeta="";
		        	while(iterator.hasNext()){
		        		Entry<String, IJsonNode> e=iterator.next();
		        		String k=e.getKey(); 		        
		        			        	       		
			        	 IJsonNode tmp=e.getValue();
			        	 
			        	 if (tmp.getType().isInstance(new TextNode())){
			        		 meta="{ \"id\": \""+k+"\", \"type\": \"String\"}";
			        	 }else if(tmp.getType().isInstance(new IntNode())|tmp.getType().isInstance(new BigIntegerNode())){
			        		 meta="{ \"id\": \""+k+"\", \"type\": \"Integer\"}";
			        	 }else if(tmp.getType().isInstance(new DoubleNode())){
			        		 meta="{ \"id\": \""+k+"\", \"type\": \"Double\"}";
			        	 }else if(tmp.getType().isInstance(new DecimalNode())){
			        		 meta="{ \"id\": \""+k+"\", \"type\": \"Decimal\"}";
			        	 }else if(tmp.getType().isInstance(new LongNode())){
			        		 meta="{ \"id\": \""+k+"\", \"type\": \"Long\"}";
			        	}
			           	tmpmeta+=meta+",";			
			        }
		        	String fields=tmpmeta.substring(0, tmpmeta.lastIndexOf(","));
		        	oneMetaField="["+fields+"]";
		 //       	TextNode fieldsNode=new TextNode(oneMetaField);
		 
		        	String metaFields="\"fields\":"+oneMetaField;
		        	String schema="\"schema\": {"+metaFields+"}";
		        	
		        	//TODO Path in meta file
		        	String p="...";
		        	String path="\"path\": \""+p+"\"";
		        	String resource="\"resources\":[ {"+path+", "+schema+"}]";
		        	//TODO generate the name        	
		        	String name="\"name\": \" MetaData";
		        	String finalMeta="{ "+name+", "+resource+"}"; 
		        	System.out.println("the meta data looks like:");
		        	System.out.println(finalMeta);      
		        	return finalMeta;
				}
				
				public String convertInputCsv(String dscontent) {
				    String csvString="";
					String firstRowCsv="";					
					//in case to write the data in a file later, each row is saving here as a element in the list content
					LinkedList<String> contentCsv = new LinkedList<String>();
					
					// read the input 
					JsonParser parser = new JsonParser(dscontent);
					
					IArrayNode<IJsonNode>data= null;
				    try {
				    	data =(IArrayNode<IJsonNode>) parser.readValueAsTree();
					} catch (JsonParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			
		           	//writing the content in csv format		        	
		           	int i;
		        	for (i=0;i<data.size();i++){
		        		String row="";
		        		ObjectNode tuple=(ObjectNode) data.get(i);
		        		Iterator<Entry<String, IJsonNode>> oneTuple=tuple.iterator();
		        		while(oneTuple.hasNext()){
		        			String tmp=oneTuple.next().getValue().toString();
		        			row+=tmp+"|";        		
		        		}
		        		contentCsv.add(row);
		        		csvString+=row+"\n";
		        	}
	//	        	System.out.println("the csv data looks like:");
	//	        	System.out.println("in String: "+csvString);
					return csvString;
			}
			}

		}
	
	
	
	@Override
	public int hashCode() {
		final int prime = 37;
		int result = super.hashCode();
		result = prime * result;
		return result;
	}

	@Override
	public PactModule asPactModule(EvaluationContext context, SopremoRecordLayout layout) {
		GenericDataSink contract = new GenericDataSink( //GenericDataSink
		DataMarketOutputFormat.class, String.format("DataMarket %s", scriptNode.toString()));

		final PactModule pactModule = new PactModule(1, 0);  //output,input
        SopremoUtil.setEvaluationContext(contract.getParameters(), context);
        SopremoUtil.setLayout(contract.getParameters(), layout);
        
        contract.getParameters().setString(DM_URL_PARAMETER, scriptNode.toString());

        pactModule.getOutput(0).setInput(contract);      //?
		return pactModule;
	}

	@Property(preferred = true)
	@Name(noun = "from")
	public void setImportParameter(EvaluationExpression value) {
		if (value == null)
			throw new NullPointerException("value expression must not be null");
		
		scriptNode = value.evaluate(NullNode.getInstance());	
		scriptString=scriptNode.toString();

	}

	@Property(preferred = false)
	@Name(noun = "key")
	public void setKeyParameter(EvaluationExpression value) {
		if (value == null)
			throw new NullPointerException("value expression must not be null");
		IJsonNode node = value.evaluate(NullNode.getInstance());
		dmApiKeyString = node.toString();
	}

   

}
