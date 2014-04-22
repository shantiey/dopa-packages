
package eu.stratosphere.sopremo.base;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;

import eu.stratosphere.sopremo.io.JsonParseException;
import eu.stratosphere.sopremo.io.JsonParser;
import eu.stratosphere.sopremo.type.BigIntegerNode;
import eu.stratosphere.sopremo.type.DecimalNode;
import eu.stratosphere.sopremo.type.DoubleNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.LongNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;



/**
 * This is a test version with the main function for operator DataImportDataMarket, 
 * which convert a json input in a meta data in json file 
 * and the content in a csv file 
 * 
 * @author T.Shan
 *
 */
@SuppressWarnings("deprecation")
public class DataMarketImportBeta {
	
	static String test="[{\"Country\":\"Germany\",\"Year\":2011,\"GDP\":81751602.50},{\"Country\":\"Germany\",\"Year\":2012,\"GDP\":80327900},{\"Country\":\"Germany\",\"Year\":2012,\"GDP\":80523746.001}]";
	static String testNodeType="[{\"Country\":\"Germany\"}]";
	static String myAccess="a40905e3e9f241ad897f6412e22c29f4";
	
	
	/**
	 * try to convert something link this:
	 *
	 * curl -H "X-DataMarket-Secret-Key: $APIKEY" 
	 * -F datapackage.json=@datapackage.json \
	 * -F countrypops.csv=@countrypops.csv \
	 * https://datamarket.com/import/job/
	 * 
	 * e.g. $APIKEY=a40905e3e9f241ad897f6412e22c29f4 
	 * @throws MalformedURLException 
	 */
	public static void postUrlConnect(String apiKey, String fileName, String content, String typ) throws MalformedURLException  {
		// build a file with content
		try {
		    // Create temp file.
		    File temp = File.createTempFile(fileName, typ);

		    // Delete temp file when program exits.
		    temp.deleteOnExit();

		    // Write to temp file
		    BufferedWriter out = new BufferedWriter(new FileWriter(temp));
		    out.write(content);
		    out.close();
		} catch (IOException e) {
		}
	
		//set up connection
		String param = "value";
		File textFile = new File("/path/to/file.txt");
		File binaryFile = new File("/path/to/file.bin");
		String boundary = Long.toHexString(System.currentTimeMillis()); 
		// generate some unique random value.
		String CRLF = "\r\n"; // Line separator required by multipart/form-data.
		URL url=new URL("https://datamarket.com/import/job/");
		URLConnection connection;
		try {
			connection = url.openConnection();
			connection.setDoOutput(true);
			connection.setRequestProperty("Content-Type","multipart/form-data; boundary=" + boundary);
		
			PrintWriter writer = null;
			OutputStream output = connection.getOutputStream();
		    writer = new PrintWriter(new OutputStreamWriter(output, content),true);// true = autoFlush
		
		    // Send normal param.
		    writer.append("--" + boundary).append(CRLF);
		    writer.append("Content-Disposition: form-data; name=\"param\"").append(CRLF);
		    writer.append("Content-Type: text/plain; charset=" + content).append(CRLF);
		    writer.append(CRLF);
		    writer.append(param).append(CRLF).flush();
		    // Send text file.
		    writer.append("--" + boundary).append(CRLF);
		    writer.append(
		        "Content-Disposition: form-data; name=\"textFile\"; filename=\""
		            + textFile.getName() + "\"").append(CRLF);
		    writer.append("Content-Type: text/plain; charset=" + content).append(CRLF);
		    writer.append(CRLF).flush();
		    BufferedReader reader = null;
		    reader = new BufferedReader(new InputStreamReader(
			        new FileInputStream(textFile), content));
		    for (String line; (line = reader.readLine()) != null;) {
	            writer.append(line).append(CRLF);
	        }
		    reader.close();
		    
		    writer.flush();
		    // Send binary file.
		    writer.append("--" + boundary).append(CRLF);
		    writer.append(
		        "Content-Disposition: form-data; name=\"binaryFile\"; filename=\""
		            + binaryFile.getName() + "\"").append(CRLF);
		    writer.append(
		        "Content-Type: "
		            + URLConnection.guessContentTypeFromName(binaryFile
		                    .getName())).append(CRLF);
		    writer.append("Content-Transfer-Encoding: binary").append(CRLF);
		    writer.append(CRLF).flush();
		    InputStream input = null;
		    input = new FileInputStream(binaryFile);
			
			  
	        byte[] buffer = new byte[1024];
	        for (int length = 0; (length = input.read(buffer)) > 0;) {
	            output.write(buffer, 0, length);
	        }
	        output.flush(); // Output cannot be closed. Close of
	                        // writer will close output as well.
	        input.close();
	        
	        writer.append(CRLF).flush(); // CRLF indicates end of binary boundary.
            
		    // End of multipart/form-data.
		    writer.append("--" + boundary + "--").append(CRLF);
		    
		    if (writer != null) writer.close();
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		    }
	
	}
	
	
	public static void postData0(String apiKey, String meta, String csv)  {
//		HttpPost httppost = new HttpPost("https://datamarket.com/import/job/");
		HttpPost httppost = new HttpPost("https://datamarket.com/lod/datasets/ds-id/view[.json]/");
		
		
		// add the cintent to a file, which is allowed to approach DM
		List<BasicNameValuePair> parameters = new ArrayList<BasicNameValuePair>();

	
	    parameters.add(new BasicNameValuePair("metaTest.json", meta));
//	    parameters.add(new BasicNameValuePair("datapackage.csv", csv));   //upload Strig/var.
	    
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

			httppost.setEntity(new UrlEncodedFormEntity(parameters));
			 
//			 post.setEntity(new FileEntity(file, "apply.json"));
			 HttpResponse httpResponse = client.execute(httppost);
			 HttpEntity resEntity = httpResponse.getEntity();
			 
			 // Get the HTTP Status Code
			    int statusCode = httpResponse.getStatusLine().getStatusCode();
			    System.out.println("HTTP Status Code: "+statusCode);
			    
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
	
	
	
	@SuppressWarnings({ "unchecked" })	
	/**
	 *e.g. data income:[{"Country":"Germany","Year":2011,"GDP":81751602.50},
	 *                 {"Country":"Germany","Year":2012,"GDP":80327900},
	 *                 {"Country":"Germany","Year":2012,"GDP":80523746.001}
	 *                 ]
	 *converted meta-data: { "name": "Country;GDP;Year;", 
	 *						"resources":[ {"path": "...", 
	 *									   "schema": {"fields":[
	 *														{ "id": "Country", "type": "String"},
	 *														{ "id": "GDP", "type": "Decimal"},
	 *														{ "id": "Year", "type": "Integer"}]}
	 *							}]}
	 */
	public static void convertInput(String dscontent) {
		    String csvString="";
			String firstRowCsv="";
			String oneMetaField="";
			//in case to write the data in a file later, each row is saving here as a element in the list content
			LinkedList<String> content = new LinkedList<String>();
			
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
        	String t="";
        	while(iterator.hasNext()){
        		Entry<String, IJsonNode> e=iterator.next();
        		String k=e.getKey(); 
        
        		firstRowCsv+=k+"|";
        	       		
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
	           	t+=meta+",";
	    //     	 System.out.println(t);
	        }
        	String fields=t.substring(0, t.lastIndexOf(","));
        	oneMetaField="["+fields+"]";
 //       	TextNode fieldsNode=new TextNode(oneMetaField);
 //       	SimpleImmutableEntry<String,IJsonNode> e_fields=new SimpleImmutableEntry <String,IJsonNode>("fields",fieldsNode);
 //       	SimpleImmutableEntry<String,IJsonNode> e_path=new SimpleImmutableEntry <String,IJsonNode>("path",null);
        	String metaFields="\"fields\":"+oneMetaField;
        	String schema="\"schema\": {"+metaFields+"}";
        	//TODO Path in meta file
        	String p="...";
        	String path="\"path\": \""+p+"\"";
        	String resource="\"resources\":[ {"+path+", "+schema+"}]";
        	//TODO generate the name        	
        	String name="\"name\": \""+ firstRowCsv+"\"";
        	String finalMeta="{ "+name+", "+resource+"}"; 
        	System.out.println("the meta data looks like:");
        	System.out.println(finalMeta);          	           	
        	        	
        	content.add(firstRowCsv);
           	csvString=firstRowCsv+"\n";  
     
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
        		content.add(row);
        		csvString+=row+"\n";
        	}
        	System.out.println("the csv data looks like:");
        	System.out.println("in list: "+content.toString());
        	System.out.println("in String: ");
        	System.out.println("    "+csvString);
 
        	// save all csv content into a array/list of String, then in BufferedWriter
    	/*
		try {
			FileOutputStream outStream = new FileOutputStream("result.csv");
			OutputStreamWriter wr = new OutputStreamWriter(outStream);
		    BufferedWriter br = new BufferedWriter(wr);
		    
		    int j;
	        for(j=0;j<content.size();j++){
	        	  
	            br.write(content.get(j));	            
	        }
		} catch (IOException  e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
         	 */
        
	 }	
	

	public static String convertInputCsv(String dscontent) {
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
    	System.out.println("the csv data looks like:");
    	System.out.println("in Array: "+contentCsv.toString());
   
		return csvString;
}
	
	
	public static void postFile(String apiKey, String jsonContent, String csvContent)  {
		HttpPost httppost = new HttpPost("https://datamarket.com/import/job/");
		httppost.addHeader("X-DataMarket-Secret-Key", apiKey);
		
		// add the content to a file, which is allowed to approach DM
		File fileJson=writeInFiles(".json", jsonContent);
		File fileCsv=writeInFiles(".csv", csvContent);
		
		System.out.println("check up a tmperary file by the datatype "+fileJson.toString());
   
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
			
	//		FileEntity fileEntity=new FileEntity(file, "text/json, application/json");
			FileBody fb0=new FileBody(fileJson);	
			FileBody fb1=new FileBody(fileCsv);
			MultipartEntityBuilder multipartEntity = MultipartEntityBuilder.create();        
		    multipartEntity.setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
			
//		    multipartEntity.addBinaryBody("datapackage.json", fileJson);
		    multipartEntity.addPart("datapackage.json", fb0); //<input type="file" name="datapackage.json"/>  
//		    multipartEntity.addPart("countrypops.csv", fb1);
		    
		    httppost.setEntity(multipartEntity.build());
			
			HttpResponse httpResponse = client.execute(httppost);
			
			System.out.println("->>>>>>>>>>>"  + httppost.containsHeader("X-DataMarket-Secret-Key"));
			System.out.println("->>>>>>>>>>>"  + httppost.getEntity().getContentType());
			System.out.println("->>>>>>>>>>>"  + httppost);
		    System.out.println("##########" + httpResponse.toString());
			
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
	
	public static File writeInFiles(String type, String content){
		File file = null ;		
			try  {
				//create a temporary file
				if (type.contains(".json")){				
					file = File.createTempFile("tmpJsonMeta", ".json");				
				}else if (type.contains(".csv")){
					file = File.createTempFile("tmpCsv", ".csv");
				}else{
					System.out.println("Please entert a file type of json/csv.");
				}
				FileOutputStream fop = new FileOutputStream(file);
				// get the content in bytes
				byte[] contentInBytes = content.getBytes();
	 
				fop.write(contentInBytes);
				fop.flush();
				fop.close();
	 
				System.out.println("A"+type+"file has been Done.");
	 
			} catch (IOException e) {
				e.printStackTrace();
			}	
		return file;
	}

	public static File writeLocalFile(String type, String content){
		File file = null ;
		if (type.contains(".json")){	
			file = new File("c:/jtest/newfile.json");
		}else if (type.equals(".csv")){
			file = new File("c:/jtest/newfile.csv");
	//			file = File.createTempFile("countrypops.csv", type);   //".csv"	
		}else{
			System.out.println("Please entert a file type of json/csv.");
		}		
			try (FileOutputStream fop = new FileOutputStream(file)) {
				 
				// if file doesn't exists, then create it
				if (!file.exists()) {
					file.createNewFile();
				}
	 
				// get the content in bytes
				byte[] contentInBytes = content.getBytes();
	 
				fop.write(contentInBytes);
				fop.flush();
				fop.close();
	 
				System.out.println("Done");
	 
			} catch (IOException e) {
				e.printStackTrace();
			}	
			System.out.println(file.exists());
		return file;
	}
	 
	 public static void main(String[] args) throws ClientProtocolException, IOException, KeyManagementException, NoSuchAlgorithmException, KeyStoreException{ 
	       
	//	 testing convert data	 	
//		 System.out.println("converting data ... "); 	        
//		 convertInputCsv(test);	 
		 
		 System.out.println("Incoming data: "+test); 
//	     writeLocalFile(".csv",test);  
//	   postData0(myAccess,testNodeType,"a,b,c");
//	   postFile(myAccess,test,".json");
		 
		 String [] m=test.split("},");
		 System.out.println(m[2]); 
	    } 
}
