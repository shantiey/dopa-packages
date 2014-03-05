
package eu.stratosphere.sopremo.base;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;

import eu.stratosphere.sopremo.io.JsonParseException;
import eu.stratosphere.sopremo.io.JsonParser;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.BigIntegerNode;
import eu.stratosphere.sopremo.type.DecimalNode;
import eu.stratosphere.sopremo.type.DoubleNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IObjectNode;
import eu.stratosphere.sopremo.type.IPrimitiveNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.LongNode;
import eu.stratosphere.sopremo.type.MissingNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * This is a pre-version with the main function of operator DataImportDataMarket, 
 * which convert a json input in a meta data in json file 
 * and the content in a csv file 
 * 
 * @author shan
 *
 */
public class DataMarketImportBeta {
	
	static String test="[{\"Country\":\"Germany\",\"Year\":2011,\"GDP\":81751602.50},{\"Country\":\"Germany\",\"Year\":2012,\"GDP\":80327900},{\"Country\":\"Germany\",\"Year\":2012,\"GDP\":80523746.001}]";
	String testNodeType="[{\"Country\":\"Germany\"}]";
	
	 @SuppressWarnings({ "unchecked", "null" })
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
        	/*
        	 * {"name": "Population of selected countries",
        	 *  "resources":[ {
        	 * 					"path": "countrypops.csv",
        	 * 					"schema": {
        	 * 					TesxNode__>  "fields": [{ "id": "Year", "type": "string" },
        	 * 						  			    	 { "id": "Country", "type": "string" },
        	 * 						   			 { "id": "Population", "type": "number" }]
        	 * 								}
        	 *              } ]
        	 * }
        	 */
        	
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
 
        	//TODO save all csv content into a array/list of String, then in BufferedWriter
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
	 
	 public static void main(String[] args){ 
	       
		 System.out.println("Incoming data: "+test); 
		 System.out.println("converting data ... "); 
	 
	        convertInput(test);	 
	        IJsonNode jnode = null;
	        String a="[{\"Country\":\"Germany\"}]";
	        String b="{Country: String, GDP: Decimal, Year: Integer,}";
	        
	       
	    } 
}
