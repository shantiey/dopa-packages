package eu.stratosphere.sopremo.base;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;

import eu.stratosphere.pact.common.contract.GenericDataSource;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.base.DataMarketAccess.DataMarketInputFormat;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.io.JsonParseException;
import eu.stratosphere.sopremo.io.JsonParser;
import eu.stratosphere.sopremo.operator.ElementaryOperator;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.Property;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.serialization.SopremoRecordLayout;
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

@Name(verb = "datamarketimport")
@InputCardinality(0)
public class DataMarketImport  extends ElementaryOperator<DataMarketImport>{

	   protected static final String DM_URL_PARAMETER = "ser_dm_import_parameter";

	   
	    private IJsonNode importDataNode = null;
	    private String  importDataString=null;

	
	
	private class DataConverter{
		
		public String convertInput(String dscontent) {

			String rowCsv="";
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
        		//collect first row of csv data
        		String k=e.getKey();         
        		rowCsv+=k+";";
        	       		
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
        	String name="\"name\": \""+ rowCsv+"\"";
        	String finalMeta="{ "+name+", "+resource+"}"; 
        	System.out.println("MetaDat: "+finalMeta);          	           	
        	
        	/* Meta File:
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
        	
        	content.add(rowCsv);
 //          	finalCsv+="\n";  
     
        	//writing the content in csv format
        	int i;
        	for (i=0;i<data.size();i++){
        		String row="";
        		ObjectNode tuple=(ObjectNode) data.get(i);
        		Iterator<Entry<String, IJsonNode>> oneTuple=tuple.iterator();
        		while(oneTuple.hasNext()){
        			String tmp=oneTuple.next().getValue().toString();
        			row+=tmp+";";        		
        		}
        		content.add(row);
        	
        	}
 //       	System.out.println(content);
			return finalMeta;
 
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
		GenericDataSource<?> contract = new GenericDataSource<DataMarketInputFormat>(
				DataMarketInputFormat.class, String.format("DataMarket %s", importDataString));

		final PactModule pactModule = new PactModule(0, 1);
        SopremoUtil.setEvaluationContext(contract.getParameters(), context);
        SopremoUtil.setLayout(contract.getParameters(), layout);
        contract.getParameters().setString(DM_URL_PARAMETER, importDataString);


		pactModule.getOutput(0).setInput(contract);
		return pactModule;
	}

	@Property(preferred = true)
	@Name(noun = "from")
	public void setImportParameter(EvaluationExpression value) {
		if (value == null)
			throw new NullPointerException("value expression must not be null");
		importDataNode = value.evaluate(NullNode.getInstance());

		DataConverter dc = new DataConverter ();
		importDataString=dc.convertInput(importDataNode.toString());

//		System.out.println("set urlParameter expression "
//				+ urlParameterNode.toString());
	}



   

}
