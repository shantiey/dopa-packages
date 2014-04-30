package eu.stratosphere.sopremo.base;


import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.template.GenericInputSplit;
import eu.stratosphere.pact.common.contract.GenericDataSource;
import eu.stratosphere.pact.common.io.statistics.BaseStatistics;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.pact.generic.io.InputFormat;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.SopremoEnvironment;
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
import eu.stratosphere.sopremo.type.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

@Name(verb = "datamarketaccess")
@InputCardinality(0)
public class DataMarketVisual extends ElementaryOperator<DataMarketVisual> {

    protected static final String DM_URL_PARAMETER = "ser_dm_url_parameter";
    protected static final String DM_API_KEY_PARAMETER = "ser_api_key_parameter";

    private static IJsonNode urlParameterNode = null;
    private static String  urlParameterNodeString=null;
    private static String dmApiKeyString = null;
 

	public static class DataMarketInputFormat implements InputFormat<SopremoRecord, GenericInputSplit> {

		private EvaluationContext context;

        private String urlParameter;

        private String apiKey;

       	private Iterator<IJsonNode> nodeIterator;

		@Override
		public void configure(Configuration parameters) {
			this.context = SopremoUtil.getEvaluationContext(parameters);
            SopremoEnvironment.getInstance().setEvaluationContext(context);
			urlParameter = parameters.getString(DM_URL_PARAMETER, null);
			           
		}

		/*
		 * (non-Javadoc)
		 *
		 * @see
		 * eu.stratosphere.pact.common.io.GenericInputFormat#createInputSplits
		 * (int)
		 */
		@Override
		public GenericInputSplit[] createInputSplits(final int minNumSplits) throws IOException {
            GenericInputSplit[] splits = new GenericInputSplit[minNumSplits];
            for (int i = 0; i < minNumSplits; i++) {
                splits[i] = new GenericInputSplit(i);
            }
            return splits;
		}

		
		/*
		 * (non-Javadoc)
		 *
		 * @see
		 * eu.stratosphere.pact.common.io.GenericInputFormat#getInputSplitType()
		 */
		@Override
		public Class<GenericInputSplit> getInputSplitType() {
			return GenericInputSplit.class;
		}

        @Override
        public BaseStatistics getStatistics(BaseStatistics baseStatistics) throws IOException {
            return null;
        }

        /*  eu.stratosphere.pact.common.io.GenericInputFormat#open(eu.stratosphere
         * .nephele.template.GenericInputSplit)
         */
                
		@Override
		public void open(final GenericInputSplit split) throws IOException {
			if (split.getSplitNumber() == 0) {
				List<String> response = showDataMarketResponse(this.urlParameter);
				
				// the visualization url is currently set as output 
				ArrayNode<IJsonNode> records = (ArrayNode<IJsonNode>) response.iterator();
				
				this.nodeIterator = records.iterator();
			} else {
				this.nodeIterator = null;
			}
		}

		@Override
		public boolean reachedEnd() throws IOException {
			if (nodeIterator == null) {
				return true;
			}
			return !this.nodeIterator.hasNext();
		}

        @Override
        public boolean nextRecord(final SopremoRecord record) throws IOException {
            if (this.reachedEnd())
                throw new IOException("End of input split is reached");

            final IJsonNode value = this.nodeIterator.next();
            record.setNode(value);
            return true;
        }
        
        @Override
    	public void close() throws IOException {
    		// TODO Auto-generated method stub
    		
    	}
        /**
         * Transform a ds_part into a url for visualization and send the request
         * as response is a HTML page for each set-part
         * @param ds_part
         * @return resultUrls
         */
        private static List<String> showDataMarketResponse(String ds_part) {
        	List<String> resultUrls = null;
    		// switch the request url for visualization
    	    // the incoming data sets are familiar with: 17tm!kqc-3.t.12.s/12rb!e4s-7a-e4t-5
    		String[] ds=ds_part.split("/");
    		
    		for(String aDS : ds){
    			System.out.println("Showing figures for a set-part: "+aDS);
    			String ds_id=aDS.substring(0, aDS.indexOf("!"));  
    			String dim_part=aDS.substring(aDS.indexOf("!")+1);
    		if (dim_part.isEmpty()){
    			System.out.println("The dataset for Visualization-API has to include a data slice specification.");
    		}else{
    		try {
    			String urlstring = "https://datamarket.com/lod/datasets/"+ds_id+"/view/ds-"+dim_part;
    			resultUrls.add(urlstring);
    			System.out.println("Please review the figure with the url below : ");
    			System.out.println("                                             "+urlstring);
    			
    			HttpPost httppost = new HttpPost(urlstring);
    						
    			SSLContextBuilder builder = new SSLContextBuilder();
    			builder.loadTrustMaterial(null, new TrustStrategy() {				
    				@Override
    				public boolean isTrusted(X509Certificate[] chain, String authType)
    						throws CertificateException {
    					// TODO Auto-generated method stub
    					return true;
    				}
    			});
    			SSLConnectionSocketFactory fac= new SSLConnectionSocketFactory(builder.build());
    			CloseableHttpClient client = HttpClients.custom().setSSLSocketFactory(fac).build();

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
    			System.out.println("-->>>>>>>>"+responseBody);				

    			//	System.out.println(result);
    		}catch (NoSuchAlgorithmException e1) {
    			e1.printStackTrace();
    		} catch (Exception e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    		}
    	}	
    		}
    		return resultUrls;
    	}
    	
    	
    	protected static class DatasetParserVis {

    		private Map<String,IArrayNode<IJsonNode>> dimensions = new HashMap<String,IArrayNode<IJsonNode>> ();
    				
    	     public String parseDS(String dscontent) {

    			String finalDS="";

    			// parse content from json file, multiple ds is allowed
    			JsonParser parser = new JsonParser(dscontent);
    			IArrayNode<IObjectNode>datasets = null;
    		    try {
    		    	datasets =(IArrayNode<IObjectNode>) parser.readValueAsTree();
    			} catch (JsonParseException e) {
    				// TODO Auto-generated catch block
    				e.printStackTrace();
    			}
    	  	     //call each dataset like: 12rb!e4s=7a:e4t=5	 or 17tm!kqc=p
    		    for (int i = 0; i < datasets.size(); i++) {

    		    	String di="";

    		    	IJsonNode ds_id=datasets.get(i).get("ds_id");
    		    	//the whole set-part of one category eg. 17tm!kqc-3.t.12.s (17tm!kqc=3.t.12.s)
    		    	String one_ds_id=ds_id.toString();    
    		        if (!(datasets.get(i).get("dimension") instanceof MissingNode)) {
    		        	IObjectNode dimensions=(IObjectNode) datasets.get(i).get ("dimension");
    		        	Iterator<Entry<String, IJsonNode>> iterator=dimensions.iterator();
    			        while(iterator.hasNext()){
    			        	Entry<String, IJsonNode> e=iterator.next();
    			           	TextNode eachDim=(TextNode)checkDimensionSlice(ds_id,e.getKey(),e.getValue());

    			  	    	di+=eachDim+"-";	    	
    			  	    	//   System.out.println(di);   //e4s-7a-e4t-5- instead of e4s=7a:e4t=5:
    			   	    	one_ds_id+="!"+di.substring(0, di.lastIndexOf("-"));  //!e4s-7a-e4t-5
    			       	}
    		        }
    		      
    		       	finalDS+=one_ds_id+"/";     // 17tm!kqc-3.t.12.s/12rb!e4s-7a-e4t-5/
    	     }
    		  // set up the whole (multi-)dataset together
    		    finalDS=finalDS.substring(0, finalDS.lastIndexOf("/"));

    			return finalDS;
    		}
    		/**
    		 * Transfer each dimension in its ds_id
    		 * return a valid form for datamarket Visual API
    		 * {e4s: 7a}   ==> e4s-7a
    		 */
    	    public TextNode checkDimensionSlice(IJsonNode ds, String id, IJsonNode value) {
    	    	 IArrayNode<IJsonNode> dimArray = dimensions.get (ds.toString());
    	    	 TextNode  tmp = null;
    	    	 if (dimArray == null) {
    	    		 // fetch dimensions from DataMarket
    	      		    BufferedReader reader = null;
    	      		    String dsData="";
    	      		    try {
    	      	            URL url = new URL("http://datamarket.com/api/v1/info.json?ds="+ds.toString());
    	      		        reader = new BufferedReader(new InputStreamReader(url.openStream()));
    	      		        StringBuffer buffer = new StringBuffer();
    	      		        int read;
    	      		        char[] chars = new char[1024];

    	      		        while ((read = reader.read(chars)) != -1)
    	      		          buffer.append(chars, 0, read);
    	      		          dsData=buffer.toString();

    	      		    } catch (Exception e) {
    	      				// TODO Auto-generated catch block
    	      				e.printStackTrace();
    	      			}

    	    			// remove Data Market API call around content
    	    			Pattern pattern = Pattern.compile("jsonDataMarketApi\\((.+)\\)", Pattern.DOTALL);
    	    			Matcher matcher = pattern.matcher(dsData);
    	    			if (!matcher.find()) {
    	    				return null;
    	    			}
    	    			String jsoncontent = matcher.group(1);
    	    			JsonParser parser = new JsonParser(jsoncontent);

    	    	 		IArrayNode<IObjectNode> dims = null;
    	      		try {
    	      			dims =(IArrayNode<IObjectNode>) parser.readValueAsTree();

    	      		} catch (JsonParseException e) {
    	      			// TODO Auto-generated catch block
    	      			e.printStackTrace();
    	      		}
    	      		//call each dimension values
    	      	    //values|subValues::  [{id: 6a, title: Eastern Europe}, {id: 3e, iso3166: PT, title: Portugal},.....
    	      		//subValues.get(0)::{id: 6a, title: Eastern Europe}
    	      	    	dimArray =(IArrayNode<IJsonNode>) dims.get(0).get("dimensions");
    	    		    this.dimensions.put(ds.toString(), dimArray);
    	    	 }

    	     		for (int i = 0; i < dimArray.size(); i++) {
    	     	    	IJsonNode d_id_key=((IObjectNode) dimArray.get(i)).get("id");
    	     	    	IJsonNode d_id_value=((IObjectNode) dimArray.get(i)).get("title");
    	     	    	IJsonNode values=((IObjectNode) dimArray.get(i)).get("values");
    	     	    	IArrayNode<IJsonNode> valueArray=(IArrayNode<IJsonNode>)values;

    	     	    	if(id.equals(d_id_value.toString())){
    	     	    		id= d_id_key.toString();
    	     	    	}
    	     	    	 for (int j = 0; j < valueArray.size(); j++) {
    	     	    		 IJsonNode keyValue=((IObjectNode) valueArray.get(j)).get("id");
    	     	    		 IJsonNode valueValue=((IObjectNode) valueArray.get(j)).get("title");

    	     	    		 if(value.equals(valueValue)){
    	     	    			value=keyValue;
    	     	    		 }
    	     	    	//set one dimension
    	    	        tmp=new TextNode(id+"-"+value.toString());
    	     	    	 }
    	     	   }
    	     		return tmp;
    	     	}
    	}

    	
	@Override
	public int hashCode() {
		final int prime = 37;
		int result = super.hashCode();
		result = prime * result;
		return result;
	}

	public PactModule asPactModule(EvaluationContext context, SopremoRecordLayout layout) {
		GenericDataSource<?> contract = new GenericDataSource<DataMarketInputFormat>(
				DataMarketInputFormat.class, String.format("DataMarket %s", urlParameterNodeString));

		final PactModule pactModule = new PactModule(0, 1);
        SopremoUtil.setEvaluationContext(contract.getParameters(), context);
        SopremoUtil.setLayout(contract.getParameters(), layout);
        contract.getParameters().setString(DM_URL_PARAMETER, urlParameterNodeString);
        
      
		pactModule.getOutput(0).setInput(contract);
		return pactModule;
	}

	@Property(preferred = true)
	@Name(noun = "for")
	public void setURLParameter(EvaluationExpression value) {
		if (value == null)
			throw new NullPointerException("value expression must not be null");
		urlParameterNode = value.evaluate(NullNode.getInstance());

		DatasetParserVis ds = new DatasetParserVis();
		urlParameterNodeString=ds.parseDS(urlParameterNode.toString());

	}
    	
}
}
