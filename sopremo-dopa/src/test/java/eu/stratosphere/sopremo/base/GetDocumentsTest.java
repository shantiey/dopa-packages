package eu.stratosphere.sopremo.base;



import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.junit.Test;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;


public class GetDocumentsTest {

	@Test
	public void test() {
		try {
			File f = new File("/Users/Etienne/Testdatei.txt");
			
			@SuppressWarnings("resource")
			FileReader reader = new FileReader(f);
			
			char[] input = new char[(int) f.length()]; 
			reader.read(input);
			
			//create a String out of the input (char[]) 
			StringBuilder builder = new StringBuilder();
			builder.append(input);
			String strInput = builder.toString();
			
			//create a JSONObject out of the given String
			JSONObject obj = new JSONObject(strInput);
			
			
			
			
			//TODO create PactRecord with the JSONObject obj
			
			//TODO to create a PactRecord with the JSONObject obj either create class PactJSONObject or choose PactMap (for example) instead of
			
			PactRecord record = new PactRecord();
			
			//record.setField(0, obj ); 
			
			
			
			
			
			
			
			
			//Tests!!
			//-----------------------------
			
			//print the whole object
			System.out.println(obj);
			
			JSONArray docArray = obj.names();
			//print the names of the object as array
			System.out.println(docArray);
			
			String[] names = JSONObject.getNames(obj.getJSONObject("publisher1"));
			//print the names of "publisher1" as array
			System.out.println(Arrays.toString(names));
			
			
			//iterate over the names of the JSONObject publisher1
			JSONObject publisher = obj.getJSONObject("publisher1");
			Iterator it = publisher.keys();
			while ( it.hasNext() ){
				System.out.println( it.next() );
			}
			
			//-------------------------------
			
			
			
			
			
			
			
			
			
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

}
