package eu.stratosphere.sopremo.base;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.junit.Test;

import com.amazonaws.util.json.JSONArray;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;

import eu.stratosphere.sopremo.io.JsonParser;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IObjectNode;

public class GetDocumentsTest {

	@Test
	public void test() {
		try {
			File f = new File("/home/hendrikheller/test_json.txt");
			FileReader reader = new FileReader(f);
			char[] input = new char[(int) f.length()]; 
			reader.read(input);
			StringBuilder builder = new StringBuilder();
			builder.append(input);
			String strInput = builder.toString();
			
			JSONObject obj = new JSONObject(strInput);
			JSONArray docArray = obj.getJSONArray("docs");
			
			
			System.out.println(docArray.length());
			for(int i = 0; i < docArray.length(); i++) {
				
			}
			
			
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
