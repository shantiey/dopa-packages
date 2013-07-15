package eu.stratosphere.sopremo.base;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.io.TableInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.io.SopremoFileFormat;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.Property;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.serialization.Schema;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.LongNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

@Name(noun = "hbase")
public class HbaseFormat extends SopremoFileFormat {

	/**
	 * Initializes HbaseFormat.
	 */
	public HbaseFormat() {
		getConfiguration().setString(TableInputFormat.CONFIG_LOCATION,
			"/etc/hbase/conf/hbase-site.xml");
	}

	@Override
	protected String getPreferredFilenameExtension() {
		return null;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.io.SopremoFileFormat#getInputFormat()
	 */
	@Override
	public Class<? extends eu.stratosphere.pact.common.generic.io.InputFormat<PactRecord, ?>> getInputFormat() {
		return InputFormat.class;
	}

	protected static final String HBASE_SCAN_PARAMETER = "imr_access_scan_parameter";

	private String tablename = null;

	public static class InputFormat extends TableInputFormat {

		private EvaluationContext context;

		/**
		 * Schema loaded from config.
		 */
		private Schema schema;

		@Override
		public void configure(Configuration parameters) {
			super.configure(parameters);
			this.context = (EvaluationContext) SopremoUtil.getObject(
				parameters, SopremoUtil.CONTEXT, null);
			this.schema = this.context.getOutputSchema(0);
		}

		@Override
		public boolean nextRecord(PactRecord record) throws IOException {
			boolean result = nextResult();
			if (result) {
				// create JSonNode with content
				// merge all family:column->value pairs in a result
				// TODO: handle mutliple versions
				// TODO: there are way too many object allocations in this code,
				// we should check whether that makes a difference, though
				ObjectNode value = new ObjectNode();

				Result res = this.hbaseResult.getResult();

				KeyValue[] results = res.raw();
				value.put("row", new TextNode(new String(results[0].getRow())));
				value.put("timestamp", new LongNode(results[0].getTimestamp()));
				for (KeyValue kv : results) {
					String familyString = new String(kv.getFamily());
					// check of current column family is already in node
					IJsonNode family = value.get(familyString);
					if (family.isMissing()) {
						family = new ObjectNode();
						value.put(familyString, family);
					}
					// insert qualifier and value
					((ObjectNode) family).put(new String(kv.getQualifier()),
						new TextNode(new String(kv.getValue())));
				}

				this.schema.jsonToRecord(value, record);
			}
			return result;
		}

	}

	@Override
	public int hashCode() {
		final int prime = 37;
		int result = super.hashCode();
		result = prime * result;
		return result;
	}

	//
	// @Override
	// public PactModule asPactModule(EvaluationContext context) {
	// context.setInputsAndOutputs(0, 1);
	// GenericDataSource<?> contract = new GenericDataSource<IMRInputFormat>(
	// IMRInputFormat.class, String.format("HBaseTable '%s'",
	// tablename.toString()));
	//
	// final PactModule pactModule = new PactModule(0, 1);
	// SopremoUtil.setObject(contract.getParameters(), SopremoUtil.CONTEXT,
	// context);
	// contract.getParameters().setString(TableInputFormat.CONFIG_LOCATION,
	// "/etc/hbase/conf/hbase-site.xml");
	// contract.getParameters().setString(TableInputFormat.INPUT_TABLE,
	// tablename.toString());
	// pactModule.getOutput(0).setInput(contract);
	// return pactModule;
	// }

	@Property(preferred = true)
	@Name(noun = "table")
	public void setURLParameter(String tablename) {
		if (tablename == null) {
			throw new NullPointerException("value expression must not be null");
		}
		this.tablename = tablename;
		getConfiguration().setString(TableInputFormat.INPUT_TABLE, tablename);
	}

	/**
	 * Returns the tablename.
	 * 
	 * @return the tablename
	 */
	public String getURLParameter() {
		return this.tablename;
	}

}
