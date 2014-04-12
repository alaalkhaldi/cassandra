package org.apache.cassandra.metadata;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DeletedColumn;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class MetadataLog extends Metadata {
	
	public static RowMutation add(String target, long time, String client, String tag, String value, String adminTag) {
		long timestamp = FBUtilities.timestampMicros();

		// attach admin data
		//value += new MetricsCollector(target, adminTag).getMetrics();
		
		RowMutation rm = new RowMutation(Metadata.MetaData_KS, ByteBufferUtil.bytes(target)); // row key

		ColumnFamily cf = rm.addOrGet(CFMetaData.MetadataLogCf);
		cf.addColumn(Column.create("", timestamp, String.valueOf(time), client, tag, ""));
		cf.addColumn(Column.create(ByteBufferUtil.bytes(value), timestamp, String.valueOf(time), client, tag, "value"));

		return rm;
	}
	
	public static RowMutation drop(String target, long time, String client, String tag) {
		long timestamp = FBUtilities.timestampMicros();
		RowMutation rm = new RowMutation(Metadata.MetaData_KS, ByteBufferUtil.bytes(target)); // row key

		ColumnFamily cf = rm.addOrGet(CFMetaData.MetadataLogCf);
		int ldt = (int) (System.currentTimeMillis() / 1000);
		
		cf.addColumn(DeletedColumn.create(ldt, timestamp, String.valueOf(time), client, tag, ""));
		cf.addColumn(DeletedColumn.create(ldt, timestamp, String.valueOf(time), client, tag, "value"));

		return rm;
	}
	
	public static void announce(String target, String dataTag, ClientState state, String logValue) {
		String client = (state == null)? "" : state.getUser().getName();
		announce(target, dataTag, client, logValue);
	}
	
	public static void announce(String target, String dataTag, String client, String logValue) {
		if(client == null) client = "";
    	
    	String exists = MetadataRegistry.instance.query(target, dataTag);
    	if(exists == null){
    		return;
    	}
    	
    	mutate(MetadataLog.add(target, FBUtilities.timestampMicros(), client, dataTag, logValue, ""));
	}
}
