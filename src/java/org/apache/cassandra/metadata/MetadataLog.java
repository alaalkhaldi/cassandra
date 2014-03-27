package org.apache.cassandra.metadata;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.Column;	
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DeletedColumn;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class MetadataLog {
	
	public static RowMutation add(String target, long time, String client, String tag, String value, String adminTag) {
		long timestamp = FBUtilities.timestampMicros();

		// attach admin data
		value += new MetricsCollector(target, adminTag).getMetrics();
		
		RowMutation rm = new RowMutation(Table.SYSTEM_KS, ByteBufferUtil.bytes(target)); // row key

		ColumnFamily cf = rm.addOrGet(CFMetaData.MetadataLogCf);
		cf.addColumn(Column.create("", timestamp, String.valueOf(time), client, tag, ""));
		cf.addColumn(Column.create(ByteBufferUtil.bytes(value), timestamp, String.valueOf(time), client, tag, "value"));

		return rm;
	}
	
	public static RowMutation drop(String target, long time, String client, String tag, String value) {
		long timestamp = FBUtilities.timestampMicros();
		RowMutation rm = new RowMutation(Table.SYSTEM_KS, ByteBufferUtil.bytes(target)); // row key

		ColumnFamily cf = rm.addOrGet(CFMetaData.MetadataLogCf);
		int ldt = (int) (System.currentTimeMillis() / 1000);
		
		cf.addColumn(DeletedColumn.create(ldt, timestamp, String.valueOf(time), client, tag, ""));
		cf.addColumn(DeletedColumn.create(ldt, timestamp, String.valueOf(time), client, tag, "value"));

		return rm;
	}
}
