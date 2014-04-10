package org.apache.cassandra.metadata;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class MetadataRegistry extends Metadata{

    private static final Logger logger = LoggerFactory.getLogger(Schema.class);
    
    public static final MetadataRegistry instance = new MetadataRegistry();
			
	public RowMutation add(String target, String dataTag, String adminTag) {
		long timestamp = FBUtilities.timestampMicros();

		RowMutation rm = new RowMutation(Metadata.MetaData_KS, ByteBufferUtil.bytes(target)); // row key

		ColumnFamily cf = rm.addOrGet(CFMetaData.MetadataRegistryCf);
		cf.addColumn(Column.create("", timestamp, dataTag, ""));
		cf.addColumn(Column.create(ByteBufferUtil.bytes(adminTag), timestamp, dataTag, "admin_tag"));
		
		return rm;
	}
	
	public RowMutation drop(String target) {
		long timestamp = FBUtilities.timestampMicros();
		RowMutation rm = new RowMutation(Metadata.MetaData_KS, ByteBufferUtil.bytes(target)); // row key

		ColumnFamily cf = rm.addOrGet(CFMetaData.MetadataRegistryCf);
		int ldt = (int) (System.currentTimeMillis() / 1000);
		
		cf.delete(new DeletionInfo(timestamp, ldt));
				
		return rm;
	}
	
	public String query(String target, String dataTag){
		String value = null;
		ColumnFamily cf = queryStorage(target, dataTag);
		if (cf != null) {
				value = new String(cf.getColumn(Column.decomposeName(dataTag, "admin_tag")).value().array());
		} 
		return value;
	}
	
	private ColumnFamily queryStorage(String target, String dataTag) {
		Table table = Table.open(Metadata.MetaData_KS);
		QueryFilter filter = QueryFilter.getSliceFilter(
				StorageService.getPartitioner().decorateKey(ByteBufferUtil.bytes(target)),
				new QueryPath(Metadata.MetadataRegistry_CF),
				Column.decomposeName(dataTag, "admin_tag"),
				Column.decomposeName(dataTag, "admin_tag"),
				false,
				Integer.MAX_VALUE);
		
		return table.getColumnFamilyStore(Metadata.MetadataRegistry_CF).getColumnFamily(filter);
	}
}
