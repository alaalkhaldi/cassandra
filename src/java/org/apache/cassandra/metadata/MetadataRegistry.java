package org.apache.cassandra.metadata;

import java.util.Map;
import java.util.HashMap;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DeletedColumn;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.SystemTable;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class MetadataRegistry {

    private static final Logger logger = LoggerFactory.getLogger(Schema.class);
    
    public static final MetadataRegistry instance = new MetadataRegistry();
       
	public static final String AlterKeyspace_Tag = "a_ks";
	public static final String DropKeyspace_Tag = "d_ks";
	
	public static final String AlterColumnFamily_Alter_Tag = "a_cf_al";
	public static final String AlterColumnFamily_Add_Tag = "a_cf_ad";
	public static final String AlterColumnFamily_Drop_Tag = "a_cf_d";
	public static final String AlterColumnFamily_Rename_Tag = "a_cf_r";
	public static final String AlterColumnFamily_Prob_Tag = "a_cf_p";
	public static final String DropColumnFamily_Tag = "d_cf";
	public static final String TruncateColumnFamily_Tag = "t_cf";
	
	
	private final Map<String, Map<String, String>> registryCache = new HashMap<String, Map<String, String>>();
	
	public MetadataRegistry()
	{}
	
	public RowMutation add(String target, String dataTag, String adminTag) {
		long timestamp = FBUtilities.timestampMicros();

		RowMutation rm = new RowMutation(Table.SYSTEM_KS, ByteBufferUtil.bytes(target)); // row key

		ColumnFamily cf = rm.addOrGet(CFMetaData.MetadataRegistryCf);
		cf.addColumn(Column.create("", timestamp, dataTag, ""));
		cf.addColumn(Column.create(ByteBufferUtil.bytes(adminTag), timestamp, dataTag, "admin_tag"));
		
		// insert into cache
		writeToCache(target, dataTag, adminTag);

		return rm;
	}
	
	public RowMutation drop(String target) {
		long timestamp = FBUtilities.timestampMicros();
		RowMutation rm = new RowMutation(Table.SYSTEM_KS, ByteBufferUtil.bytes(target)); // row key

		ColumnFamily cf = rm.addOrGet(CFMetaData.MetadataRegistryCf);
		int ldt = (int) (System.currentTimeMillis() / 1000);
		
		cf.delete(new DeletionInfo(timestamp, ldt));
		
		// delete from cache
		deleteFromCache(target);
		
		return rm;
	}
	
	
	public String query(String target, String dataTag){
		// only check the cache
		if(registryCache.containsKey(target))
			if(registryCache.get(target).containsKey(dataTag))
				return registryCache.get(target).get(dataTag );
		
		return null;
	}
	
	private ColumnFamily queryStorage(String target, String dataTag) {
		Table table = Table.open(Table.SYSTEM_KS);
		QueryFilter filter = QueryFilter.getSliceFilter(
				StorageService.getPartitioner().decorateKey(ByteBufferUtil.bytes(target)),
				new QueryPath(SystemTable.MetadataRegistry_CF),
				Column.decomposeName(dataTag, "admin_tag"),
				Column.decomposeName(dataTag, "admin_tag"),
				false,
				Integer.MAX_VALUE);
		
		return table.getColumnFamilyStore(SystemTable.MetadataRegistry_CF).getColumnFamily(filter);
	}
	 
	// Cache functions
	private void writeToCache(String target, String dataTag, String adminTag){
		if(registryCache.containsKey(target)){
			registryCache.get(target).put(dataTag, adminTag);
		}
		else{
			Map<String, String> value = new HashMap<String, String>();
			value.put(dataTag, adminTag);
			registryCache.put(target, value);
		}
	}
	
	private void deleteFromCache(String target){
		if(registryCache.containsKey(target)) 
			registryCache.remove(target);
	}
}
