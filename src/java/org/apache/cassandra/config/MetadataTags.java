package org.apache.cassandra.config;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DeletedColumn;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.SystemTable;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetadataTags {
	 private static final Logger logger = LoggerFactory.getLogger(MetadataTags.class);
	 
	 public static final ByteBuffer ColumnDrop_Tag = ByteBufferUtil.bytes("column_drop_tag");
	 public static final String ColumnDrop_Dropped = "dropped";
	 public static final String ColumnDrop_PermanentDropp = "permanent_drop";
	 
	 private ByteBuffer tag;
	 private String targetObject;
	 private String value;
	 
	 public MetadataTags(ByteBuffer tag, String... targetObjects){
		 this.tag = tag;
		 
		 assert targetObjects.length > 0;
		 this.targetObject = targetObjects[0];
		 for(int idx=1; idx<targetObjects.length; idx++){
			 this.targetObject += "." + targetObjects[idx];
		 }
		 
		 value = generateValue();
	 }
	 
	 public String getValue(){ return value; }
	 
	 private String generateValue(){
		 String value = "";
		 
		 if(tag.equals(ColumnDrop_Tag)){	
			ColumnFamily cf = queryTag();
			if (cf != null) {
				try{
				assert cf.getColumnCount() == 1;
				ByteBuffer colValue;
				colValue = cf.getColumn(Column.decomposeName(targetObject, "value")).value();
				if( !colValue.equals(ByteBufferUtil.bytes(ColumnDrop_Dropped)) ) throw new Exception();
				value = ColumnDrop_Dropped;
				}catch(Exception ex){
					value = "";
				}
			} 
		 }
		 
		 return value;
	 }
		
	 private RowMutation addTag(long timestamp)
	 {
        RowMutation rm = new RowMutation(Table.SYSTEM_KS, tag);  // row key
        ByteBuffer createdAt = LongType.instance.decompose(timestamp / 1000);
 
        ColumnFamily cf = rm.addOrGet(CFMetaData.MetadataTagsCf);
       
        cf.addColumn(Column.create("", timestamp, targetObject, ""));
        
        if(tag.equals(ColumnDrop_Tag)){
        	if(value.equals("")){
        		value = ColumnDrop_Dropped;
        	}else if(value.equals(ColumnDrop_Dropped)){
        		value = ColumnDrop_PermanentDropp;
        	}else{
        		assert false; // through some meaningful error
        	}
        }
        
        cf.addColumn(Column.create(value, timestamp, targetObject, "value"));
        cf.addColumn(Column.create(createdAt, timestamp, targetObject, "created_at"));
       
        return rm;
	 }
	 
	 private RowMutation dropTag(long timestamp)
	 {
        RowMutation rm = new RowMutation(Table.SYSTEM_KS, ColumnDrop_Tag);  // row key
 
        ColumnFamily cf = rm.addOrGet(CFMetaData.MetadataTagsCf);
        int ldt = (int) (System.currentTimeMillis() / 1000);
       
        cf.addColumn(DeletedColumn.create(ldt, timestamp, targetObject, ""));
        cf.addColumn(DeletedColumn.create(ldt, timestamp, targetObject, "value"));
        cf.addColumn(DeletedColumn.create(ldt, timestamp, targetObject, "created_at"));
       
        return rm;
	 }
	 
	 private ColumnFamily queryTag(){
		 Table table = Table.open(Table.SYSTEM_KS);
         QueryFilter filter = QueryFilter.getSliceFilter( 
        		 StorageService.getPartitioner().decorateKey(ColumnDrop_Tag),
        		 new QueryPath(SystemTable.MetadataTags_CF),
             	 Column.decomposeName(targetObject, "value"),
             	 Column.decomposeName(targetObject, "value"),
             	 false,
             	 Integer.MAX_VALUE);
        return table.getColumnFamilyStore(SystemTable.MetadataTags_CF).getColumnFamily(filter);
	 }
	 
	 public RowMutation toSchemaUpdate(MetadataTags newState, long modificationTimestamp)
	 {
		 return newState.addTag(modificationTimestamp);
	 }
	 
	 public RowMutation dropFromSchema(MetadataTags newState, long modificationTimestamp)
	 {
		 return newState.dropTag(modificationTimestamp);
	 }
}
