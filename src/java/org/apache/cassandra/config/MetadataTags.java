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
	 public static final ByteBuffer ColumnDrop_Dropped = ByteBufferUtil.bytes("dropped");
	 public static final ByteBuffer ColumnDrop_PermanentDropp = ByteBufferUtil.bytes("permanent_drop");
	 
	 private ByteBuffer tag;
	 private ByteBuffer value;
	 private ByteBuffer createdAt;
	 private String targetObject;
	 
	 public ByteBuffer getTag(){ return tag; }
	 public ByteBuffer getValue(){ return value; }
	 public ByteBuffer getCreatedAt(){ return createdAt; }
	 public String getTargetObject(){return targetObject; }
	 
	 public MetadataTags(ByteBuffer tag, String... targetObjects){
		 this.tag = tag;
		 
		 assert targetObjects.length > 0;
		 this.targetObject = targetObjects[0];
		 for(int idx=1; idx<targetObjects.length; idx++){
			 this.targetObject += "." + targetObjects[idx];
		 }
		 
		 ColumnFamily cf = queryTag();
		 
		 value = generateValue(cf);
		 createdAt = generateCreatedAt(cf);
	 }
	 	 	
	 public RowMutation addTag(long timestamp)
	 {
        RowMutation rm = new RowMutation(Table.SYSTEM_KS, tag);  // row key
        createdAt = LongType.instance.decompose(timestamp / 1000);
 
        ColumnFamily cf = rm.addOrGet(CFMetaData.MetadataTagsCf);
       
        cf.addColumn(Column.create("", timestamp, targetObject, ""));
        
        if(tag.equals(ColumnDrop_Tag)){
        	if(value == null){
        		value = ColumnDrop_Dropped;
        	}else if(value.equals(ColumnDrop_Dropped)){
        		value = ColumnDrop_PermanentDropp;
        	}else{
        		value = ColumnDrop_Dropped;
        	}
        }
        
        cf.addColumn(Column.create(value, timestamp, targetObject, "value"));
        cf.addColumn(Column.create(createdAt, timestamp, targetObject, "created_at"));
       
        return rm;
	 }
	 
	 public RowMutation dropTag(long timestamp)
	 {
        RowMutation rm = new RowMutation(Table.SYSTEM_KS, tag);  // row key
 
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
        		 StorageService.getPartitioner().decorateKey(tag),
        		 new QueryPath(SystemTable.MetadataTags_CF),
             	 Column.decomposeName(targetObject, "created_at"),
             	 Column.decomposeName(targetObject, "value"),
             	 false,
             	 Integer.MAX_VALUE);
        return table.getColumnFamilyStore(SystemTable.MetadataTags_CF).getColumnFamily(filter);
	 }
	 
	 private ByteBuffer generateValue(ColumnFamily cf){
		 ByteBuffer value = null;
		 if(tag.equals(ColumnDrop_Tag)){	
			if (cf != null) {
				//assert cf.getColumnCount() == 1;
				value = cf.getColumn(Column.decomposeName(targetObject, "value")).value();
			} 
		 }
		 return value;
	 }
	 
	 private ByteBuffer generateCreatedAt(ColumnFamily cf){
		 ByteBuffer createdAt = null;
		 if(tag.equals(ColumnDrop_Tag)){	
			if (cf != null) {
				//assert cf.getColumnCount() == 1;
				createdAt = cf.getColumn(Column.decomposeName(targetObject, "created_at")).value();
			} 
		 }
		 return createdAt;
	 }
}
