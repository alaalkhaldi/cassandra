package org.apache.cassandra.metadata;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Column;	
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DeletedColumn;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.MigrationManager.MigrationsSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import com.google.common.collect.Iterables;

public class MetadataLog {
	
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
	
	public static RowMutation drop(String target, long time, String client, String tag, String value) {
		long timestamp = FBUtilities.timestampMicros();
		RowMutation rm = new RowMutation(Metadata.MetaData_KS, ByteBufferUtil.bytes(target)); // row key

		ColumnFamily cf = rm.addOrGet(CFMetaData.MetadataLogCf);
		int ldt = (int) (System.currentTimeMillis() / 1000);
		
		cf.addColumn(DeletedColumn.create(ldt, timestamp, String.valueOf(time), client, tag, ""));
		cf.addColumn(DeletedColumn.create(ldt, timestamp, String.valueOf(time), client, tag, "value"));

		return rm;
	}
	
	public static void mutateMetadata(final RowMutation mutation){
		 Runnable runnable = new DroppableRunnable(MessagingService.Verb.MUTATION)
	        {
	        	public void runMayThrow() throws IOException
	            {
	            	String table =  Metadata.MetaData_KS;
	        		Token tk = StorageService.getPartitioner().getToken(mutation.key());
	        		
	        		List<InetAddress> naturalEndpoints = StorageService.instance.getNaturalEndpoints(table, tk);
	        		Collection<InetAddress> pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, table);

	        		final Iterable<InetAddress> targets = Iterables.concat(naturalEndpoints, pendingEndpoints);
	        		       		
	            	//logger.error("METADATA: announceMetadata");
	        		for (InetAddress endpoint : targets) {
	        			// don't send schema to the nodes with the versions older than
	        			// current major
	        			if (MessagingService.instance().getVersion(endpoint) < MessagingService.current_version)
	        				continue;
	       			
	                    MessageOut<Collection<RowMutation>> msg = new MessageOut<Collection<RowMutation>>(
	                    		MessagingService.Verb.DEFINITIONS_UPDATE,
	                    		Collections.singletonList(mutation),
	                            MigrationsSerializer.instance);

	                    MessagingService.instance().sendOneWay(msg, endpoint);
	                }
	            }
	        };
	        
	        //runnable.run();
	        StageManager.getStage(Stage.MIGRATION).execute(runnable);    	
	}
}
