/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.CFDefinition;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.metadata.MetadataLog;
import org.apache.cassandra.net.*;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

public class RowMutationVerbHandler implements IVerbHandler<RowMutation>
{
    private static final Logger logger = LoggerFactory.getLogger(RowMutationVerbHandler.class);

    public void doVerb(MessageIn<RowMutation> message, String id)
    {
        try
        {
            RowMutation rm = message.payload;

            // Check if there were any forwarding headers in this message
            byte[] from = message.parameters.get(RowMutation.FORWARD_FROM);
            InetAddress replyTo;
            if (from == null)
            {
                replyTo = message.from;
                byte[] forwardBytes = message.parameters.get(RowMutation.FORWARD_TO);
                if (forwardBytes != null && message.version >= MessagingService.VERSION_11)
                    forwardToLocalNodes(rm, message.verb, forwardBytes, message.from);
            }
            else
            {
                replyTo = InetAddress.getByAddress(from);
            }

            rm.apply();
            WriteResponse response = new WriteResponse();
            Tracing.trace("Enqueuing response to {}", replyTo);
            MessagingService.instance().sendReply(response.createMessage(), id, replyTo);
            
            // decide if you want to add metadata for the recieved rm:
            for(ColumnFamily cf: rm.getColumnFamilies()){
            	CFMetaData cfm = Schema.instance.getCFMetaData(cf.id());
            	//announceMetadataLogMigration(cfm.getCfDef(), rm.key(), cf, "");
            }
        }
        catch (IOException e)
        {
            logger.error("Error in row mutation", e);
        }
    }

    private void announceMetadataLogMigration(CFDefinition cfDef, ByteBuffer key, ColumnFamily cf, String dataTag){
    	
    	String partitioningKeyName = "";	
		try {
			if (cfDef.hasCompositeKey) {
				for (int i = 0; i < cfDef.keys.size(); i++) {
					ByteBuffer bb = CompositeType.extractComponent(key, i);
					if (i != 0) partitioningKeyName += ".";
					partitioningKeyName += ByteBufferUtil.string(bb);
				}
			} else {
				partitioningKeyName = ByteBufferUtil.string(key);
			}
		} catch (CharacterCodingException e) {
			return;
		}
			
    	// Iterating Column Family to get columns
    	ArrayList<Pair<String,String>> targets = new ArrayList<Pair<String,String>>();
    	partitioningKeyName = cfDef.cfm.ksName + "." + cfDef.cfm.cfName + "." + partitioningKeyName;
    	String allValues = ""; 
    	
    	for( IColumn col: cf.getSortedColumns()){
    		String colName = col.getString(cf.getComparator());

    		// filter column markers
    		if(colName.indexOf("::") != -1)
    			continue;
    		
    		int colNameBoundary = colName.indexOf("false");
    		if(colNameBoundary == -1) 
    			colNameBoundary = colName.indexOf("true");

    		colName = colName.substring(0, colNameBoundary-1);
    		colName = colName.replace(':', '.');
    		
    		String colVal = new String(col.value().array());
    		
    		if(!colName.equals("")){
        		allValues +=  colName + "=" + colVal + ";";
        		//targets.add( Pair.create(partitioningKeyName + "." + colName, colVal));
    		}
    	}
    	targets.add( Pair.create(partitioningKeyName, allValues));
    	
    	//String client = (clientState == null)? "" :  clientState.getUser().getName();
    	//return MetadataLog.add(partitioningKeyName, FBUtilities.timestampMicros(), "", dataTag, allValues, "");
    	MigrationManager.announceMetadataLogMigration(partitioningKeyName, dataTag, "", allValues);
    }
 
    /**
     * Older version (< 1.0) will not send this message at all, hence we don't
     * need to check the version of the data.
     */
    private void forwardToLocalNodes(RowMutation rm, MessagingService.Verb verb, byte[] forwardBytes, InetAddress from) throws IOException
    {
        DataInputStream dis = new DataInputStream(new FastByteArrayInputStream(forwardBytes));
        int size = dis.readInt();

        // tell the recipients who to send their ack to
        MessageOut<RowMutation> message = new MessageOut<RowMutation>(verb, rm, RowMutation.serializer).withParameter(RowMutation.FORWARD_FROM, from.getAddress());
        // Send a message to each of the addresses on our Forward List
        for (int i = 0; i < size; i++)
        {
            InetAddress address = CompactEndpointSerializationHelper.deserialize(dis);
            String id = dis.readUTF();
            Tracing.trace("Enqueuing forwarded write to {}", address);
            MessagingService.instance().sendOneWay(message, id, address);
        }
    }
}
