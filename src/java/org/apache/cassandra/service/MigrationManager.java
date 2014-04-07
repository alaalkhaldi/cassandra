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
package org.apache.cassandra.service;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.gms.*;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.FastByteArrayOutputStream;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.metadata.*;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.WrappedRunnable;

import com.google.common.collect.Iterables;

public class MigrationManager implements IEndpointStateChangeSubscriber
{
    private static final Logger logger = LoggerFactory.getLogger(MigrationManager.class);

    private static final ByteBuffer LAST_MIGRATION_KEY = ByteBufferUtil.bytes("Last Migration");

    public static final MigrationManager instance = new MigrationManager();

    private static final RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();

    public static final int MIGRATION_DELAY_IN_MS = 60000;

    private final List<IMigrationListener> listeners = new CopyOnWriteArrayList<IMigrationListener>();

    private MigrationManager() {}

    public void register(IMigrationListener listener)
    {
        listeners.add(listener);
    }

    public void unregister(IMigrationListener listener)
    {
        listeners.remove(listener);
    }

    public void onJoin(InetAddress endpoint, EndpointState epState)
    {}

    public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value)
    {
        if (state != ApplicationState.SCHEMA || endpoint.equals(FBUtilities.getBroadcastAddress()))
            return;

        maybeScheduleSchemaPull(UUID.fromString(value.value), endpoint);
    }

    public void onAlive(InetAddress endpoint, EndpointState state)
    {
        VersionedValue value = state.getApplicationState(ApplicationState.SCHEMA);

        if (value != null)
            maybeScheduleSchemaPull(UUID.fromString(value.value), endpoint);
    }

    public void onDead(InetAddress endpoint, EndpointState state)
    {}

    public void onRestart(InetAddress endpoint, EndpointState state)
    {}

    public void onRemove(InetAddress endpoint)
    {}

    /**
     * If versions differ this node sends request with local migration list to the endpoint
     * and expecting to receive a list of migrations to apply locally.
     */
    private static void maybeScheduleSchemaPull(final UUID theirVersion, final InetAddress endpoint)
    {
        if (Schema.instance.getVersion().equals(theirVersion) || !shouldPullSchemaFrom(endpoint))
            return;

        if (Schema.emptyVersion.equals(Schema.instance.getVersion()) || runtimeMXBean.getUptime() < MIGRATION_DELAY_IN_MS)
        {
            // If we think we may be bootstrapping or have recently started, submit MigrationTask immediately
            submitMigrationTask(endpoint);
        }
        else
        {
            // Include a delay to make sure we have a chance to apply any changes being
            // pushed out simultaneously. See CASSANDRA-5025
            Runnable runnable = new Runnable()
            {
                public void run()
                {
                    // grab the latest version of the schema since it may have changed again since the initial scheduling
                    VersionedValue value = Gossiper.instance.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.SCHEMA);
                    UUID currentVersion = UUID.fromString(value.value);
                    if (Schema.instance.getVersion().equals(currentVersion))
                        return;

                    submitMigrationTask(endpoint);
                }
            };
            StorageService.optionalTasks.schedule(runnable, MIGRATION_DELAY_IN_MS, TimeUnit.MILLISECONDS);
        }
    }

    private static Future<?> submitMigrationTask(InetAddress endpoint)
    {
        /*
         * Do not de-ref the future because that causes distributed deadlock (CASSANDRA-3832) because we are
         * running in the gossip stage.
         */
        return StageManager.getStage(Stage.MIGRATION).submit(new MigrationTask(endpoint));
    }

    private static boolean shouldPullSchemaFrom(InetAddress endpoint)
    {
        /*
         * Don't request schema from nodes with versions younger than 1.1.7 (timestamps in versions prior to 1.1.7 are broken)
         * Don't request schema from nodes with a higher major (may have incompatible schema)
         * Don't request schema from fat clients
         */
        return MessagingService.instance().getVersion(endpoint) >= MessagingService.VERSION_117
            && MessagingService.instance().getVersion(endpoint) <= MessagingService.current_version
            && !Gossiper.instance.isFatClient(endpoint);
    }

    public static boolean isReadyForBootstrap()
    {
        return ((ThreadPoolExecutor) StageManager.getStage(Stage.MIGRATION)).getActiveCount() == 0;
    }

    public void notifyCreateKeyspace(KSMetaData ksm)
    {
        for (IMigrationListener listener : listeners)
            listener.onCreateKeyspace(ksm.name);
    }

    public void notifyCreateColumnFamily(CFMetaData cfm)
    {
        for (IMigrationListener listener : listeners)
            listener.onCreateColumnFamily(cfm.ksName, cfm.cfName);
    }

    public void notifyUpdateKeyspace(KSMetaData ksm)
    {
        for (IMigrationListener listener : listeners)
            listener.onUpdateKeyspace(ksm.name);
    }

    public void notifyUpdateColumnFamily(CFMetaData cfm)
    {
        for (IMigrationListener listener : listeners)
            listener.onUpdateColumnFamily(cfm.ksName, cfm.cfName);
    }

    public void notifyDropKeyspace(KSMetaData ksm)
    {
        for (IMigrationListener listener : listeners)
            listener.onDropKeyspace(ksm.name);
    }

    public void notifyDropColumnFamily(CFMetaData cfm)
    {
        for (IMigrationListener listener : listeners)
            listener.onDropColumnFamily(cfm.ksName, cfm.cfName);
    }

    public static void announceNewKeyspace(KSMetaData ksm) throws ConfigurationException
    {
        announceNewKeyspace(ksm, FBUtilities.timestampMicros());
    }

    public static void announceNewKeyspace(KSMetaData ksm, long timestamp) throws ConfigurationException
    {
        ksm.validate();

        if (Schema.instance.getTableDefinition(ksm.name) != null)
            throw new AlreadyExistsException(ksm.name);

        logger.info(String.format("Create new Keyspace: %s", ksm));
        announce(ksm.toSchema(timestamp));
    }

    public static void announceNewColumnFamily(CFMetaData cfm) throws ConfigurationException
    {
        cfm.validate();

        KSMetaData ksm = Schema.instance.getTableDefinition(cfm.ksName);
        if (ksm == null)
            throw new ConfigurationException(String.format("Cannot add column family '%s' to non existing keyspace '%s'.", cfm.cfName, cfm.ksName));
        else if (ksm.cfMetaData().containsKey(cfm.cfName))
            throw new AlreadyExistsException(cfm.ksName, cfm.cfName);

        logger.info(String.format("Create new ColumnFamily: %s", cfm));
        announce(cfm.toSchema(FBUtilities.timestampMicros()));
    }

    public static void announceKeyspaceUpdate(KSMetaData ksm, ClientState state) throws ConfigurationException
    {
        ksm.validate();

        KSMetaData oldKsm = Schema.instance.getKSMetaData(ksm.name);
        if (oldKsm == null)
            throw new ConfigurationException(String.format("Cannot update non existing keyspace '%s'.", ksm.name));

        logger.info(String.format("Update Keyspace '%s' From %s To %s", ksm.name, oldKsm, ksm));
        announce(oldKsm.toSchemaUpdate(ksm, FBUtilities.timestampMicros()));
        
        // prepare metadata_log  
        String logValue = "Old: " + oldKsm.strategyClass.getSimpleName() + "," + oldKsm.strategyOptions.toString() + "," + oldKsm.durableWrites + ";" +
        		"New: " + ksm.strategyClass.getSimpleName() + "," + ksm.strategyOptions.toString() + "," + ksm.durableWrites;
        announceMetadataLogMigration(ksm.name, Metadata.AlterKeyspace_Tag, state, logValue);
    }

    public static void announceColumnFamilyUpdate(CFMetaData cfm) throws ConfigurationException
    {
        cfm.validate();

        CFMetaData oldCfm = Schema.instance.getCFMetaData(cfm.ksName, cfm.cfName);
        if (oldCfm == null)
            throw new ConfigurationException(String.format("Cannot update non existing column family '%s' in keyspace '%s'.", cfm.cfName, cfm.ksName));

        oldCfm.validateCompatility(cfm);

        logger.info(String.format("Update ColumnFamily '%s/%s' From %s To %s", cfm.ksName, cfm.cfName, oldCfm, cfm));
        announce(oldCfm.toSchemaUpdate(cfm, FBUtilities.timestampMicros()));
    }
    
    public static void announceMetadataRegistryUpdate(String target, String dataTag, String AdminTag) throws ConfigurationException{
    	announceMetadata(MetadataRegistry.instance.add(target, dataTag, AdminTag));
    }
    
    public static void announceMetadataRegistryDrop(String target){
    	announceMetadata(MetadataRegistry.instance.drop(target));
    }
       
    public static void announceKeyspaceDrop(String ksName, ClientState state) throws ConfigurationException
    {
        KSMetaData oldKsm = Schema.instance.getKSMetaData(ksName);
        if (oldKsm == null)
            throw new ConfigurationException(String.format("Cannot drop non existing keyspace '%s'.", ksName));

        logger.info(String.format("Drop Keyspace '%s'", oldKsm.name));
        announce(oldKsm.dropFromSchema(FBUtilities.timestampMicros()));
        
        // prepare metadata_log  
        String log_value = oldKsm.strategyClass.getSimpleName() + "," + oldKsm.strategyOptions.toString() + "," + oldKsm.durableWrites;
        announceMetadataLogMigration(oldKsm.name, Metadata.DropKeyspace_Tag, state, log_value);
    }

    public static void announceColumnFamilyDrop(String ksName, String cfName, ClientState state) throws ConfigurationException
    {
        CFMetaData oldCfm = Schema.instance.getCFMetaData(ksName, cfName);
        if (oldCfm == null)
            throw new ConfigurationException(String.format("Cannot drop non existing column family '%s' in keyspace '%s'.", cfName, ksName));

        logger.info(String.format("Drop ColumnFamily '%s/%s'", oldCfm.ksName, oldCfm.cfName));
        announce(oldCfm.dropFromSchema(FBUtilities.timestampMicros()));
        
        // prepare metadata_log  
        String log_value = "";
        announceMetadataLogMigration(oldCfm.ksName + "." + oldCfm.cfName, Metadata.DropColumnFamily_Tag, state, log_value);
    }

    /**
     * actively announce a new version to active hosts via rpc
     * @param schema The schema mutation to be applied
     */
    private static void announce(RowMutation schema)
    {
        FBUtilities.waitOnFuture(announce(Collections.singletonList(schema)));
    }

	private static void pushSchemaMutation(InetAddress endpoint, Collection<RowMutation> schema)
    {
        MessageOut<Collection<RowMutation>> msg = new MessageOut<Collection<RowMutation>>(MessagingService.Verb.DEFINITIONS_UPDATE,
                                                                                          schema,
                                                                                          MigrationsSerializer.instance);
        MessagingService.instance().sendOneWay(msg, endpoint);
    }

    // Returns a future on the local application of the schema
    private static Future<?> announce(final Collection<RowMutation> schema)
    {
        Future<?> f = StageManager.getStage(Stage.MIGRATION).submit(new WrappedRunnable()
        {
            protected void runMayThrow() throws IOException, ConfigurationException
            {
                DefsTable.mergeSchema(schema);
            }
        });

        for (InetAddress endpoint : Gossiper.instance.getLiveMembers())
        {
            if (endpoint.equals(FBUtilities.getBroadcastAddress()))
                continue; // we've dealt with localhost already

            // don't send schema to the nodes with the versions older than current major
            if (MessagingService.instance().getVersion(endpoint) < MessagingService.current_version)
                continue;

            pushSchemaMutation(endpoint, schema);
        }
        return f;
    }
    
    private static void announceMetadata(final RowMutation mutation)
    {
    	String table =  Metadata.MetaData_KS;
		Token tk = StorageService.getPartitioner().getToken(mutation.key());
		
		List<InetAddress> naturalEndpoints = StorageService.instance.getNaturalEndpoints(table, tk);
		Collection<InetAddress> pendingEndpoints = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, table);

		Iterable<InetAddress> targets = Iterables.concat(naturalEndpoints, pendingEndpoints);
		
		for (InetAddress endpoint : targets) {
			// don't send schema to the nodes with the versions older than
			// current major
			if (MessagingService.instance().getVersion(endpoint) < MessagingService.current_version)
				continue;

			pushSchemaMutation(endpoint, Collections.singletonList(mutation));
		}
    }

    /**
     * Announce my version passively over gossip.
     * Used to notify nodes as they arrive in the cluster.
     *
     * @param version The schema version to announce
     */
    public static void passiveAnnounce(UUID version)
    {
        Gossiper.instance.addLocalApplicationState(ApplicationState.SCHEMA, StorageService.instance.valueFactory.schema(version));
        logger.debug("Gossiping my schema version " + version);
    }

    /**
     * Clear all locally stored schema information and reset schema to initial state.
     * Called by user (via JMX) who wants to get rid of schema disagreement.
     *
     * @throws IOException if schema tables truncation fails
     */
    public static void resetLocalSchema() throws IOException
    {
        logger.info("Starting local schema reset...");

        try
        {
            logger.debug("Truncating schema tables...");

            // truncate schema tables
            FBUtilities.waitOnFutures(new ArrayList<Future<?>>(3)
            {{
                SystemTable.schemaCFS(SystemTable.SCHEMA_KEYSPACES_CF).truncate();
                SystemTable.schemaCFS(SystemTable.SCHEMA_COLUMNFAMILIES_CF).truncate();
                SystemTable.schemaCFS(SystemTable.SCHEMA_COLUMNS_CF).truncate();
            }});

            logger.debug("Clearing local schema keyspace definitions...");

            Schema.instance.clear();

            Set<InetAddress> liveEndpoints = Gossiper.instance.getLiveMembers();
            liveEndpoints.remove(FBUtilities.getBroadcastAddress());

            // force migration if there are nodes around
            for (InetAddress node : liveEndpoints)
            {
                if (shouldPullSchemaFrom(node))
                {
                    logger.debug("Requesting schema from {}", node);
                    FBUtilities.waitOnFuture(submitMigrationTask(node));
                    break;
                }
            }

            logger.info("Local schema reset is complete.");
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Used only in case node has old style migration schema (newly updated)
     * @return the UUID identifying version of the last applied migration
     */
    @Deprecated
    public static UUID getLastMigrationId()
    {
        DecoratedKey dkey = StorageService.getPartitioner().decorateKey(LAST_MIGRATION_KEY);
        Table defs = Table.open(Table.SYSTEM_KS);
        ColumnFamilyStore cfStore = defs.getColumnFamilyStore(DefsTable.OLD_SCHEMA_CF);
        QueryFilter filter = QueryFilter.getNamesFilter(dkey, new QueryPath(DefsTable.OLD_SCHEMA_CF), LAST_MIGRATION_KEY);
        ColumnFamily cf = cfStore.getColumnFamily(filter);
        if (cf == null || cf.getColumnNames().size() == 0)
            return null;
        else
            return UUIDGen.getUUID(cf.getColumn(LAST_MIGRATION_KEY).value());
    }

    public static class MigrationsSerializer implements IVersionedSerializer<Collection<RowMutation>>
    {
        public static MigrationsSerializer instance = new MigrationsSerializer();

        public void serialize(Collection<RowMutation> schema, DataOutput out, int version) throws IOException
        {
            out.writeInt(schema.size());
            for (RowMutation rm : schema)
                RowMutation.serializer.serialize(rm, out, version);
        }

        public Collection<RowMutation> deserialize(DataInput in, int version) throws IOException
        {
            int count = in.readInt();
            Collection<RowMutation> schema = new ArrayList<RowMutation>(count);

            for (int i = 0; i < count; i++)
                schema.add(RowMutation.serializer.deserialize(in, version));

            return schema;
        }

        public long serializedSize(Collection<RowMutation> schema, int version)
        {
            int size = TypeSizes.NATIVE.sizeof(schema.size());
            for (RowMutation rm : schema)
                size += RowMutation.serializer.serializedSize(rm, version);
            return size;
        }
    }
    
    public static void announceMetadataLogMigration(String target, String dataTag, String client, String logValue){   	
    	if(client == null) client = "";
    	announceMetadata(MetadataLog.add(target, FBUtilities.timestampMicros(), client, dataTag, logValue, ""));
    }
    
    public static void announceMetadataLogMigration(String target, String dataTag, ClientState clientstate, String logValue){
    	String client = (clientstate == null)? "" : clientstate.getUser().getName();
    	announceMetadata(MetadataLog.add(target, FBUtilities.timestampMicros(), client, dataTag, logValue, ""));
    }
    
    public static void announceMetadataLogMigration(ArrayList<Pair<String,String>>targets, String dataTag, String client){
    	if(client == null) client = "";
    	for(Pair<String,String> target: targets){
        	announceMetadata(MetadataLog.add(target.left, FBUtilities.timestampMicros(), client, dataTag, target.right, ""));
    	}
    }
}
