package org.apache.cassandra.metadata;

public class Metadata {

	public static final String MetaData_KS = "system_metadata";
	
	public static final String MetadataRegistry_CF = "registry";
	public static final String MetadataLog_CF = "log";
	
	public static final String AlterKeyspace_Tag = "a_ks";
	public static final String DropKeyspace_Tag = "d_ks";
	
	public static final String AlterColumnFamily_Alter_Tag = "a_cf_al";
	public static final String AlterColumnFamily_Add_Tag = "a_cf_ad";
	public static final String AlterColumnFamily_Drop_Tag = "a_cf_d";
	public static final String AlterColumnFamily_Rename_Tag = "a_cf_r";
	public static final String AlterColumnFamily_Prob_Tag = "a_cf_p";
	public static final String DropColumnFamily_Tag = "d_cf";
	public static final String TruncateColumnFamily_Tag = "t_cf";
	
	public static final String Insert_Tag = "i_r";
	public static final String Update_Tag = "u_r";
	public static final String delete_Tag = "d_r";
}
