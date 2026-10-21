/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6254: the Query Store replica setting. The first group is the security rule: a saved password opens only for the replica
/// it was sealed for, and a seal made before the setting existed (no replica) opens unchanged. The second group is the one
/// replica connection builder.
/// </summary>
public sealed class QueryStoreReplicaSettingTests
{
    private const string Secret = "not-a-real-password-6254";
    private static readonly PasswordPrivateKey Key = PasswordPrivateKey.Generate();

    private static ServerConnectionIdentity Identity(string? replica) =>
        new("example-sql-01", 1433, "sqlserver", "example_db", false, "sql", "example_login", "Mandatory", false, false, replica);

    private static string? Open(string sealedText, string? replica)
    {
        try
        {
            return PasswordSeal.Open(sealedText, Key, PasswordBinding.ForServer(Identity(replica)));
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            return null;
        }
    }

    private static string Seal(string? replica) =>
        PasswordSeal.Seal(Secret, Key.PublicKey, PasswordBinding.ForServer(Identity(replica)));

    [Fact]
    public void A_server_with_no_replica_binds_the_same_ten_fields_it_always_did()
    {
        Assert.Equal(10, PasswordBinding.ForServer(Identity(null)).Fields.Count);
        Assert.Equal(10, PasswordBinding.ForServer(Identity("")).Fields.Count);
        Assert.Equal(10, PasswordBinding.ForServer(Identity("   ")).Fields.Count);
        Assert.Equal(
            PasswordBinding.ForServer(Identity(null)).EncodeAad("k"),
            PasswordBinding.ForServer(Identity("  ")).EncodeAad("k"));
    }

    [Fact]
    public void A_seal_made_with_no_replica_opens_with_no_replica_and_a_seal_made_for_a_replica_opens_for_that_replica()
    {
        Assert.Equal(Secret, Open(Seal(null), null));
        Assert.Equal(Secret, Open(Seal("replica-a,1433"), "replica-a,1433"));
        Assert.Equal(Secret, Open(Seal("replica-a"), "  replica-a "));
    }

    [Fact]
    public void A_seal_made_for_replica_A_does_not_open_for_replica_B_or_with_the_replica_removed()
    {
        var forA = Seal("replica-a");

        Assert.Null(Open(forA, "replica-b"));
        Assert.Null(Open(forA, null));
        Assert.Null(Open(forA, ""));
    }

    [Fact]
    public void A_seal_made_with_no_replica_does_not_open_once_a_replica_is_set()
    {
        var before = Seal(null);

        Assert.Null(Open(before, "replica-a"));
    }

    [Fact]
    public void Two_identities_that_differ_only_in_the_replica_differ_and_blank_and_none_do_not()
    {
        Assert.True(ServerConnectionIdentity.Differ(Identity(null), Identity("replica-a")));
        Assert.True(ServerConnectionIdentity.Differ(Identity("replica-a"), Identity("replica-b")));
        Assert.False(ServerConnectionIdentity.Differ(Identity(null), Identity("  ")));
        Assert.False(ServerConnectionIdentity.Differ(Identity("replica-a"), Identity(" replica-a ")));
    }

    private static MonitoredServer Server(string? replica) => new()
    {
        Host = "primary-01,1433",
        Database = "example_db",
        Auth = "sql",
        Username = "example_login",
        EncryptMode = "Strict",
        TrustServerCertificate = false,
        MultiSubnetFailover = true,
        ReadOnlyIntent = false,
        QueryStoreReplica = replica,
    };

    /// <summary>A server whose saved password is sealed for the connection it holds, replica included (the way the store saves it).</summary>
    private static MonitoredServer SealedServer(string? replica)
    {
        var server = Server(replica);
        server.Name = "alpha";
        server.EncryptedPassword = PasswordSeal.Seal(Secret, Key.PublicKey, server.SecretBinding);
        return server;
    }

    private static IPasswordKeyRing Ring => DarlingPasswordKey.FromPrivateKey(Key);

    [Fact]
    public void The_replica_connection_is_the_servers_own_with_the_host_swapped_and_read_only_intent()
    {
        var text = MonitoredServerConnection.BuildQueryStoreReplicaConnectionString(SealedServer("replica-01,14331"), false, Ring);

        Assert.NotNull(text);
        var builder = new SqlConnectionStringBuilder(text);
        var primary = new SqlConnectionStringBuilder(MonitoredServerConnection.BuildConnectionString(SealedServer("replica-01,14331"), Secret));
        Assert.Equal("replica-01,14331", builder.DataSource);
        Assert.Equal(ApplicationIntent.ReadOnly, builder.ApplicationIntent);
        Assert.Equal(ApplicationIntent.ReadWrite, primary.ApplicationIntent);
        Assert.Equal(primary.UserID, builder.UserID);
        Assert.Equal(Secret, builder.Password);
        Assert.Equal(primary.Password, builder.Password);
        Assert.Equal(primary.Encrypt, builder.Encrypt);
        Assert.Equal(primary.TrustServerCertificate, builder.TrustServerCertificate);
        Assert.Equal(primary.MultiSubnetFailover, builder.MultiSubnetFailover);
        Assert.Equal(primary.InitialCatalog, builder.InitialCatalog);
        Assert.Equal(primary.ConnectTimeout, builder.ConnectTimeout);
        Assert.Equal(primary.CommandTimeout, builder.CommandTimeout);
    }

    [Fact]
    public void The_replica_connection_is_null_when_the_setting_is_empty_and_the_primary_string_is_unchanged()
    {
        Assert.Null(MonitoredServerConnection.BuildQueryStoreReplicaConnectionString(SealedServer(null), false, Ring));
        Assert.Null(MonitoredServerConnection.BuildQueryStoreReplicaConnectionString(SealedServer("   "), false, Ring));
        Assert.Equal(
            MonitoredServerConnection.BuildConnectionString(Server(null), "pw-6254"),
            MonitoredServerConnection.BuildConnectionString(Server("replica-01"), "pw-6254"));
    }

    /* #6254 security round 1, L2: an Azure SQL Database reads Query Store from itself in this version. */
    [Fact]
    public void An_azure_sql_database_never_gets_a_replica_connection()
    {
        Assert.Null(MonitoredServerConnection.BuildQueryStoreReplicaConnectionString(SealedServer("replica-01"), true, Ring));
    }

    /* #6254 security round 1, L2: the host is the one the seal was opened with (the bound identity), not whatever the free property
       says now, so a password opened for one replica is never sent to another. */
    [Fact]
    public void The_replica_host_is_the_one_the_password_seal_is_bound_to_not_the_free_property()
    {
        var server = SealedServer("bound-replica,14331");
        server.StoredIdentity = ServerConnectionIdentity.FromStoredColumns(
            server.Host, server.Port, server.Engine, server.Database, server.ReadOnlyIntent, server.Auth, server.Username,
            server.EncryptMode, server.TrustServerCertificate, server.MultiSubnetFailover, "bound-replica,14331");
        server.QueryStoreReplica = "attacker-chosen-host";

        var text = MonitoredServerConnection.BuildQueryStoreReplicaConnectionString(server, false, Ring);

        var builder = new SqlConnectionStringBuilder(text);
        Assert.Equal("bound-replica,14331", builder.DataSource);
        Assert.Equal(Secret, builder.Password);
    }

    /* #6254 security round 1, L2: a seal made for no replica, or another one, does not open for this replica: no connection string. */
    [Theory]
    [InlineData(null)]
    [InlineData("another-replica")]
    public void A_password_sealed_for_another_replica_is_not_sent_to_this_one(string? sealedFor)
    {
        var server = SealedServer(sealedFor);
        server.StoredIdentity = null;
        server.QueryStoreReplica = "replica-01";

        Assert.Throws<InvalidOperationException>(() => MonitoredServerConnection.BuildQueryStoreReplicaConnectionString(server, false, Ring));
    }

    /* #6254 security round 1, L1: a stored env:/file: reference is not sealed, so no binding covers it. It goes to a replica only when
       darling.json declares it; a store-only reference (a self-managed store whose trigger predates the replica) is refused. */
    [Fact]
    public void A_stored_reference_goes_to_a_replica_only_when_the_file_declares_it()
    {
        const string Variable = "SYNTH_REPLICA_PW_6254";
        Environment.SetEnvironmentVariable(Variable, "reference-secret-6254");
        try
        {
            var storeOnly = Server("replica-01");
            storeOnly.Name = "alpha";
            storeOnly.EncryptedPassword = "env:" + Variable;
            storeOnly.EncryptedPasswordDeclaredByFile = false;

            var refused = Assert.Throws<InvalidOperationException>(() => MonitoredServerConnection.BuildQueryStoreReplicaConnectionString(storeOnly, false, Ring));
            Assert.Contains("Query Store replica", refused.Message, StringComparison.Ordinal);
            Assert.DoesNotContain("reference-secret-6254", refused.Message, StringComparison.Ordinal);
            Assert.DoesNotContain(Variable, refused.Message, StringComparison.Ordinal);

            storeOnly.EncryptedPasswordDeclaredByFile = true;
            var declared = MonitoredServerConnection.BuildQueryStoreReplicaConnectionString(storeOnly, false, Ring);
            Assert.Equal("reference-secret-6254", new SqlConnectionStringBuilder(declared).Password);
        }
        finally
        {
            Environment.SetEnvironmentVariable(Variable, null);
        }
    }

    /* #6254 security round 1, L3: the replica becomes a Data Source the service opens with its own credentials; SqlClient reads a protocol
       prefix or a UNC-looking value there as a named pipe over SMB. */
    [Theory]
    [InlineData("np:\\\\attacker\\pipe\\sql\\query")]
    [InlineData("np:attacker")]
    [InlineData("lpc:attacker")]
    [InlineData("admin:attacker")]
    [InlineData("tcp:attacker,1433")]
    [InlineData("  NP:attacker")]
    [InlineData("\\\\attacker\\share")]
    [InlineData("\\attacker")]
    [InlineData("//attacker/share")]
    public void A_replica_with_a_protocol_prefix_or_a_path_is_refused(string value)
    {
        Assert.NotNull(QueryStoreReplicaSetting.Refusal(value));
        var server = SealedServer(value);
        Assert.Throws<InvalidOperationException>(() => MonitoredServerConnection.BuildQueryStoreReplicaConnectionString(server, false, Ring));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("replica-01")]
    [InlineData("replica-01.example.test,14331")]
    [InlineData("replica-01\\INST2")]
    [InlineData("ag1-listener")]
    [InlineData("10.1.2.3,1433")]
    [InlineData("[fe80::1]")]
    [InlineData("2001:db8::1")]
    public void A_plain_host_host_port_instance_or_listener_name_is_allowed(string? value)
    {
        Assert.Null(QueryStoreReplicaSetting.Refusal(value));
    }
}
