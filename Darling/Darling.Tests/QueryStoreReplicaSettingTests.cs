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

    [Fact]
    public void The_replica_connection_is_the_servers_own_with_the_host_swapped_and_read_only_intent()
    {
        var text = MonitoredServerConnection.BuildQueryStoreReplicaConnectionString(Server("replica-01,14331"), "pw-6254");

        Assert.NotNull(text);
        var builder = new SqlConnectionStringBuilder(text);
        var primary = new SqlConnectionStringBuilder(MonitoredServerConnection.BuildConnectionString(Server("replica-01,14331"), "pw-6254"));
        Assert.Equal("replica-01,14331", builder.DataSource);
        Assert.Equal(ApplicationIntent.ReadOnly, builder.ApplicationIntent);
        Assert.Equal(ApplicationIntent.ReadWrite, primary.ApplicationIntent);
        Assert.Equal(primary.UserID, builder.UserID);
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
        Assert.Null(MonitoredServerConnection.BuildQueryStoreReplicaConnectionString(Server(null), "pw-6254"));
        Assert.Null(MonitoredServerConnection.BuildQueryStoreReplicaConnectionString(Server("   "), "pw-6254"));
        Assert.Equal(
            MonitoredServerConnection.BuildConnectionString(Server(null), "pw-6254"),
            MonitoredServerConnection.BuildConnectionString(Server("replica-01"), "pw-6254"));
    }
}
