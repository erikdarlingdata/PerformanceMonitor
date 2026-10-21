/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #6254: the Add and Edit dialog's Test button also opens the Query Store replica, built from the same connection string as the
/// primary test with the replica host and read-only intent, and reports it apart. A replica that will not open is a warning with
/// its error, never an exception that fails the test. Before this the button tested only the primary.
/// </summary>
public sealed class QueryStoreReplicaConnectionTestTests
{
    private const string Primary = "Data Source=primary.example.test;Initial Catalog=master;Integrated Security=True;Encrypt=Mandatory;TrustServerCertificate=True";

    [Fact]
    public async Task TheReplicaConnection_IsThePrimaryStringWithTheReplicaHostAndReadOnlyIntent()
    {
        var opened = new List<string>();

        var result = await QueryStoreReplicaConnectionTest.RunAsync(
            Primary, "  replica-01,14331 ", s => new RecordingConnection(s, opened, null));

        Assert.Equal(("replica-01,14331", (string?)null), result);
        var builder = new SqlConnectionStringBuilder(Assert.Single(opened));
        Assert.Equal("replica-01,14331", builder.DataSource);
        Assert.Equal(ApplicationIntent.ReadOnly, builder.ApplicationIntent);
        var primary = new SqlConnectionStringBuilder(Primary);
        Assert.Equal(primary.InitialCatalog, builder.InitialCatalog);
        Assert.Equal(primary.IntegratedSecurity, builder.IntegratedSecurity);
        Assert.Equal(primary.Encrypt, builder.Encrypt);
    }

    [Fact]
    public async Task AReplicaThatWillNotOpen_IsAnErrorInTheResult_NotAnException_AndTheTextSaysTheCost()
    {
        var result = await QueryStoreReplicaConnectionTest.RunAsync(
            Primary, "replica-01", s => new RecordingConnection(s, [], new InvalidOperationException("Login failed for user 'monitor'.")));

        Assert.NotNull(result);
        Assert.Equal("replica-01", result.Value.Host);
        Assert.Equal("Login failed for user 'monitor'.", result.Value.Error);

        var text = QueryStoreReplicaConnectionTest.Compose(result.Value);
        Assert.Contains("Primary: connected.", text, StringComparison.Ordinal);
        Assert.Contains("Query Store replica replica-01: could not connect (Login failed for user 'monitor'.)", text, StringComparison.Ordinal);
        Assert.Contains("reads Query Store from the primary", text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AReachableReplica_IsNamedConnected_AndAnEmptyBoxTestsNothing()
    {
        var opened = new List<string>();
        var ok = await QueryStoreReplicaConnectionTest.RunAsync(Primary, "replica-01", s => new RecordingConnection(s, opened, null));
        Assert.Equal("Primary: connected.\nQuery Store replica replica-01: connected.", QueryStoreReplicaConnectionTest.Compose(ok!.Value));

        opened.Clear();
        Assert.Null(await QueryStoreReplicaConnectionTest.RunAsync(Primary, null, s => new RecordingConnection(s, opened, null)));
        Assert.Null(await QueryStoreReplicaConnectionTest.RunAsync(Primary, "   ", s => new RecordingConnection(s, opened, null)));
        Assert.Empty(opened);
    }

    [Fact]
    public async Task AnUnreachableHost_ThroughTheRealSqlConnection_IsAnErrorNotAnException()
    {
        var result = await QueryStoreReplicaConnectionTest.RunAsync(Primary + ";Connect Timeout=3", "127.0.0.1,1");

        Assert.NotNull(result);
        Assert.False(string.IsNullOrWhiteSpace(result.Value.Error));
    }

    private sealed class RecordingConnection(string connectionString, List<string> opened, Exception? failure) : DbConnection
    {
        private ConnectionState _state = ConnectionState.Closed;

        [System.Diagnostics.CodeAnalysis.AllowNull]
        public override string ConnectionString { get; set; } = connectionString;

        public override string Database => "";

        public override string DataSource => "";

        public override string ServerVersion => "16.0";

        public override ConnectionState State => _state;

        public override void ChangeDatabase(string databaseName) { }

        public override void Close() => _state = ConnectionState.Closed;

        public override void Open()
        {
            opened.Add(ConnectionString);
            if (failure is not null)
            {
                throw failure;
            }

            _state = ConnectionState.Open;
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();

        protected override DbCommand CreateDbCommand() => throw new NotSupportedException();
    }
}
