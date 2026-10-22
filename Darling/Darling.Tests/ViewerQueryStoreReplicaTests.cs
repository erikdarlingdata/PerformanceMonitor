/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the live fact mints its own scratch database through ScratchPostgres and never touches another one. */

/// <summary>
/// #6254: the desktop viewer and the service must bind a password to the same Query Store replica. A viewer that sealed a row
/// with a replica as if it had none (10 fields) would store a password the service (11 fields) cannot open.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerQueryStoreReplicaTests
{
    private const string Secret = "not-a-real-password-6254";
    private const int ServerId = 7625;
    private static readonly PasswordPrivateKey Key = PasswordPrivateKey.Generate();

    private static MonitoredServerRow Row(string? replica) => new()
    {
        ServerId = ServerId, Name = "qs-replica", Host = "primary.example.test", Auth = "sql", Username = "monitor",
        EncryptMode = "Mandatory", QueryStoreReplica = replica,
    };

    /// <summary>The service's own view of the same row: the definition it reads from the store, bound the way it opens passwords.</summary>
    private static PasswordBinding ServiceBindingOf(string? replica) => PasswordBinding.ForServer(new MonitoredServer
    {
        Name = "qs-replica", Host = "primary.example.test", Auth = "sql", Username = "monitor", EncryptMode = "Mandatory",
        QueryStoreReplica = replica,
    }.ConnectionIdentity);

    [Fact]
    public void APasswordTheViewerSealedForARowWithAReplica_OpensThroughTheServiceBinding()
    {
        var sealedText = new ViewerPasswordSealer(Key.PublicKey).Seal(Secret, Row("replica-a,1433"));

        Assert.Equal(Secret, PasswordSeal.Open(sealedText, Key, ServiceBindingOf("replica-a,1433")));
        Assert.Equal(Secret, PasswordSeal.Open(sealedText, Key, ServiceBindingOf("  replica-a,1433 ")));
        Assert.Throws<PasswordSealException>(() => PasswordSeal.Open(sealedText, Key, ServiceBindingOf(null)));
    }

    [Fact]
    public void APasswordTheServiceSealed_OpensThroughTheViewerBindingOfTheRowItRead()
    {
        var sealedText = PasswordSeal.Seal(Secret, Key.PublicKey, ServiceBindingOf("replica-a"));

        Assert.Equal(Secret, PasswordSeal.Open(sealedText, Key, ViewerPasswordSealer.BindingOf(Row("replica-a"))));
        Assert.Throws<PasswordSealException>(() => PasswordSeal.Open(sealedText, Key, ViewerPasswordSealer.BindingOf(Row(null))));
    }

    [Fact]
    public void ARowWithNoReplica_SealsTheTenFieldsEverySealHasAlwaysHad()
    {
        var sealedText = new ViewerPasswordSealer(Key.PublicKey).Seal(Secret, Row("   "));

        Assert.Equal(Secret, PasswordSeal.Open(sealedText, Key, ServiceBindingOf(null)));
    }

    [Fact]
    public void ATestConnectRequestCarriesTheReplica_SoTheServiceTestsTheConnectionTheSaveWillStore()
    {
        var json = ViewerDataService.BuildTestConnectArgs(new TestConnectServer { Host = "primary.example.test", QueryStoreReplica = "replica-a" });
        var server = JsonSerializer.Deserialize<MonitoredServer>(json);

        Assert.Equal("replica-a", server!.QueryStoreReplica);
    }

    // The collection log page: a run with a read source carries it, and a page with none is byte-for-byte what it was.
    [Fact]
    public void TheCollectionLogPage_CarriesReadSourceOnlyOnTheRunsThatHaveOne()
    {
        const string page = """{"runs_returned":2,"runs":[{"collector":"query_store","status":"SUCCESS"},{"collector":"wait_stats","status":"SUCCESS"}]}""";

        Assert.Equal(page, DarlingMcpDataTools.WithReadSource(page, new string?[] { null, null }));
        var patched = JsonNode.Parse(DarlingMcpDataTools.WithReadSource(page, new string?[] { "replica 12, primary 2", null }))!;
        Assert.Equal("replica 12, primary 2", (string?)patched["runs"]![0]!["read_source"]);
        Assert.Null(patched["runs"]![1]!["read_source"]);
        Assert.False(patched["runs"]![1]!.AsObject().ContainsKey("read_source"));
    }

    [Fact]
    public async Task ASavedReplica_IsStoredNormalized_ReadBackBothWays_AndAReplicaOnlyEditIsRefusedWithTheStoredBlob()
    {
        var ct = TestContext.Current.CancellationToken;
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live viewer Query Store replica test (it mints its own scratch database).");

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var ownerString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using (var connection = new NpgsqlConnection(ownerString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, null, ct);
        }

        await using var viewer = new ViewerDataService(scratch.ConnectionString);

        var first = Row("  replica-a,1433 ");
        first.EncryptedPassword = new ViewerPasswordSealer(Key.PublicKey).Seal(Secret, first);
        await viewer.UpsertMonitoredServerAsync(first, ct);

        Assert.Equal("replica-a,1433", (await viewer.GetMonitoredServerAsync(ServerId, ct))!.QueryStoreReplica);
        Assert.Equal("replica-a,1433", (await viewer.GetMonitoredServersAsync(ct)).Single().QueryStoreReplica);

        /* The same blob with another replica is a change of how the server is reached: it cannot keep a password sealed for the old one. */
        var moved = Row("replica-b");
        moved.EncryptedPassword = first.EncryptedPassword;
        await Assert.ThrowsAsync<MonitoredServerPasswordNeededException>(() => viewer.UpsertMonitoredServerAsync(moved, ct));

        moved.EncryptedPassword = new ViewerPasswordSealer(Key.PublicKey).Seal(Secret, moved);
        await viewer.UpsertMonitoredServerAsync(moved, ct);
        Assert.Equal("replica-b", (await viewer.GetMonitoredServerAsync(ServerId, ct))!.QueryStoreReplica);

        var cleared = Row("  ");
        cleared.EncryptedPassword = new ViewerPasswordSealer(Key.PublicKey).Seal(Secret, cleared);
        await viewer.UpsertMonitoredServerAsync(cleared, ct);
        Assert.Null((await viewer.GetMonitoredServerAsync(ServerId, ct))!.QueryStoreReplica);
    }
}
