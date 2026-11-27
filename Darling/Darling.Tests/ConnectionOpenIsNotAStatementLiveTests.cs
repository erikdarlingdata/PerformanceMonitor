/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9923 live: a read that opens a physical connection inside a capturing scope counts the open apart from its statements.
/// The pool is cleared and the data source is new, so the first command waits on a physical open, as the first read after a
/// service start or a pool recycle does. The slow-read row built from the scope carries the statements with text and the open
/// on its own.
/// </summary>
[Collection("live-postgres")]
public sealed class ConnectionOpenIsNotAStatementLiveTests
{
    private const string Probe = "SELECT 9923 AS connection_open_probe";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static void RequirePg() =>
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the #9923 connection-open live tests.");

    private static async Task RunProbeAsync(NpgsqlDataSource source)
    {
        await using var command = source.CreateCommand(Probe);
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync()) { }
    }

    [Fact]
    public async Task APhysicalOpenInsideACapturingScope_IsCountedAsAnOpen_AndNotAsAStatement()
    {
        RequirePg();
        ReadStatementCapture.Register();
        NpgsqlConnection.ClearAllPools();
        await using var source = NpgsqlDataSource.Create(ConnectionString!);

        using var opened = ReadScope.Open(null);
        var scope = opened.Scope;
        scope.CaptureStatements = true;
        await RunProbeAsync(source);
        await RunProbeAsync(source);
        await RunProbeAsync(source);

        var seen = scope.Snapshot();
        Assert.NotEmpty(seen);
        Assert.All(seen, t => Assert.False(string.IsNullOrEmpty(t.Label), "every kept statement has its text, so no open was kept as an empty statement"));
        Assert.Equal(3, seen.Count(t => t.Label == Probe));
        Assert.Equal(seen.Count, scope.StatementTotal);
        Assert.True(scope.ConnectionOpens >= 1, "the first command on a new pool waited on a physical open, and the scope counted it");
        Assert.True(scope.ConnectionOpenMs >= 0);
    }

    [Fact]
    public async Task TheSlowReadRow_CarriesTheStatementsWithText_AndTheOpensOnTheirOwn()
    {
        RequirePg();
        ReadStatementCapture.Register();
        NpgsqlConnection.ClearAllPools();
        await using var source = NpgsqlDataSource.Create(ConnectionString!);

        var log = new SlowReadLog();
        using var opened = ReadScope.Open(null);
        var scope = opened.Scope;
        scope.CaptureStatements = true;
        await RunProbeAsync(source);
        await RunProbeAsync(source);

        log.Offer(scope, ReadSurface.Mcp, "get_slow_reads_9923", ReadOutcome.Ok, 6000, null, null, null);
        Assert.True(log.TryRead(out var record));

        var stored = JsonNode.Parse(record!.StatementsJson)!.AsArray();
        var withText = stored.Count(s => !string.IsNullOrEmpty((string?)s!["label"]));
        Assert.Equal(stored.Count, withText);
        Assert.Equal(withText, record.StatementCount);
        Assert.False(record.StatementsTruncated);
        Assert.True(record.ConnectionOpens >= 1);
        Assert.NotNull(record.ConnectionOpenMs);
    }
}
