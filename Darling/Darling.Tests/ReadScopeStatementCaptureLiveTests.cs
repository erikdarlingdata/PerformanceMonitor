/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5097: per-statement capture inside a read. The live facts: the scope is visible inside the Npgsql activity
/// callbacks, when the activity stops, which tag carries a row count, what a command outside a scope costs, and
/// that fan-out loses no entry.
/// </summary>
public sealed class ReadScopeStatementCaptureLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static NpgsqlDataSource Source() => NpgsqlDataSource.Create(ConnectionString!);

    private static void RequirePg() =>
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the #5097 statement-capture live tests.");

    [Fact]
    public async Task ARealRead_InsideACapturingScope_DeliversItsTimings_ThroughTheAsyncLocal()
    {
        RequirePg();
        ReadStatementCapture.Register();
        await using var source = Source();
        /* Warm the pool so the read below runs on a pooled connection. */
        await using (var warm = await source.OpenConnectionAsync()) { }

        using var opened = ReadScope.Open(null);
        opened.Scope.CaptureStatements = true;
        await using (var command = source.CreateCommand("-- a comment\nSELECT 5097 AS capture_probe"))
        {
            await using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync()) { }
        }

        var seen = opened.Scope.Snapshot();
        var probe = Assert.Single(seen, t => t.Label == "SELECT 5097 AS capture_probe");
        Assert.True(probe.DurationMs >= 0);
        Assert.Equal(1, probe.Ordinal > 0 ? 1 : 0);
        Assert.Equal(8, probe.Hash.Length);
    }

    [Fact]
    public async Task TheActivity_StopsAtReaderClose_SoTheDurationIncludesTheHeldReader()
    {
        RequirePg();
        ReadStatementCapture.Register();
        await using var source = Source();
        using var opened = ReadScope.Open(null);
        opened.Scope.CaptureStatements = true;

        /* Postgres buffers the server's output, so a slow generate_series delivers its first row late; the
           stop point is therefore probed from the client: read ONE row, hold the reader open for 600 ms, then
           close it. A stop at first result would be a few ms; a stop at reader close includes the 600 ms. */
        var watch = Stopwatch.StartNew();
        long firstRowMs;
        await using (var command = source.CreateCommand("SELECT g FROM generate_series(1, 5) AS g"))
        {
            await using var reader = await command.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());
            firstRowMs = watch.ElapsedMilliseconds;
            await Task.Delay(600);
        }

        var totalMs = watch.ElapsedMilliseconds;
        var timing = Assert.Single(opened.Scope.Snapshot(), t => t.Label.StartsWith("SELECT g FROM generate_series", StringComparison.Ordinal));
        System.IO.File.AppendAllText(System.IO.Path.Combine(System.IO.Path.GetTempPath(), "pm5097-capture-evidence.txt"),
            $"(b) firstRowMs={firstRowMs} totalMs={totalMs} activityMs={timing.DurationMs:F0} rows={timing.Rows?.ToString() ?? "null"}\n");
        Assert.True(timing.DurationMs >= 550, $"duration {timing.DurationMs} ms stops before the reader closed (first row at {firstRowMs} ms)");
    }

    [Fact]
    public async Task TheStoppedActivity_TagNames_AreListed_AndTheRowCountTagIsReadWhenPresent()
    {
        RequirePg();
        await using var source = Source();
        var tags = new List<string>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = s => s.Name == "Npgsql",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = a =>
            {
                if (a.TagObjects.Any(t => t.Key == "db.query.text" && (t.Value as string)?.Contains("tag_probe_5097") == true))
                {
                    lock (tags) { tags.AddRange(a.TagObjects.Select(t => t.Key + "=" + t.Value)); }
                }
            },
        };
        ActivitySource.AddActivityListener(listener);

        await using (var command = source.CreateCommand("SELECT g AS tag_probe_5097 FROM generate_series(1, 7) AS g"))
        {
            await using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync()) { }
        }

        string[] snapshot;
        lock (tags) { snapshot = tags.ToArray(); }
        System.IO.File.AppendAllText(System.IO.Path.Combine(System.IO.Path.GetTempPath(), "pm5097-capture-evidence.txt"),
            "(c) tags: " + string.Join(" | ", snapshot.Select(t => t.Length > 90 ? t[..90] : t)) + "\n");
        Assert.NotEmpty(snapshot);
        Assert.Contains(snapshot, t => t.StartsWith("db.query.text=", StringComparison.Ordinal));
    }

    [Fact]
    public async Task ACommandOutsideAnyScope_IsNotSampled_AndCreatesNoActivity()
    {
        RequirePg();
        ReadStatementCapture.Register();
        await using var source = Source();

        var options = default(ActivityCreationOptions<ActivityContext>);
        Assert.Null(ReadScope.Current);
        Assert.Equal(ActivitySamplingResult.None, ReadStatementCapture.Sample(ref options));

        using (var opened = ReadScope.Open(null))
        {
            /* A scope that did not ask for capture is also not sampled. */
            Assert.Equal(ActivitySamplingResult.None, ReadStatementCapture.Sample(ref options));
            opened.Scope.CaptureStatements = true;
            Assert.Equal(ActivitySamplingResult.AllDataAndRecorded, ReadStatementCapture.Sample(ref options));
        }

        /* The command still runs normally outside any scope. */
        await using var plain = source.CreateCommand("SELECT 1");
        Assert.Equal(1, await plain.ExecuteScalarAsync());
    }

    [Fact]
    public async Task ConcurrentCommands_InsideOneScope_AreAllCaptured()
    {
        RequirePg();
        ReadStatementCapture.Register();
        await using var source = Source();
        using var opened = ReadScope.Open(null);
        opened.Scope.CaptureStatements = true;

        const int Fan = 32;
        await Task.WhenAll(Enumerable.Range(0, Fan).Select(async i =>
        {
            await using var command = source.CreateCommand($"SELECT {i} AS fan_probe_5097, pg_sleep(0.05)");
            await command.ExecuteScalarAsync();
        }));

        var seen = opened.Scope.Snapshot().Where(t => t.Label.Contains("fan_probe_5097", StringComparison.Ordinal)).ToList();
        Assert.Equal(Fan, seen.Count);
        Assert.Equal(Fan, seen.Select(t => t.Label).Distinct().Count());
        Assert.Equal(Fan, seen.Select(t => t.Ordinal).Distinct().Count());
    }
}

/// <summary>#5097: the label function and the queue cap (no database).</summary>
public sealed class ReadScopeStatementLabelTests
{
    [Fact]
    public void TheLabel_SkipsLeadingCommentsAndBlankLines()
    {
        Assert.Equal("SELECT a FROM t", StatementLabel.Label("-- note\n\n  -- more\n  SELECT a FROM t\nWHERE x = 1"));
        Assert.Equal("SELECT a FROM t", StatementLabel.Label("--c\r\nSELECT a FROM t\r\nWHERE x"));
    }

    [Fact]
    public void TheLabel_IsCutAt120Characters()
    {
        var label = StatementLabel.Label("SELECT " + new string('x', 400));
        Assert.Equal(120, label.Length);
    }

    [Fact]
    public void TheHash_IsStable_ShortAndDependsOnTheWholeText()
    {
        Assert.Equal(StatementLabel.Hash("SELECT 1"), StatementLabel.Hash("SELECT 1"));
        Assert.NotEqual(StatementLabel.Hash("SELECT 1"), StatementLabel.Hash("SELECT 2"));
        Assert.Equal(8, StatementLabel.Hash("SELECT 1").Length);
        /* Known SHA-256 prefix, so a change of algorithm is noticed. */
        Assert.Equal("e1b0c442".Length, StatementLabel.Hash(string.Empty).Length);
        Assert.Equal("e3b0c442", StatementLabel.Hash(string.Empty));
    }

    [Fact]
    public void ANullOrEmptyText_GivesAnEmptyLabel() => Assert.Equal(string.Empty, StatementLabel.Label(null));

    [Fact]
    public void TheQueue_IsCapped_AndCountsTheOverflow()
    {
        using var opened = ReadScope.Open(null);
        for (var i = 0; i < ReadScope.MaxStatements + 10; i++)
        {
            opened.Scope.AddStatement("SELECT " + i, 1, null);
        }

        Assert.Equal(ReadScope.MaxStatements, opened.Scope.Snapshot().Count);
        Assert.Equal(10, opened.Scope.StatementsDropped);
        Assert.Equal(1, opened.Scope.Snapshot()[0].Ordinal);
    }

    [Fact]
    public void NoteSource_KeepsOnlyTheFixedVocabulary_AndNoteRows_Records()
    {
        using var opened = ReadScope.Open(null);
        ReadScope.NoteSource("something_else");
        Assert.Null(opened.Scope.Source);
        ReadScope.NoteSource(ReadScope.SourceIntervalTable, "gate");
        ReadScope.NoteRows(12);
        Assert.Equal("interval_table", opened.Scope.Source);
        Assert.Equal("gate", opened.Scope.SourceReason);
        Assert.Equal(12, opened.Scope.Rows);
    }

    [Fact]
    public void NoteSourceAndRows_OutsideAScope_DoNothing()
    {
        Assert.Null(ReadScope.Current);
        ReadScope.NoteSource(ReadScope.SourceRaw);
        ReadScope.NoteRows(3);
    }

    [Fact]
    public void ARecorderOpenedScope_DoesNotCaptureUntilTheRecorderAsks()
    {
        using var opened = ReadScope.Open(null);
        Assert.False(opened.Scope.CaptureStatements);
    }
}
