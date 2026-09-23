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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3944 end to end, against a real migrated store: the classifier's census written with the sweep's own insert
/// statements, then read back through <c>get_store_log</c>. The store keeps a retained message's GROUPING key, so the
/// same error for three values across two captures is ONE row with all three occurrences; the tool shows the newest
/// sample's own message as PostgreSQL wrote it, and a statement's literal reaches neither the row nor the answer.
///
/// <para><b>#1776 own-store</b>: it mints and migrates a scratch database through <c>ScratchPostgres</c>, so it is
/// not in the <c>live-postgres</c> collection.</para>
/// </summary>
public sealed class StoreLogGroupingLiveTests
{
    private const string Prefix = "2026-09-22 14:03:02.551 UTC [5288] ";

    [Fact]
    public async Task OneErrorForThreeValues_IsOneRow_ShownAsWritten_ThroughTheTool_AgainstScratchPostgres()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(baseConnectionString),
            "Set DARLING_TEST_PG to a superuser connection string to run the #3944 grouping proof in a scratch database.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        await using (var migrate = await postgres.OpenConnectionAsync(ct))
        {
            await PgMigrations.MigrateAsync(migrate, ct);
        }

        var now = DateTime.UtcNow;
        await WriteCaptureAsync(postgres, now.AddHours(-2), string.Join("\n",
        [
            Prefix + "ERROR:  invalid input syntax for type integer: \"abc\"",
            Prefix + "ERROR:  invalid input syntax for type integer: \"def\"",
            Prefix + "ERROR:  canceling statement due to statement timeout",
            Prefix + "STATEMENT:  UPDATE creds SET secret = 'Leak3944x' WHERE id = 7",
            "",
        ]), ct);
        await WriteCaptureAsync(postgres, now.AddHours(-1), Prefix + "ERROR:  invalid input syntax for type integer: \"ghi\"\n", ct);

        /* A 3.8.0 capture's fragment, not yet reached by the re-mask pass: the reader shows no text for it. */
        await using (var fragment = postgres.CreateCommand(@"
INSERT INTO collect.store_log_events (capture_time, event_class, severity, occurrences, message_text, sample_line)
VALUES ($1, 'lock_timeout', 'ERROR', 1, 'noted', $2)"))
        {
            fragment.Parameters.AddWithValue(DateTime.SpecifyKind(now.AddMinutes(-30), DateTimeKind.Unspecified));
            fragment.Parameters.AddWithValue("\t-- ERROR:  noted\n\tFROM creds WHERE pw = 'Leak3944z'");
            await fragment.ExecuteNonQueryAsync(ct);
        }

        var answer = await DarlingMcpStoreLogTools.GetStoreLog(postgres, hours_back: 24);
        Assert.DoesNotContain("Leak3944x", answer, StringComparison.Ordinal);
        Assert.DoesNotContain("Leak3944z", answer, StringComparison.Ordinal);

        using var parsed = JsonDocument.Parse(answer);
        var retained = parsed.RootElement.GetProperty("retained_events").EnumerateArray().ToList();
        Assert.Equal(2, retained.Count);

        var invalid = Assert.Single(retained, e => e.GetProperty("event_class").GetString() == StoreLogClassifier.UnclassifiedClass);
        Assert.Equal(3, invalid.GetProperty("occurrences").GetInt64());
        Assert.Equal("invalid input syntax for type integer: \"ghi\"", invalid.GetProperty("message_text").GetString());
        Assert.Equal(Prefix + "ERROR:  invalid input syntax for type integer: \"ghi\"", invalid.GetProperty("sample_line").GetString());

        var timeout = Assert.Single(retained, e => e.GetProperty("event_class").GetString() == "statement_timeout");
        Assert.Equal("canceling statement due to statement timeout", timeout.GetProperty("message_text").GetString());
        Assert.EndsWith("STATEMENT:  UPDATE creds SET secret = '?' WHERE id = ?", timeout.GetProperty("sample_line").GetString(), StringComparison.Ordinal);

        /* What the store holds under the tool: the key, never the text, for the three rows of the one message. */
        await using var read = postgres.CreateCommand(
            "SELECT DISTINCT message_text FROM collect.store_log_events WHERE event_class = 'unclassified'");
        await using var reader = await read.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        Assert.Equal("invalid input syntax for type integer: \"?\"", reader.GetString(0));
        Assert.False(await reader.ReadAsync(ct));
    }

    /// <summary>One capture as <see cref="StoreLogSweep"/> writes it: the census rows and the denominator row,
    /// through the sweep's own statements.</summary>
    private static async Task WriteCaptureAsync(NpgsqlDataSource postgres, DateTime captureUtc, string slab, CancellationToken ct)
    {
        var census = StoreLogClassifier.Classify(slab);
        var captureTime = DateTime.SpecifyKind(captureUtc, DateTimeKind.Unspecified);
        await using var connection = await postgres.OpenConnectionAsync(ct);

        await using (var events = new NpgsqlCommand(StoreLogSweep.EventInsertSql, connection))
        {
            events.Parameters.AddWithValue(captureTime);
            events.Parameters.AddWithValue(census.Groups.Select(g => g.EventClass).ToArray());
            events.Parameters.AddWithValue(census.Groups.Select(g => g.Severity).ToArray());
            events.Parameters.AddWithValue(census.Groups.Select(g => g.Occurrences).ToArray());
            events.Parameters.AddWithValue(census.Groups.Select(g => g.MessageText).ToArray());
            events.Parameters.AddWithValue(census.Groups.Select(g => g.SampleLine).ToArray());
            await events.ExecuteNonQueryAsync(ct);
        }

        await using var capture = new NpgsqlCommand(StoreLogSweep.CaptureInsertSql, connection);
        capture.Parameters.AddWithValue(captureTime);
        capture.Parameters.AddWithValue("postgresql-Tue.log");
        capture.Parameters.AddWithValue((long)slab.Length);
        capture.Parameters.AddWithValue(0L);
        capture.Parameters.AddWithValue(census.LinesRead);
        capture.Parameters.AddWithValue(census.EntriesRead);
        capture.Parameters.AddWithValue(false);
        capture.Parameters.AddWithValue(census.GroupsDropped);
        await capture.ExecuteNonQueryAsync(ct);
    }
}
