/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The re-mask of store-log rows captured before #3915 and #3944, against a real migrated store. An earlier build
/// kept retained entries whole (an ERROR's STATEMENT line with a password literal, the first #3899 build's raw slow
/// statements) for the sweep's 400-day retention, where the viewer and mcp roles read them, and stored a message's
/// text where this build stores its grouping key. The pass normalizes the SQL, re-keys the message and keeps the
/// prose as PostgreSQL wrote it; it rewrites exactly the rows that changes, walks the table in bounded slices by
/// cursor, and a second pass changes nothing.
///
/// <para><b>#1776 own-store</b>: it mints and migrates a scratch database through <c>ScratchPostgres</c>, so
/// it is not in the <c>live-postgres</c> collection.</para>
/// </summary>
public sealed class StoreLogRemaskLiveTests
{
    private const string Prefix = "2026-09-05 14:03:02.551 UTC [5288] ";

    [Fact]
    public async Task RowsCapturedBeforeThisBuild_AreRemasked_AndASecondPassChangesNothing_AgainstScratchPostgres()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(baseConnectionString),
            "Set DARLING_TEST_PG to a superuser connection string to run the #3915 re-mask proof in a scratch database.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var c = new NpgsqlConnection(scratch.ConnectionString);
        await c.OpenAsync(ct);
        await PgMigrations.MigrateAsync(c, ct);

        var rows = new (string Class, string Severity, string? Message, string? Sample)[]
        {
            ("statement_timeout", "ERROR", "canceling statement due to statement timeout",
                Prefix + "ERROR:  canceling statement due to statement timeout\n" + Prefix + "STATEMENT:  ALTER ROLE admin PASSWORD 'Remask3915a'"),
            ("unclassified", "ERROR", "invalid input syntax for type integer: \"Remask3915b\"",
                Prefix + "ERROR:  invalid input syntax for type integer: \"Remask3915b\""),
            (StoreLogClassifier.SlowStatementClass, "LOG", "duration: 6001.000 ms  statement: SELECT * FROM t WHERE code = 'Remask3915c'",
                Prefix + "LOG:  duration: 6001.000 ms  statement: SELECT * FROM t WHERE code = 'Remask3915c'"),
            ("deadlock", "ERROR", "deadlock detected",
                Prefix + "ERROR:  deadlock detected\n" + Prefix + "DETAIL:  Process 1 waits for ShareLock on transaction 2; blocked by process 3."),
            /* A 3.8.0 capture's fragment: a statement's tab-led continuation opened an entry of its own. */
            ("lock_timeout", "ERROR", "noted",
                "\t-- ERROR:  noted\n\tFROM creds WHERE pw = 'Remask3944f'"),
            ("routine", "LOG", null, null),
        };

        foreach (var (eventClass, severity, message, sample) in rows)
        {
            await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.store_log_events (capture_time, event_class, severity, occurrences, message_text, sample_line)
VALUES ((now() AT TIME ZONE 'UTC'), $1, $2, 1, $3, $4)", c);
            insert.Parameters.AddWithValue(eventClass);
            insert.Parameters.AddWithValue(severity);
            insert.Parameters.Add(new NpgsqlParameter { Value = (object?)message ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
            insert.Parameters.Add(new NpgsqlParameter { Value = (object?)sample ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
            await insert.ExecuteNonQueryAsync(ct);
        }

        var (examined, rewritten) = await RunToTheEndAsync(c, ct);
        Assert.Equal(5, examined);
        Assert.Equal(4, rewritten);

        var stored = new Dictionary<string, (string? Message, string? Sample)>();
        await using (var read = new NpgsqlCommand("SELECT event_class, message_text, sample_line FROM collect.store_log_events", c))
        await using (var reader = await read.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                stored[reader.GetString(0)] = (
                    reader.IsDBNull(1) ? null : reader.GetString(1),
                    reader.IsDBNull(2) ? null : reader.GetString(2));
            }
        }

        /* The SQL literals are gone from every row; the prose value is kept in the sample, and its message is the
           grouping key. */
        foreach (var (_, (message, sample)) in stored)
        {
            Assert.DoesNotContain("Remask3915a", (message ?? "") + sample, StringComparison.Ordinal);
            Assert.DoesNotContain("Remask3915c", (message ?? "") + sample, StringComparison.Ordinal);
            Assert.DoesNotContain("Remask3944f", (message ?? "") + sample, StringComparison.Ordinal);
        }

        /* The fragment keeps no text: its occurrence stays in the class count. */
        Assert.Equal(((string?)null, (string?)null), stored["lock_timeout"]);

        Assert.Equal("statement: SELECT * FROM t WHERE code = '?'", stored[StoreLogClassifier.SlowStatementClass].Message);
        Assert.Contains("STATEMENT:  ALTER ROLE admin PASSWORD '?'", stored["statement_timeout"].Sample, StringComparison.Ordinal);
        Assert.Equal("invalid input syntax for type integer: \"?\"", stored["unclassified"].Message);
        Assert.Equal(Prefix + "ERROR:  invalid input syntax for type integer: \"Remask3915b\"", stored["unclassified"].Sample);
        Assert.Equal("deadlock detected", stored["deadlock"].Message);

        /* Idempotent: a second pass over the same table rewrites nothing. */
        var (_, again) = await RunToTheEndAsync(c, ct);
        Assert.Equal(0, again);
    }

    private static async Task<(int Examined, int Rewritten)> RunToTheEndAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        string? cursor = null;
        var examined = 0;
        var rewritten = 0;
        do
        {
            var pass = await StoreLogSweep.RemaskStoredEventsAsync(connection, cursor, ct);
            cursor = pass.NextCursor;
            examined += pass.Examined;
            rewritten += pass.Rewritten;
        }
        while (cursor is not null);

        return (examined, rewritten);
    }
}
