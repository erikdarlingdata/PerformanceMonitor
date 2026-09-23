/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4005: the SQL in a stored deadlock report reaches no reader with its literals, whichever build stored it.
///
/// <para>The collector normalizes each query before it is stored (<c>PgDeadlockLogParserTests</c> pins that,
/// in Lite.Tests beside the parser). What is pinned here is the READ half, against a live store: a row stored
/// before #4005 holds its queries raw, and every read (<c>get_pg_deadlocks</c>, <c>get_pg_deadlock_detail</c>,
/// the web dispatch and the desktop viewer through <see cref="DarlingPgDeadlockReader"/>, and the deadlock
/// alert built from the same read) normalizes them on the way out. Such a row's hash is over its raw graph, so
/// it is a test for the literals the read removed (#4004): it is neither returned nor found by.</para>
/// </summary>
public sealed class PgDeadlockNormalizationTests
{
    private const string ServerName = "darling-pg-deadlock-normalization-4005";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    /* A graph as the collector stored it before #4005: the DETAIL block verbatim, tabs stripped, a PIN and a
       card number in each query. */
    private const string LegacyGraph =
        "Process 3101 waits for ShareLock on transaction 5501; blocked by process 3102.\n"
        + "Process 3102 waits for ShareLock on transaction 5500; blocked by process 3101.\n"
        + "Process 3101: UPDATE accounts SET pin = '4721' WHERE card = 4111111111111111\n"
        + "Process 3102: UPDATE accounts SET pin = '9034' WHERE card = 5500005555555559";

    private const string LegacyVictimStatement = "UPDATE accounts SET pin = '4721' WHERE card = 4111111111111111";

    private static readonly string[] s_secrets = ["4721", "9034", "4111111111111111", "5500005555555559"];

    /* ───────────────────────── pure ───────────────────────── */

    /// <summary>
    /// Each read spells the raw-hash test out because its SQL is a constant; this holds every spelling to the
    /// parser's, whose hash it has to recognise. A drifted copy would return a raw hash as an identity.
    /// </summary>
    [Fact]
    public void EveryReadSpellsTheParsersRawHashTest()
    {
        Assert.Contains(PgDeadlockLogParser.RawGraphHashSql("d.deadlock_hash", "MIN(d.graph_text)"), DarlingPgDeadlockReader.DeadlocksSql, StringComparison.Ordinal);
        Assert.Contains(PgDeadlockLogParser.RawGraphHashSql("report.deadlock_hash", "report.graph_text"), DarlingPgDeadlockReader.DeadlockDetailSql, StringComparison.Ordinal);
        Assert.Contains(PgDeadlockLogParser.RawGraphHashSql("latest_hash", "latest_graph_text"), PgTargetDrillDownCollector.PgTargetDeadlockExemplarsSql, StringComparison.Ordinal);
    }

    [Fact]
    public void ALegacyIdentityRoundTrips_AndAHashIsNotOne()
    {
        var at = new DateTime(2026, 8, 26, 22, 25, 24, 100);
        var identity = PgDeadlockLogParser.LegacyIdentity(at, 1549);

        Assert.True(PgDeadlockLogParser.TryParseLegacyIdentity(identity, out var parsedAt, out var parsedPid));
        Assert.Equal(at, parsedAt);
        Assert.Equal(1549, parsedPid);

        Assert.False(PgDeadlockLogParser.TryParseLegacyIdentity(PgDeadlockLogParser.HashOf(LegacyGraph), out _, out _));
        Assert.False(PgDeadlockLogParser.TryParseLegacyIdentity(null, out _, out _));
        Assert.False(PgDeadlockLogParser.TryParseLegacyIdentity("legacy-2026", out _, out _));
    }

    /* ───────────────────────── live ───────────────────────── */

    /// <summary>
    /// A row stored before #4005 reads normalized, in the summary and in the detail, and under an identity
    /// that is not its raw hash. The raw hash, which a reader could compute from a guessed PIN, finds nothing.
    /// </summary>
    [Fact]
    public async Task ALegacyRow_ReadsNormalized_AndItsRawHashIsNeitherReturnedNorFound()
    {
        await WithStoreAsync(async (connection, postgres, ct) =>
        {
            var at = TruncateToSeconds(DateTime.UtcNow).AddMinutes(-10);
            var rawHash = PgDeadlockLogParser.HashOf(LegacyGraph);
            await PlantAsync(connection, at, 3101, rawHash, LegacyVictimStatement, LegacyGraph, ct);
            await PlantAsync(connection, at, 3101, rawHash, LegacyVictimStatement, LegacyGraph, ct);

            var summary = Assert.Single(await DarlingPgDeadlockReader.GetDeadlocksAsync(
                postgres, ServerId, at.AddHours(-1), at.AddHours(1), 25, ct));
            Assert.Equal("UPDATE accounts SET pin = '?' WHERE card = ?", summary.VictimStatement);
            Assert.Equal(PgDeadlockLogParser.LegacyIdentity(at, 3101), summary.DeadlockHash);
            Assert.Equal(2, summary.TimesSeen);

            var recent = Assert.Single(await DarlingPgDeadlockReader.GetDeadlockDetailAsync(postgres, ServerId, null, 5, ct));
            AssertNoSecret(recent.GraphText);
            Assert.StartsWith(
                "Process 3101 waits for ShareLock on transaction 5501; blocked by process 3102.\n"
                + "Process 3102 waits for ShareLock on transaction 5500; blocked by process 3101.\n",
                recent.GraphText, StringComparison.Ordinal);
            Assert.EndsWith("Process 3102: UPDATE accounts SET pin = '?' WHERE card = ?", recent.GraphText, StringComparison.Ordinal);
            Assert.Equal(summary.DeadlockHash, recent.DeadlockHash);

            var byIdentity = Assert.Single(await DarlingPgDeadlockReader.GetDeadlockDetailAsync(postgres, ServerId, summary.DeadlockHash, 5, ct));
            Assert.Equal(recent.GraphText, byIdentity.GraphText);

            Assert.Empty(await DarlingPgDeadlockReader.GetDeadlockDetailAsync(postgres, ServerId, rawHash, 5, ct));
        });
    }

    /// <summary>
    /// A row the collector stores since #4005 is normalized already, reads back unchanged, and keeps its
    /// hash as its identity: the hash covers only what a reader sees.
    /// </summary>
    [Fact]
    public async Task ANewRow_ReadsBackUnchanged_UnderItsOwnHash()
    {
        await WithStoreAsync(async (connection, postgres, ct) =>
        {
            var at = TruncateToSeconds(DateTime.UtcNow).AddMinutes(-5);
            var parsed = Assert.Single(PgDeadlockLogParser.Extract(Report(at, 3201, 3202)));
            await PlantAsync(connection, parsed.OccurredAtUtc, parsed.VictimPid, parsed.DeadlockHash, parsed.VictimStatement, parsed.GraphText, ct);

            var summary = Assert.Single(await DarlingPgDeadlockReader.GetDeadlocksAsync(
                postgres, ServerId, at.AddHours(-1), at.AddHours(1), 25, ct));
            Assert.Equal(parsed.DeadlockHash, summary.DeadlockHash);
            Assert.Equal(parsed.VictimStatement, summary.VictimStatement);

            var detail = Assert.Single(await DarlingPgDeadlockReader.GetDeadlockDetailAsync(postgres, ServerId, parsed.DeadlockHash, 5, ct));
            Assert.Equal(parsed.GraphText, detail.GraphText);
            AssertNoSecret(detail.GraphText);
        });
    }

    /// <summary>
    /// Without an identity the detail read returns the most recent reports, as its panel says. Its LIMIT sat
    /// on the DISTINCT ON sort, which leads with the hash, so it returned the reports whose hashes sort first.
    /// </summary>
    [Fact]
    public async Task TheDetailReadWithoutAnIdentity_ReturnsTheNewestReports()
    {
        await WithStoreAsync(async (connection, postgres, ct) =>
        {
            var now = TruncateToSeconds(DateTime.UtcNow);
            await PlantAsync(connection, now.AddMinutes(-30), 11, "0000000000000000000000000000000A", "SELECT ?", "Process 11 waits for ShareLock on transaction 1; blocked by process 12.", ct);
            await PlantAsync(connection, now.AddMinutes(-20), 21, "8000000000000000000000000000000B", "SELECT ?", "Process 21 waits for ShareLock on transaction 2; blocked by process 22.", ct);
            await PlantAsync(connection, now.AddMinutes(-10), 31, "F000000000000000000000000000000C", "SELECT ?", "Process 31 waits for ShareLock on transaction 3; blocked by process 32.", ct);

            var rows = await DarlingPgDeadlockReader.GetDeadlockDetailAsync(postgres, ServerId, null, 2, ct);

            Assert.Equal([31, 21], rows.Select(r => r.VictimPid));
        });
    }

    /// <summary>
    /// The deadlock alert's body: its incident is built from the same read, so the statement it carries to
    /// email, a webhook's <c>{{incidents_json}}</c> and the persisted alert context is normalized, and its
    /// dedup key, which the cards render as "Dedup Key", is not the raw hash.
    /// </summary>
    [Fact]
    public async Task TheDeadlockAlertsNotificationBody_CarriesNoLiteralAndNoRawHash()
    {
        await WithStoreAsync(async (connection, postgres, ct) =>
        {
            var at = TruncateToSeconds(DateTime.UtcNow).AddMinutes(-10);
            var rawHash = PgDeadlockLogParser.HashOf(LegacyGraph);
            await PlantAsync(connection, at, 3101, rawHash, LegacyVictimStatement, LegacyGraph, ct);

            var rows = await DarlingPgDeadlockReader.GetDeadlocksAsync(postgres, ServerId, at.AddHours(-1), at.AddHours(1), 50, ct);
            var incidents = rows.Select(DarlingWorker.BuildPgDeadlockIncident).ToList();
            var context = new AlertContext { Incidents = incidents };
            AlertIncidentRenderer.Apply(context, incidents);

            var incident = Assert.Single(incidents);
            Assert.Equal(PgDeadlockLogParser.LegacyIdentity(at, 3101), incident.DedupKey);
            Assert.Equal("UPDATE accounts SET pin = '?' WHERE card = ?", Assert.Single(incident.InvolvedObjects));

            foreach (var body in new[] { AlertContextSerializer.Serialize(context), AlertContextSerializer.SerializeIncidents(context) })
            {
                AssertNoSecret(body);
                Assert.DoesNotContain(rawHash, body, StringComparison.Ordinal);
                /* JSON writes the quote as ', so the statement is found by its unquoted half. */
                Assert.Contains("WHERE card = ?", body, StringComparison.Ordinal);
            }
        });
    }

    /* ───────────────────────── helpers ───────────────────────── */

    /* A report as a target writes it, with values in both queries and the HINT that proves it whole. */
    private static string Report(DateTime at, int victim, int other)
    {
        var prefix = at.ToString("yyyy-MM-dd HH:mm:ss.fff", System.Globalization.CultureInfo.InvariantCulture) + $" UTC [{victim}] ";
        return prefix + "ERROR:  deadlock detected\n"
            + prefix + $"DETAIL:  Process {victim} waits for ShareLock on transaction 5501; blocked by process {other}.\n"
            + $"\tProcess {other} waits for ShareLock on transaction 5500; blocked by process {victim}.\n"
            + $"\tProcess {victim}: UPDATE accounts SET pin = '4721' WHERE card = 4111111111111111\n"
            + $"\tProcess {other}: UPDATE accounts SET pin = '9034' WHERE card = 5500005555555559\n"
            + prefix + "HINT:  See server log for query details.\n";
    }

    private static void AssertNoSecret(string? text)
    {
        Assert.NotNull(text);
        foreach (var secret in s_secrets)
        {
            Assert.DoesNotContain(secret, text, StringComparison.Ordinal);
        }
    }

    private static DateTime TruncateToSeconds(DateTime value) =>
        new(value.Ticks - value.Ticks % TimeSpan.TicksPerSecond, DateTimeKind.Unspecified);

    private static async Task WithStoreAsync(Func<NpgsqlConnection, NpgsqlDataSource, CancellationToken, Task> body)
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the deadlock normalization reads.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        try
        {
            await body(connection, postgres, ct);
        }
        finally
        {
            await DeleteRowsAsync(connection, CancellationToken.None);
        }
    }

    private static async Task PlantAsync(
        NpgsqlConnection connection, DateTime at, int victimPid, string hash, string? victimStatement, string graphText, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO pg_deadlocks
    (collection_id, collection_time, server_id, server_name, occurred_at, victim_pid, participant_count, deadlock_hash, lock_modes, resources, victim_statement, graph_text)
VALUES ($1, $2, $3, $4, $2, $5, 2, $6, 'ShareLock', 'transaction 5500, transaction 5501', $7, $8)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(at, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(victimPid);
        command.Parameters.AddWithValue(hash);
        command.Parameters.AddWithValue((object?)victimStatement ?? DBNull.Value);
        command.Parameters.AddWithValue(graphText);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var cleanup = new NpgsqlCommand($"DELETE FROM pg_deadlocks WHERE server_id = {ServerId}", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
