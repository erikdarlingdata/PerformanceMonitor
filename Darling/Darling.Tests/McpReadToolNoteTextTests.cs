/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The notes the MCP read tools return are sentences a reader takes at face value: a count's verb agrees with the count, the
/// counts in one note agree with the counts beside it, and the dash a note uses is a comma or a colon, never U+2014, the rule
/// the analysis and plan text already follow.
/// </summary>
public sealed class McpReadToolNoteTextTests
{
    [Fact]
    public void TheAlertReadHealthNote_CarriesNoEmDash()
    {
        Assert.DoesNotContain('—', AlertReadFailureCounter.WindowNote);
        Assert.Contains("counting_since", AlertReadFailureCounter.WindowNote, StringComparison.Ordinal);
    }

    /// <summary>
    /// The finding beside that note, for a server whose newest failing read carries an elapsed: the sentence that reads the
    /// elapsed against the deadline and the one that says what a failed read is NOT both keep their clauses and carry no U+2014.
    /// </summary>
    [Fact]
    public void TheAlertReadHealthFinding_CarriesNoEmDash()
    {
        var counter = new AlertReadFailureCounter(() => new DateTime(2026, 8, 23, 10, 0, 0, DateTimeKind.Utc));
        counter.RecordPass("101");
        counter.RecordReadFailure("101", "deadlocks", 10_004);

        var finding = AlertReadFailureCounter.FormatFinding(counter.ReadFor("101"))!;
        Assert.Contains("ran 10004 ms before it failed", finding, StringComparison.Ordinal);
        Assert.Contains("not a single bounded read at all: some entries cover a whole alert pass", finding, StringComparison.Ordinal);
        Assert.Contains("not judged on for that pass, not a fired alert that was lost", finding, StringComparison.Ordinal);
        Assert.DoesNotContain('—', finding);
    }

    /// <summary>get_finops' index-analysis notes (the short-uptime caveat and the dedupe-only notice): same surface, same rule.</summary>
    [Fact]
    public void TheFinOpsIndexAnalysisNotes_CarryNoEmDash()
    {
        Assert.StartsWith("Server uptime is under 14 days: index usage data may be incomplete", DarlingMcpFinOpsTools.IndexAnalysisUptimeNote, StringComparison.Ordinal);
        Assert.StartsWith("Dedupe-only mode is in effect: unused indexes were NOT flagged", DarlingMcpFinOpsTools.IndexAnalysisDedupeNote, StringComparison.Ordinal);
        Assert.DoesNotContain('—', DarlingMcpFinOpsTools.IndexAnalysisUptimeNote);
        Assert.DoesNotContain('—', DarlingMcpFinOpsTools.IndexAnalysisDedupeNote);
    }

    [Theory]
    [InlineData(1, false, 300)]
    [InlineData(10, true, 300)]
    [InlineData(1440, false, 60)]
    public void EveryBucketedTrendsAggregateNote_CarriesNoEmDash(int bucketMinutes, bool requested, int budgetPoints)
    {
        var note = TrendBuckets.AggregateNote(bucketMinutes, requested, budgetPoints);
        Assert.DoesNotContain('—', note);
        Assert.Contains("never averaged", note, StringComparison.Ordinal);
    }

    [Fact]
    public void TheExtensionMissingCollectorOutcome_CarriesNoEmDash()
    {
        var missing = new PostgresException("extension \"pg_buffercache\" is not created in this database", "ERROR", "ERROR", "42P01");
        var (_, explanation) = DarlingWorker.PostgresFaultOutcome(missing, "pg_buffer_usage", "postgres");
        Assert.Contains("extension this collector reads is not installed on this target", explanation, StringComparison.Ordinal);
        Assert.DoesNotContain('\u2014', explanation);
    }

    /// <summary>
    /// The sentence for the rows the reply lists as not collected names its own count: one row "says why", and two or more rows
    /// "each" say why, so the parenthetical never points a plural count at a single row.
    /// </summary>
    [Theory]
    [InlineData(1, "(the row says why)", "(each row says why)")]
    [InlineData(2, "(each row says why)", "(the row says why)")]
    [InlineData(3, "(each row says why)", "(the row says why)")]
    public void TheNotCollectedRowsSentence_AgreesItsParentheticalWithTheCount(int notCollectedCount, string expected, string notExpected)
    {
        var note = CollectorDetailNote.Build(compactCount: 3, shownCount: 4, partialCount: 1, notCollectedCount);
        Assert.Contains($"{CountText.Plural(notCollectedCount, "more row")} below", note, StringComparison.Ordinal);
        Assert.Contains(expected, note, StringComparison.Ordinal);
        Assert.DoesNotContain(notExpected, note, StringComparison.Ordinal);
    }

    /// <summary>
    /// The alert-read note's list of fleet-scoped reads names the two #3826 self-alerts and then says which two they are. Without a
    /// dash, that appositive has to sit in parentheses: a comma would make the two reads look like further list members.
    /// </summary>
    [Fact]
    public void TheFleetScopedReadsList_KeepsTheTwoSelfAlertsAsOneAppositive()
    {
        Assert.DoesNotContain('\u2014', AlertReadFailureCounter.FleetScopedReads);
        Assert.Contains(
            "the two informational store self-alerts #3826 added (the plan dimension's TOAST slack read and the "
            + "store checkpointer pressure read behind the WAL levers), and the store settings self-alert's",
            AlertReadFailureCounter.FleetScopedReads,
            StringComparison.Ordinal);
    }

    /// <summary>
    /// Every other collector fault sentence that is written to <c>collection_log.error_message</c>, and so reaches
    /// <c>get_collection_health</c>'s <c>last_error</c> and <c>get_collection_log</c>'s <c>error_message</c>: the pg_read_file
    /// grant pair, the plain grant, the missing log file, the missing source object with its per-database caution, and the
    /// unsupported source. Each keeps the clause the issue cares about and carries no U+2014.
    /// </summary>
    [Theory]
    [InlineData("42501", "pg_deadlocks", "appdb", "this collector reads the server log with pg_read_file(), which pg_monitor does NOT cover")]
    [InlineData("42501", "pg_deadlocks", "appdb", "the one this collector connects to: issued in a DIFFERENT database on the same cluster, they change nothing here")]
    [InlineData("42501", "pg_wait_stats", "appdb", "the monitoring login lacks a grant this source needs")]
    [InlineData("58P01", "pg_log_events", "appdb", "58P01 is a missing FILE, not a missing grant, and no GRANT changes it (a denial is 42501 and gets different advice)")]
    [InlineData("42P01", "pg_not_a_catalog_collector", "appdb", "which is the one this collector connects to: an extension installed in a DIFFERENT database on the same cluster is invisible from here")]
    [InlineData("0A000", "pg_wait_stats", "appdb", "this source is unsupported or disabled on this server")]
    public void EveryOtherCollectorFaultSentence_CarriesNoEmDash(string sqlState, string collectorName, string connectedDatabase, string keeps)
    {
        var fault = new PostgresException("boom", "ERROR", "ERROR", sqlState);
        var (_, explanation) = DarlingWorker.PostgresFaultOutcome(fault, collectorName, connectedDatabase);
        Assert.Contains(keeps, explanation, StringComparison.Ordinal);
        Assert.DoesNotContain('\u2014', explanation);
    }

    /// <summary>
    /// The three timeout sentences: cancelled by the server's statement_timeout, cut off by this service's own command deadline,
    /// and a 57014 whose wording proves neither. Same surface, same rule.
    /// </summary>
    [Theory]
    [InlineData((int)PostgresCancelSource.TargetStatementTimeout, "canceling statement due to statement timeout", "(SQLSTATE 57014): the target's own statement_timeout expired")]
    [InlineData((int)PostgresCancelSource.OurCommandDeadline, null, "hit its CLIENT-SIDE command deadline after 300,000 ms: its command timeout expired")]
    [InlineData((int)PostgresCancelSource.Unproven, "canceling statement due to user request", "not its statement_timeout wording, so do not change statement_timeout on the target")]
    public void EveryTimeoutExplanation_CarriesNoEmDash(int source, string? serverText, string keeps)
    {
        var explanation = DarlingWorker.PostgresTimeoutExplanation(
            "pg_index_bloat", "appdb", elapsedMs: 300_000, origin: new CollectorFaultCancelOrigin((PostgresCancelSource)source, serverText));
        Assert.Contains(keeps, explanation, StringComparison.Ordinal);
        Assert.DoesNotContain('\u2014', explanation);
    }
}

/// <summary>
/// get_collection_health's <c>collector_detail_note</c> over planted collector runs: one collector that needs a look reads
/// "1 needs a look", and the note's collector count is the count the reply's <c>collectors_total</c> carries.
/// </summary>
[Collection("live-postgres")]
public sealed class CollectionHealthDetailNoteLiveTests
{
    private const string ServerName = "darling-collection-health-note-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task OneCollectorThatNeedsALook_ReadsNeeds_AndTheNoteCountsWhatTheReplyCounts()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the collection-health note live test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            for (var i = 0; i < 6; i++)
            {
                await InsertLogRowAsync(connection, "wait_stats", now.AddHours(-i * 4 - 1), "SUCCESS", 60, 12, null, ct);
                await InsertLogRowAsync(connection, "deadlocks", now.AddHours(-i * 4 - 1), "ERROR", 90, null, "Timeout expired.", ct);
            }

            using var doc = JsonDocument.Parse(await DarlingMcpDataTools.GetCollectionHealth(postgres, ServerName, cancellationToken: ct));
            var root = doc.RootElement;
            var note = root.GetProperty("collector_detail_note").GetString()!;
            var total = root.GetProperty("collectors_total").GetInt32();

            Assert.Contains("1 needs a look", note, StringComparison.Ordinal);
            Assert.DoesNotContain("1 need a look", note, StringComparison.Ordinal);
            Assert.Contains($"of {CountText.Plural(total, "collector")} below", note, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task InsertLogRowAsync(
        NpgsqlConnection connection, string collectorName, DateTime collectionTime, string status,
        int durationMs, int? rowsCollected, string? errorMessage, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO collection_log (log_id, collection_time, server_id, server_name, collector_name, status, duration_ms, rows_collected, error_message)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(collectorName);
        command.Parameters.AddWithValue(status);
        command.Parameters.AddWithValue(durationMs);
        command.Parameters.AddWithValue((object?)rowsCollected ?? DBNull.Value);
        command.Parameters.AddWithValue((object?)errorMessage ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM collection_log WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
