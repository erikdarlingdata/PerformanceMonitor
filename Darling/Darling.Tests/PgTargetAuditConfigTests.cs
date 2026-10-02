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
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Lane 44 of #3691 (line 70, Erik's ruling 2026-09-22) — <c>audit_config</c> answers a PostgreSQL target with
/// the target's own settings, executed end to end against a planted <c>pg_server_config</c> snapshot.
///
/// <para><b>What this file proves that a source pin cannot.</b> The projection is a payload SHAPE promise made in
/// the tool's description: one row per <c>CONFIG_PG_*</c> fact, <c>setting</c> naming the <c>pg_settings</c> row an
/// operator would go set, <c>current_value</c> a STRING carrying its unit, <c>engine</c> where <c>edition</c> sits
/// on the SQL Server arm, and NO <c>suggested_value</c> key at all. Every one of those is a property of the
/// serialized JSON — a shape that compiles and string-pins perfectly while emitting <c>"current_value": 4</c> or
/// a <c>suggested_value: null</c> the discipline forbids. So the pass runs, the tool runs, and the JSON is read.
/// <see cref="PgTargetMcpSurfaceTests.AuditConfig_ProjectsConfigPgFacts_ForAPostgresTarget"/> holds the arm's
/// structure; this holds what a caller receives.</para>
///
/// <para><b>Two arms, because rider 1 is an engine difference.</b> The stock target's rows are graded by the pass
/// (the two knobs at their shipped defaults are <c>review</c> at 0.4; the tuned convention checks are <c>ok</c>),
/// and on Aurora <c>checkpoint_timeout</c> and <c>max_wal_size</c> become <c>not_applicable</c> — counted on their
/// own summary line, EXCLUDED from <c>settings_checked</c>, and carrying the sentence that says the storage layer
/// owns checkpointing. A knob the engine does not consult is not a finding and was not checked; folding it into
/// either count would make the audit overstate both what it looked at and what it found.</para>
///
/// <para><b>Absences asserted as absences.</b> <c>threshold_lineage</c> is <c>null</c> on every config row — not
/// 0 — because the config family's bars are PostgreSQL's own shipped defaults and there is no chosen number to
/// disclose (<c>PgTargetScorer.Config</c> states this in its class summary); <c>object_name</c> is OMITTED on a
/// server-level row rather than null, so a reader of a <c>shared_buffers</c> row is never invited to wonder which
/// table it meant; and <c>total_physical_memory_mb</c> is <c>null</c> on a stock target because nothing reports
/// the host's RAM there. Asserting <c>0</c> or <c>null</c>-as-present in any of those places would pin the exact
/// lie each one exists to avoid.</para>
///
/// <para>Gated on <c>DARLING_TEST_PG</c> like every live class here, serialized on the shared store, and cleaned
/// up through <see cref="LiveStoreCleanup"/> on its own connection.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgTargetAuditConfigTests
{
    private const string StockServerName = "darling-pg-audit-config-stock";
    private static readonly int StockServerId = ServerIdHelper.GetDeterministicHashCode(StockServerName);
    private const string AuroraServerName = "darling-pg-audit-config-aurora";
    private static readonly int AuroraServerId = ServerIdHelper.GetDeterministicHashCode(AuroraServerName);

    /// <summary>
    /// The stock arm: <c>engine</c> present and <c>edition</c> absent, one row per planted <c>CONFIG_PG_*</c> fact
    /// with its setting name and a unit-carrying string value, statuses off the pass's severities, and no
    /// <c>suggested_value</c> anywhere in the payload.
    /// </summary>
    [Fact]
    public async Task APostgresTarget_ProjectsItsConfigPgFacts_WithUnitsAndNoSuggestedValue()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the audit_config projection e2e.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, StockServerId, StockServerName, MonitoredEngineKind.Postgres, 18, ct);

            var windowEnd = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, StockServerId, StockServerName, windowEnd.AddMinutes(-30), ct);
            await PgTargetKnobsTests.PlantConfigSnapshotAsync(connection, StockServerId, StockServerName, windowEnd.AddMinutes(-30), ct);

            var service = new DarlingAnalysisService(postgres);
            var json = await DarlingMcpTools.AuditConfig(service, postgres, StockServerName);

            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            /* The engine token stands where the SQL Server arm's edition does — and edition is not merely empty
               here, it is absent: a PostgreSQL payload carrying "edition": null would be the same false
               edition-awareness #3653 removed from the description. */
            Assert.Equal(StockServerName, root.GetProperty("server").GetString());
            Assert.Equal(MonitoredEngineKind.Postgres, root.GetProperty("engine").GetString());
            Assert.False(root.TryGetProperty("edition", out _));

            /* Nothing measured the host's RAM on a stock target: null, never 0. */
            Assert.Equal(JsonValueKind.Null, root.GetProperty("total_physical_memory_mb").ValueKind);

            var rows = root.GetProperty("recommendations").EnumerateArray().ToList();
            Assert.NotEmpty(rows);

            /* Every row: a setting name that is not the fact key, a STRING current_value, a status from the
               vocabulary, a recommendation with both halves of the advice, an absent suggested_value, and
               threshold_lineage null rather than 0 (the config family stamps no lineage — the bar is
               PostgreSQL's own default). */
            foreach (var row in rows)
            {
                var factKey = row.GetProperty("fact_key").GetString()!;
                Assert.StartsWith(PgTargetFactKeys.ConfigPrefix, factKey, StringComparison.Ordinal);
                Assert.Equal(PgTargetFactKeys.ConfigPgSettingName(factKey), row.GetProperty("setting").GetString());
                Assert.Equal(JsonValueKind.String, row.GetProperty("current_value").ValueKind);
                Assert.Contains(row.GetProperty("status").GetString(), new[] { "ok", "review", "warning", "not_applicable" });
                Assert.False(string.IsNullOrWhiteSpace(row.GetProperty("recommendation").GetString()));
                Assert.False(row.TryGetProperty("suggested_value", out _), $"{factKey} carries a suggested_value — the PostgreSQL arm's remediation sentence IS the recommendation");
                Assert.Equal(JsonValueKind.Null, row.GetProperty("threshold_lineage").ValueKind);
            }

            /* No key in the whole payload is suggested_value, at any depth. */
            Assert.DoesNotContain("suggested_value", json, StringComparison.Ordinal);

            JsonElement Row(string key) => rows.Single(r => r.GetProperty("fact_key").GetString() == key);

            /* The two knobs at their shipped defaults: the advisory band (0.4) spells `review`, and each value
               reads as an operator set it — 128 MB, 1 GB — not as the raw megabyte double. */
            var sharedBuffers = Row(PgTargetFactKeys.ConfigSharedBuffers);
            Assert.Equal("shared_buffers", sharedBuffers.GetProperty("setting").GetString());
            Assert.Equal("128 MB", sharedBuffers.GetProperty("current_value").GetString());
            Assert.Equal("review", sharedBuffers.GetProperty("status").GetString());
            Assert.Contains("the initdb default", sharedBuffers.GetProperty("recommendation").GetString()!, StringComparison.Ordinal);
            /* object_name is OMITTED on a server-level setting, not null. */
            Assert.False(sharedBuffers.TryGetProperty("object_name", out _));

            var maxWal = Row(PgTargetFactKeys.ConfigMaxWalSize);
            Assert.Equal("1 GB", maxWal.GetProperty("current_value").GetString());
            Assert.Equal("review", maxWal.GetProperty("status").GetString());

            /* The convention checks the snapshot planted TUNED score 0 and read `ok` — and their units are the
               three shapes the formatter distinguishes: megabytes, a bare ratio, and a boolean word. */
            var effectiveCache = Row(PgTargetFactKeys.ConfigEffectiveCacheSize);
            Assert.Equal("8 GB", effectiveCache.GetProperty("current_value").GetString());
            Assert.Equal("ok", effectiveCache.GetProperty("status").GetString());

            var randomPageCost = Row(PgTargetFactKeys.ConfigRandomPageCost);
            Assert.Equal("random_page_cost", randomPageCost.GetProperty("setting").GetString());
            Assert.Equal("1.1", randomPageCost.GetProperty("current_value").GetString());
            Assert.Equal("ok", randomPageCost.GetProperty("status").GetString());

            var trackIoTiming = Row(PgTargetFactKeys.ConfigTrackIoTiming);
            Assert.Equal("on", trackIoTiming.GetProperty("current_value").GetString());
            Assert.Equal("ok", trackIoTiming.GetProperty("status").GetString());

            /* checkpoint_timeout is base-0 context on a stock target: reported, ok, in seconds-as-set. */
            var checkpointTimeout = Row(PgTargetFactKeys.ConfigCheckpointTimeout);
            Assert.Equal("checkpoint_timeout", checkpointTimeout.GetProperty("setting").GetString());
            Assert.Equal("5 min", checkpointTimeout.GetProperty("current_value").GetString());
            Assert.Equal("ok", checkpointTimeout.GetProperty("status").GetString());

            /* wal_compression off is the shipped value and the boolean's other word. */
            Assert.Equal("off", Row(PgTargetFactKeys.ConfigWalCompression).GetProperty("current_value").GetString());

            /* The summary counts what the rows say, and nothing is not_applicable on a stock target. */
            var summary = root.GetProperty("summary");
            Assert.Equal(rows.Count, summary.GetProperty("settings_checked").GetInt32());
            Assert.Equal(rows.Count(r => r.GetProperty("status").GetString() == "warning"), summary.GetProperty("warnings").GetInt32());
            Assert.Equal(rows.Count(r => r.GetProperty("status").GetString() == "review"), summary.GetProperty("needs_review").GetInt32());
            Assert.Equal(0, summary.GetProperty("not_applicable").GetInt32());
            Assert.True(summary.GetProperty("needs_review").GetInt32() >= 2, "both shipped-default knobs are advisory rows on this snapshot");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// Rider 1 of Erik's ruling: on <c>aurora-postgres</c> the two checkpointing knobs read
    /// <c>not_applicable</c> — counted on their own line, out of <c>settings_checked</c>, and each carrying the
    /// sentence that says who actually owns checkpointing there. The same snapshot, the same tool, one column
    /// of the registry different.
    /// </summary>
    [Fact]
    public async Task AnAuroraTarget_RendersTheCheckpointingKnobsNotApplicable_AndExcludesThemFromTheCheckedCount()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the audit_config Aurora e2e.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, AuroraServerId, AuroraServerName, MonitoredEngineKind.AuroraPostgres, 17, ct);

            var windowEnd = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, AuroraServerId, AuroraServerName, windowEnd.AddMinutes(-30), ct);
            await PgTargetKnobsTests.PlantConfigSnapshotAsync(connection, AuroraServerId, AuroraServerName, windowEnd.AddMinutes(-30), ct);

            var service = new DarlingAnalysisService(postgres);
            var json = await DarlingMcpTools.AuditConfig(service, postgres, AuroraServerName);

            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            Assert.Equal(MonitoredEngineKind.AuroraPostgres, root.GetProperty("engine").GetString());

            var rows = root.GetProperty("recommendations").EnumerateArray().ToList();
            JsonElement Row(string key) => rows.Single(r => r.GetProperty("fact_key").GetString() == key);

            /* The knob the collector stamps (lane 15 / #3728 §A4) and the context fact the ENGINE makes moot:
               both not_applicable, both still REPORTING the parameter group's value, because what Aurora holds
               for a knob it ignores is a fact about the parameter group and worth seeing. */
            var maxWal = Row(PgTargetFactKeys.ConfigMaxWalSize);
            Assert.Equal("not_applicable", maxWal.GetProperty("status").GetString());
            Assert.Equal("1 GB", maxWal.GetProperty("current_value").GetString());
            /* Lane 15's own composed sentence, verbatim from PgTargetAdvice.Write's Aurora arm — the headline
               says it is not a finding HERE and the remediation redirects to where Aurora does report write
               pressure. Asserted as the composed pair this tool renders (headline + " " + remediation), which
               is what a caller reads. */
            Assert.Contains("not a finding on Aurora, where the engine does not consult it",
                maxWal.GetProperty("recommendation").GetString()!, StringComparison.Ordinal);
            Assert.Contains("Read write pressure where Aurora reports it",
                maxWal.GetProperty("recommendation").GetString()!, StringComparison.Ordinal);

            var checkpointTimeout = Row(PgTargetFactKeys.ConfigCheckpointTimeout);
            Assert.Equal("not_applicable", checkpointTimeout.GetProperty("status").GetString());
            Assert.Equal("5 min", checkpointTimeout.GetProperty("current_value").GetString());
            Assert.Contains("Not applicable on Aurora PostgreSQL", checkpointTimeout.GetProperty("recommendation").GetString()!, StringComparison.Ordinal);

            /* shared_buffers is Aurora's too, and it is graded there — the rider is about checkpointing, not
               about switching the audit off. */
            Assert.Equal("review", Row(PgTargetFactKeys.ConfigSharedBuffers).GetProperty("status").GetString());

            /* Counted apart, and out of the checked count: two knobs nothing consults were not checked. */
            var summary = root.GetProperty("summary");
            Assert.Equal(2, summary.GetProperty("not_applicable").GetInt32());
            Assert.Equal(rows.Count - 2, summary.GetProperty("settings_checked").GetInt32());
            Assert.Equal(0, rows.Count(r => r.GetProperty("status").GetString() == "not_applicable") - 2);

            /* And neither is a finding: the Aurora rows are reported, never warned. */
            Assert.DoesNotContain(new[] { maxWal, checkpointTimeout }, r => r.GetProperty("status").GetString() == "warning");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static DateTime TruncateToMinutes(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_database_stats WHERE server_id IN ({StockServerId}, {AuroraServerId}); " +
            $"DELETE FROM pg_server_config WHERE server_id IN ({StockServerId}, {AuroraServerId}); " +
            $"DELETE FROM analysis_findings WHERE server_id IN ({StockServerId}, {AuroraServerId}); " +
            $"DELETE FROM analysis_muted WHERE server_id IN ({StockServerId}, {AuroraServerId}); " +
            $"DELETE FROM servers WHERE server_id IN ({StockServerId}, {AuroraServerId});", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
