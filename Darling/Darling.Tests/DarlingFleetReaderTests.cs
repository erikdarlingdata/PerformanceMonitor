/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The service-side cross-server fleet reader (#1562, the GAP-1 enriched card + GAP-2 rollup lifted out of the
/// WPF-only ViewerDataService into a scalable, bounded-round-trip reader). Ungated pins cover the Pg dialect of
/// the new cross-server SQL (positional window params, DISTINCT ON per server, no T-SQL) and the pre-banded DTO
/// JSON shape (snake_case field names, string enum bands); a gated (DARLING_TEST_PG) round-trip verifies the
/// per-server XE-preferred / DMV-fallback and the card banding against planted rows, OWN-SCOPED to negative
/// sentinel server ids.
/// </summary>
public sealed class DarlingFleetReaderSqlTests
{
    [Fact]
    public void FleetServersSql_ReadsEnabledRegistryServers()
    {
        var sql = DarlingFleetReader.FleetServersSql;
        Assert.Contains("FROM servers", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE s.is_enabled", sql, StringComparison.Ordinal);
        Assert.Contains("COALESCE(s.display_name, s.server_name)", sql, StringComparison.Ordinal);
        /* The per-server platform signal (D4) rides on the same registry row — no extra round-trip. */
        Assert.Contains("sql_engine_edition", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The is_silenced column (#2031) must stay the SQL mirror of the Viewer's IsWholeServerSilence predicate:
    /// server-scoped (case-insensitive, on the SAME COALESCE(display, storage) name the card shows — the name
    /// the Viewer's Silence writes), enabled, unexpired, and with EVERY narrowing pattern NULL — so a
    /// metric/database/query/wait/job mute an operator authored for the server never renders the whole-server
    /// bell. Losing any one of the five NULL guards would silently widen what counts as "silenced".
    /// </summary>
    [Fact]
    public void FleetServersSql_IsSilenced_MirrorsTheWholeServerSilencePredicate()
    {
        var sql = DarlingFleetReader.FleetServersSql;
        Assert.Contains("EXISTS", sql, StringComparison.Ordinal);
        Assert.Contains("FROM config_mute_rules m", sql, StringComparison.Ordinal);
        Assert.Contains("lower(m.server_name) = lower(COALESCE(s.display_name, s.server_name))", sql, StringComparison.Ordinal);
        Assert.Contains("m.enabled", sql, StringComparison.Ordinal);
        Assert.Contains("m.expires_at_utc IS NULL OR m.expires_at_utc >", sql, StringComparison.Ordinal);
        Assert.Contains("m.metric_name IS NULL", sql, StringComparison.Ordinal);
        Assert.Contains("m.database_pattern IS NULL", sql, StringComparison.Ordinal);
        Assert.Contains("m.query_text_pattern IS NULL", sql, StringComparison.Ordinal);
        Assert.Contains("m.wait_type_pattern IS NULL", sql, StringComparison.Ordinal);
        Assert.Contains("m.job_name_pattern IS NULL", sql, StringComparison.Ordinal);
        Assert.Contains("AS is_silenced", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(5, true, false)]     // Azure SQL Database
    [InlineData(8, false, true)]     // Azure SQL Managed Instance
    [InlineData(3, false, false)]    // box Enterprise
    [InlineData(2, false, false)]    // box Standard
    [InlineData(0, false, false)]    // unknown/unprobed
    [InlineData(null, false, false)] // never connected (null edition)
    public void ClassifyPlatform_MapsEngineEdition_ToReliableAzureFlags(int? engineEdition, bool expectAzureSqlDb, bool expectAzureMi)
    {
        /* The reliable per-server platform the composer's D4 auto-greying keys on: only editions 5/8 are Azure;
           every other edition (box) and null (no signal) leave both flags false so the composer keeps the badge. */
        var (isAzureSqlDb, isAzureMi) = DarlingFleetReader.ClassifyPlatform(engineEdition);
        Assert.Equal(expectAzureSqlDb, isAzureSqlDb);
        Assert.Equal(expectAzureMi, isAzureMi);
    }

    [Fact]
    public void LatestSnapshotReads_AreDistinctOnPerServer()
    {
        Assert.Contains("DISTINCT ON (server_id)", DarlingFleetReader.FleetCpuSql, StringComparison.Ordinal);
        /* collection_time leads, matching FleetMemorySql; sample_time is the within-batch tiebreak. For the
           frame, not the cost - LatestCpuReadShapeSqlTests and the constant's own doc carry both halves. */
        Assert.Contains("ORDER BY server_id, collection_time DESC, sample_time DESC", DarlingFleetReader.FleetCpuSql, StringComparison.Ordinal);
        Assert.Contains("DISTINCT ON (server_id)", DarlingFleetReader.FleetMemorySql, StringComparison.Ordinal);
        Assert.Contains("DISTINCT ON (server_id)", DarlingFleetReader.FleetThreadsSql, StringComparison.Ordinal);
    }

    [Fact]
    public void FleetBlockingSql_PerServerXeDmvFallback_WindowedBothBounds()
    {
        var sql = DarlingFleetReader.FleetBlockingSql;
        Assert.Contains("FROM v_blocked_process_reports", sql, StringComparison.Ordinal);
        Assert.Contains("FROM v_dmv_blocking_snapshots", sql, StringComparison.Ordinal);
        Assert.Contains("FULL OUTER JOIN", sql, StringComparison.Ordinal);
        Assert.Contains("COALESCE(xe.server_id, dmv.server_id)", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY server_id", sql, StringComparison.Ordinal);
        Assert.Contains("event_time >= $1", sql, StringComparison.Ordinal);
        Assert.Contains("event_time <= $2", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void FleetDeadlockSql_PerServerCount_WindowedBothBounds()
    {
        var sql = DarlingFleetReader.FleetDeadlockSql;
        Assert.Contains("FROM v_deadlocks", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY server_id", sql, StringComparison.Ordinal);
        Assert.Contains("deadlock_time >= $1", sql, StringComparison.Ordinal);
        Assert.Contains("deadlock_time <= $2", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void FleetMemoryPressureSql_SumsAtNewestSnapshotPerServer()
    {
        var sql = DarlingFleetReader.FleetMemoryPressureSql;
        Assert.Contains("FROM v_memory_grant_stats", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY m.server_id", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void FleetCollectionHealthSql_PerServerPerCollector()
    {
        var sql = DarlingFleetReader.FleetCollectionHealthSql;
        Assert.Contains("FROM v_collection_log", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY server_id, collector_name", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $1", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Regression pin for a fleet false-Offline (found live on prod-pos-use1-monitor-01, 2026-08-30): this
    /// query used to be a bare `GROUP BY server_id` with no bound at all — a full scan of the server's ENTIRE
    /// collection_log history on every fleet-overview call, exactly the pattern already fixed elsewhere the same
    /// day (pg_statement_stats #2691, pg_wait_stats #2695). It must stay windowed like every other fleet read
    /// here (<see cref="FleetCollectionHealthSql_PerServerPerCollector"/>, <see cref="FleetDeadlockSql_PerServerCount_WindowedBothBounds"/>).
    /// </summary>
    [Fact]
    public void FleetLastCollectionSql_IsWindowed_NotAFullHistoryScan()
    {
        var sql = DarlingFleetReader.FleetLastCollectionSql;
        Assert.Contains("FROM v_collection_log", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY server_id", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $1", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The read-only fleet-tag join (#2020): every (server, tag) assignment, joined to the tag for its name and
    /// stored colour, ordered so a card's pills are stable. Bare table names resolve through the store's
    /// search_path to the config-schema tag tables, the same way FleetServersSql reads servers / config_mute_rules.
    /// </summary>
    [Fact]
    public void FleetTagsSql_JoinsAssignmentsToTags_OrderedForStablePills()
    {
        var sql = DarlingFleetReader.FleetTagsSql;
        Assert.Contains("FROM server_tag_map m", sql, StringComparison.Ordinal);
        Assert.Contains("JOIN server_tags t ON t.id = m.tag_id", sql, StringComparison.Ordinal);
        Assert.Contains("t.colour", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY m.server_id, t.sort_order, lower(t.name)", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The full tag-forest read (#2020) backing the web tree/group view: every tag with its parent, ordering, and
    /// colour, so the fleet page can nest an organisational parent tag that has no directly-assigned servers —
    /// the reason it is a separate read from the per-server <see cref="DarlingFleetReader.FleetTagsSql"/>.
    /// </summary>
    [Fact]
    public void FleetTagForestSql_SelectsTheWholeHierarchy_OrderedForStableSiblings()
    {
        var sql = DarlingFleetReader.FleetTagForestSql;
        Assert.Contains("FROM server_tags", sql, StringComparison.Ordinal);
        Assert.Contains("parent_id", sql, StringComparison.Ordinal);
        Assert.Contains("sort_order", sql, StringComparison.Ordinal);
        Assert.Contains("colour", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY sort_order, lower(name)", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(nameof(DarlingFleetReader.FleetServersSql))]
    [InlineData(nameof(DarlingFleetReader.FleetTagsSql))]
    [InlineData(nameof(DarlingFleetReader.FleetTagForestSql))]
    [InlineData(nameof(DarlingFleetReader.FleetCpuSql))]
    [InlineData(nameof(DarlingFleetReader.FleetMemorySql))]
    [InlineData(nameof(DarlingFleetReader.FleetMemoryPressureSql))]
    [InlineData(nameof(DarlingFleetReader.FleetThreadsSql))]
    [InlineData(nameof(DarlingFleetReader.FleetBlockingSql))]
    [InlineData(nameof(DarlingFleetReader.FleetDeadlockSql))]
    [InlineData(nameof(DarlingFleetReader.FleetLastCollectionSql))]
    [InlineData(nameof(DarlingFleetReader.FleetCollectionHealthSql))]
    public void EveryFleetSql_IsPgDialect_NoTSql(string constName)
    {
        var sql = (string)typeof(DarlingFleetReader).GetField(constName, BindingFlags.Public | BindingFlags.Static)!.GetValue(null)!;
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);          // no T-SQL @params
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);         // no N'' literals
        Assert.DoesNotContain("now(", sql.ToLowerInvariant());              // no bare now()
        Assert.DoesNotContain("getdate", sql.ToLowerInvariant());           // no T-SQL GETDATE()
        Assert.DoesNotContain("[", sql, StringComparison.Ordinal);          // no T-SQL bracket identifiers
    }
}

/// <summary>
/// The pre-banded fleet DTOs' JSON contract (#1562) — the SAME serialized shape the web <c>/api/fleet</c> and the
/// <c>get_fleet_overview</c> MCP tool emit (one <see cref="DarlingFleetReader.JsonOptions"/>). Pins the snake_case
/// field names the browser consumes and that band / severity enums serialize as their STRING names, not ordinals.
/// </summary>
public sealed class DarlingFleetDtoJsonTests
{
    [Fact]
    public void FleetServerCard_SerializesSnakeCase_WithStringBands()
    {
        var card = new FleetServerCard
        {
            ServerId = 7,
            DisplayName = "prod-sql-01",
            ServerName = "prod-sql-01",
            Band = FleetHealthBand.Critical,
            Status = "Online",
            IsOnline = true,
            LastCollectionTime = new DateTime(2026, 7, 18, 3, 30, 0, DateTimeKind.Unspecified),
            CpuPercent = 60,
            TotalCpuPercent = 96,
            CpuSeverity = HealthSeverity.Critical,
            MemorySeverity = HealthSeverity.Healthy,
            BlockingCount = 4,
            BlockingSeverity = HealthSeverity.Warning,
            DeadlockCount = 1,
            DeadlockLastSeen = new DateTime(2026, 7, 18, 3, 15, 0, DateTimeKind.Unspecified),
            DeadlockSeverity = HealthSeverity.Critical,
            ThreadsSeverity = HealthSeverity.Unknown,
            FailedCollectorCount = 0,
            CollectorSeverity = HealthSeverity.Healthy,
            OverallMetricSeverity = HealthSeverity.Critical,
        };

        var json = JsonSerializer.Serialize(card, DarlingFleetReader.JsonOptions);

        foreach (var field in new[]
        {
            "\"server_id\"", "\"display_name\"", "\"server_name\"", "\"engine_edition\"",
            "\"is_azure_sql_db\"", "\"is_azure_mi\"", "\"is_silenced\"", "\"tags\"", "\"band\"", "\"status\"",
            "\"is_online\"", "\"last_collection\"", "\"cpu_percent\"", "\"total_cpu_percent\"",
            "\"cpu_severity\"", "\"memory_severity\"", "\"blocking_count\"", "\"blocking_severity\"",
            "\"deadlock_count\"", "\"deadlock_last_seen\"", "\"deadlock_severity\"", "\"threads_severity\"",
            "\"failed_collector_count\"", "\"collector_severity\"", "\"overall_metric_severity\"",
        })
        {
            Assert.Contains(field, json, StringComparison.Ordinal);
        }

        /* Bands / severities serialize as strings, not ordinals — the frontend maps a name to a color. */
        JsonAssert.Contains("\"band\": \"Critical\"", json);
        JsonAssert.Contains("\"cpu_severity\": \"Critical\"", json);
        JsonAssert.Contains("\"threads_severity\": \"Unknown\"", json);
        /* naive-UTC instants carry no zone suffix (localized in the browser). */
        JsonAssert.Contains("\"last_collection\": \"2026-07-18T03:30:00\"", json);
        JsonAssert.Contains("\"deadlock_last_seen\": \"2026-07-18T03:15:00\"", json);
        JsonAssert.DoesNotContain("\"cpu_severity\": 3", json);
    }

    [Fact]
    public void FleetServerCard_SerializesPerServerPlatform_ForComposerD4Greying()
    {
        /* Azure SQL DB card: engine_edition 5, is_azure_sql_db true — the frontend matches this against a
           measure's appliesTo.azureSqlDb to auto-grey a measure that platform can't collect. */
        var azure = new FleetServerCard { ServerId = 1, DisplayName = "az-db", ServerName = "az-db", EngineEdition = 5, IsAzureSqlDb = true };
        var azureJson = JsonSerializer.Serialize(azure, DarlingFleetReader.JsonOptions);
        JsonAssert.Contains("\"engine_edition\": 5", azureJson);
        JsonAssert.Contains("\"is_azure_sql_db\": true", azureJson);
        JsonAssert.Contains("\"is_azure_mi\": false", azureJson);

        /* A server that has not connected: null edition serializes as JSON null and both flags are false, so the
           frontend has no signal and keeps the measure badge rather than greying on a guess. */
        var unknown = new FleetServerCard { ServerId = 2, DisplayName = "new", ServerName = "new" };
        var unknownJson = JsonSerializer.Serialize(unknown, DarlingFleetReader.JsonOptions);
        JsonAssert.Contains("\"engine_edition\": null", unknownJson);
        JsonAssert.Contains("\"is_azure_sql_db\": false", unknownJson);
        JsonAssert.Contains("\"is_azure_mi\": false", unknownJson);
    }

    [Fact]
    public void FleetOverviewResult_SerializesRollupShape()
    {
        var result = new FleetOverviewResult
        {
            GeneratedAt = new DateTime(2026, 7, 18, 12, 0, 0, DateTimeKind.Unspecified),
            TotalServers = 3,
            HealthyCount = 1,
            WarningCount = 1,
            CriticalCount = 1,
            OfflineCount = 0,
            ServersWithCollectionFailures = 0,
            TotalBlockingEvents = 9,
            TotalDeadlocks = 2,
            AdditionalProblemCount = 0,
            WorstServers = new[]
            {
                new FleetRankedServer { ServerId = 1, DisplayName = "c1", Band = FleetHealthBand.Critical, Score = 3001, Reason = "Deadlocks 1" },
            },
            Cards = Array.Empty<FleetServerCard>(),
            Tags = new[]
            {
                new FleetTagNode { Id = 5, Name = "Prod", ParentId = null, SortOrder = 0, Colour = "#3B82F6" },
                new FleetTagNode { Id = 6, Name = "US", ParentId = 5, SortOrder = 1, Colour = null },
            },
        };

        var json = JsonSerializer.Serialize(result, DarlingFleetReader.JsonOptions);

        foreach (var field in new[]
        {
            "\"generated_at\"", "\"total_servers\"", "\"healthy_count\"", "\"warning_count\"",
            "\"critical_count\"", "\"offline_count\"", "\"servers_with_collection_failures\"",
            "\"total_blocking_events\"", "\"total_deadlocks\"", "\"additional_problem_count\"",
            "\"worst_servers\"", "\"cards\"", "\"band_label\"", "\"reason\"", "\"score\"",
            "\"tags\"", "\"parent_id\"", "\"sort_order\"",
        })
        {
            Assert.Contains(field, json, StringComparison.Ordinal);
        }

        JsonAssert.Contains("\"band_label\": \"Critical\"", json);
    }
}

/// <summary>The new fleet MCP tool's surface — exactly get_fleet_overview, static, string-returning, on a
/// discoverable [McpServerToolType] class (the WithGeminiCompatibleTools registration path's requirements).</summary>
public sealed class DarlingMcpFleetToolsSurfaceTests
{
    [Fact]
    public void ToolSurface_ExactlyGetFleetOverview()
    {
        var toolMethods = typeof(DarlingMcpFleetTools)
            .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
            .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
            .ToList();

        Assert.Equal(
            new[] { "get_fleet_overview" },
            toolMethods.Select(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name).ToArray());

        Assert.NotNull(typeof(DarlingMcpFleetTools).GetCustomAttribute<McpServerToolTypeAttribute>());
        Assert.All(toolMethods, m => Assert.True(m.IsStatic));
        Assert.All(toolMethods, m => Assert.Equal(typeof(Task<string>), m.ReturnType));
    }
}

/// <summary>
/// #3017: the denominator beside <c>total_deadlocks</c>.
///
/// <para><b>The defect.</b> <see cref="DarlingFleetReader.FleetDeadlockSql"/> reads <c>v_deadlocks</c>, which
/// is <c>SELECT * FROM deadlocks</c>, and <c>deadlocks</c> is written by exactly one collector — the SQL
/// Server extended-event capture. A PostgreSQL target's deadlocks go to <c>pg_deadlocks</c>, nothing joins
/// the two, and there is no <c>v_pg_deadlocks</c> at all. So on a PostgreSQL fleet <c>total_deadlocks</c> is
/// permanently zero, for every server, whatever those clusters do and whatever grants the role holds — and
/// zero is also exactly what a genuinely quiet SQL Server fleet reports. The reading that needs no action and
/// the reading that does not cover the fleet were the same character, which is what made the bare number
/// unusable.</para>
///
/// <para><b>Both directions, deliberately.</b> The failure mode of a coverage figure is over-exclusion: a
/// denominator that quietly shrinks reads as a smaller fleet, which is a new wrong number rather than a fix.
/// So every test asserting a band is EXCLUDED sits beside one asserting the degraded-but-reading bands are
/// INCLUDED.</para>
/// </summary>
public sealed class DarlingFleetDeadlockCoverageTests
{
    /* Every band a collector that DID read can be in. Their rows are in the total, so all four count as
       covered — including FAILING, which is loud on its own surfaces and does not need to be hidden inside a
       coverage number to be seen. */
    [Theory]
    [InlineData(CollectorHealthClassifier.Healthy)]
    [InlineData(CollectorHealthClassifier.Warning)]
    [InlineData(CollectorHealthClassifier.Stale)]
    [InlineData(CollectorHealthClassifier.Failing)]
    public void ADeadlockReaderThatWorked_Counts_EvenDegraded(string band)
        => Assert.Equal(
            FleetDeadlockSource.Read,
            FleetDeadlockCoverage.ClassifyDeadlockSource(isPostgres: false, band));

    /// <summary>
    /// The excluded bands, each attributed to the cause that names its fix. STOPPED, NEVER_RUN and a null
    /// band (the <c>deadlocks</c> collector left no row in the health window at all) are one bucket because
    /// they take one action — look at the collector. NO_PERMISSIONS is its own, because a grant is the fix
    /// and no amount of looking at the collector produces one.
    /// </summary>
    [Theory]
    [InlineData(CollectorHealthClassifier.Stopped, FleetDeadlockSource.CollectorSilent)]
    [InlineData(CollectorHealthClassifier.NeverRun, FleetDeadlockSource.CollectorSilent)]
    [InlineData(null, FleetDeadlockSource.CollectorSilent)]
    [InlineData(CollectorHealthClassifier.NoPermissions, FleetDeadlockSource.CollectorDenied)]
    public void ADeadlockReaderThatReadNothing_DoesNotCount_AndNamesItsCause(
        string? band, FleetDeadlockSource expected)
        => Assert.Equal(expected, FleetDeadlockCoverage.ClassifyDeadlockSource(isPostgres: false, band));

    /// <summary>
    /// The issue's own case: a PostgreSQL target is never covered, and its collector's band cannot change
    /// that. <c>pg_deadlocks</c> can be perfectly HEALTHY on all fifty targets and this total still counts
    /// none of it — the rows are in a different table. That is why PostgreSQL is asked before any band.
    /// </summary>
    [Theory]
    [InlineData(CollectorHealthClassifier.Healthy)]
    [InlineData(CollectorHealthClassifier.NoPermissions)]
    [InlineData(CollectorHealthClassifier.Stopped)]
    [InlineData(null)]
    public void APostgresTarget_IsNeverCovered_WhateverItsCollectorSays(string? band)
        => Assert.Equal(
            FleetDeadlockSource.PostgresTarget,
            FleetDeadlockCoverage.ClassifyDeadlockSource(isPostgres: true, band));

    /// <summary>
    /// A card that sets nothing reads as UNCOVERED, and that is the load-bearing default. <c>DeadlockSource</c>
    /// is derived rather than assigned precisely so a card built by a path nobody re-reads cannot sit at an
    /// enum default meaning "read" and inflate the fleet's coverage — the silent-default shape the READER's
    /// own <c>AbandonedCount</c> comment (inside <c>FleetCollectionHealthSql</c>) already names as a defect
    /// class: "it would COMPILE, because the default is silent".
    /// </summary>
    [Fact]
    public void ACardThatMakesNoClaim_IsUncovered_NotCovered()
    {
        var card = new FleetServerCard { ServerId = 1, DisplayName = "target-a", ServerName = "target-a" };

        Assert.Equal(FleetDeadlockSource.CollectorSilent, card.DeadlockSource);
        Assert.NotEqual(FleetDeadlockSource.Read, card.DeadlockSource);

        var rollup = DarlingFleetReader.BuildRollup(
            new[] { card }, Now, Now.AddHours(-1), Now);

        Assert.Equal(0, rollup.DeadlockCoverage.ServersRead);
        Assert.Equal(1, rollup.DeadlockCoverage.ServersTotal);
    }

    /// <summary>
    /// The reduction, over one card of every kind: the coverage comes from the CARDS, the same way the totals
    /// beside it do, so the denominator and the number it qualifies reconcile by construction rather than by
    /// two queries agreeing.
    /// </summary>
    [Fact]
    public void BuildRollup_ReducesCoverageFromTheCards_WithEveryCauseAttributed()
    {
        var rollup = DarlingFleetReader.BuildRollup(
            new[]
            {
                Card(1, band: CollectorHealthClassifier.Healthy),
                Card(2, band: CollectorHealthClassifier.Failing),
                Card(3, isPostgres: true),
                Card(4, isPostgres: true),
                Card(5, band: CollectorHealthClassifier.Stopped),
                Card(6, band: CollectorHealthClassifier.NoPermissions),
                Card(7, band: null),
            },
            Now, Now.AddHours(-1), Now);

        var coverage = rollup.DeadlockCoverage;

        Assert.Equal(2, coverage.ServersRead);
        Assert.Equal(7, coverage.ServersTotal);
        Assert.Equal(2, coverage.PostgresServers);
        Assert.Equal(2, coverage.ServersCollectorSilent);   // STOPPED + the null band
        Assert.Equal(1, coverage.ServersCollectorDenied);

        /* Every server is accounted for exactly once — an unattributed server would mean coverage that
           reports a gap it cannot explain, which is the same shape as a total that reports no denominator. */
        Assert.Equal(
            coverage.ServersTotal,
            coverage.ServersRead + coverage.PostgresServers
                + coverage.ServersCollectorSilent + coverage.ServersCollectorDenied);

        /* And it agrees with the field the fleet already reported. */
        Assert.Equal(rollup.TotalServers, coverage.ServersTotal);
    }

    /// <summary>
    /// The measured case, end to end: a PostgreSQL-only fleet reports <c>total_deadlocks: 0</c> with zero
    /// coverage beside it, and the note sends the reader to the tool that can actually answer.
    /// </summary>
    [Fact]
    public void APostgresOnlyFleet_ReportsZeroCoverage_AndNamesTheToolThatCanAnswer()
    {
        var rollup = DarlingFleetReader.BuildRollup(
            new[] { Card(1, isPostgres: true), Card(2, isPostgres: true), Card(3, isPostgres: true) },
            Now, Now.AddHours(-1), Now);

        Assert.Equal(0, rollup.TotalDeadlocks);
        Assert.Equal(0, rollup.DeadlockCoverage.ServersRead);
        Assert.Equal(3, rollup.DeadlockCoverage.PostgresServers);

        var note = rollup.DeadlockCoverage.Note;

        Assert.Contains("read a deadlock source for 0 of 3", note, StringComparison.Ordinal);
        Assert.Contains("get_pg_deadlocks", note, StringComparison.Ordinal);

        /* The two causes that do not apply are absent, so a reader is not handed three actions when one is
           called for. */
        Assert.DoesNotContain("get_collection_health", note, StringComparison.Ordinal);
        Assert.DoesNotContain("needs a grant", note, StringComparison.Ordinal);
    }

    /// <summary>
    /// <b>The sentence this whole issue turns on.</b> The two windows genuinely diverge —
    /// <c>total_deadlocks</c> is counted between the caller's bounds (one hour by default), while coverage is
    /// banded over a FIXED trailing seven days of collection health — and that divergence is correct: whether
    /// a reader works is a durable fact, and the banding thresholds are themselves defined in DAYS, so an
    /// hour-wide health window could not produce a band at all.
    ///
    /// <para>But a note claiming both were "read in this window" would be the same defect one level up: a
    /// surface asserting a scope it did not measure. So the note names each window against the figure it
    /// belongs to, and says outright that coverage makes no claim about the caller's window.</para>
    /// </summary>
    [Fact]
    public void TheNote_NamesEachWindow_AndClaimsNeitherForTheOther()
    {
        var note = DarlingFleetReader.BuildRollup(
            new[] { Card(1, band: CollectorHealthClassifier.Healthy) },
            Now, Now.AddHours(-1), Now).DeadlockCoverage.Note;

        /* The coverage figure's own window, named as fixed and as seven days. */
        Assert.Contains("fixed trailing seven days of collection health", note, StringComparison.Ordinal);

        /* The total's window, named as the caller's bounds and as the ONLY thing bounded by them. */
        Assert.Contains(
            "total_deadlocks counts only between window_start and window_end", note, StringComparison.Ordinal);

        /* And the disclaimer that keeps the first from being read as the second. */
        Assert.Contains(
            "makes no claim about what was read inside window_start..window_end", note, StringComparison.Ordinal);

        /* The wording that would have been the defect: coverage described as something measured inside the
           caller's window. */
        Assert.DoesNotContain("read in this window", note, StringComparison.Ordinal);
        Assert.DoesNotContain("in this window for", note, StringComparison.Ordinal);
    }

    /// <summary>
    /// The wire shape, on the same <see cref="DarlingFleetReader.JsonOptions"/> the web <c>/api/fleet</c> and
    /// the <c>get_fleet_overview</c> MCP tool both serialize with: the coverage rides beside the total it
    /// qualifies, and the per-card cause is a NAME rather than an ordinal a reordering would move.
    /// </summary>
    [Fact]
    public void TheCoverageSerializes_BesideTheTotal_WithTheCauseAsAName()
    {
        var json = JsonSerializer.Serialize(
            DarlingFleetReader.BuildRollup(
                new[] { Card(1, isPostgres: true), Card(2, band: CollectorHealthClassifier.Healthy) },
                Now, Now.AddHours(-1), Now),
            DarlingFleetReader.JsonOptions);

        foreach (var field in new[]
        {
            "\"total_deadlocks\"", "\"deadlock_coverage\"", "\"servers_read\"", "\"servers_total\"",
            "\"postgres_servers\"", "\"servers_collector_silent\"", "\"servers_collector_denied\"",
            "\"note\"", "\"deadlock_source\"", "\"deadlock_collector_band\"",
        })
        {
            Assert.Contains(field, json, StringComparison.Ordinal);
        }

        JsonAssert.Contains("\"deadlock_source\": \"PostgresTarget\"", json);
        JsonAssert.Contains("\"deadlock_source\": \"Read\"", json);
        JsonAssert.Contains("\"servers_read\": 1", json);
    }

    private static readonly DateTime Now = new(2026, 9, 5, 12, 0, 0, DateTimeKind.Unspecified);

    private static FleetServerCard Card(int id, bool isPostgres = false, string? band = null) =>
        new()
        {
            ServerId = id,
            DisplayName = "target-" + id.ToString(CultureInfo.InvariantCulture),
            ServerName = "target-" + id.ToString(CultureInfo.InvariantCulture),
            IsPostgres = isPostgres,
            DeadlockCollectorBand = band,
        };
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trip for the fleet reader against planted rows across MULTIPLE servers —
/// the cross-server read the Dashboard / Lite cannot run. Verifies each server's XE-preferred / DMV-fallback
/// blocking count and the resulting card band, OWN-SCOPED to negative sentinel server ids (never global rollup
/// counts, which real store data would pollute). Shares the serialized "live-postgres" collection.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingFleetReaderLivePostgresTests
{
    private const int XeServerId = -951001;   // 2 XE reports + 1 DMV snapshot -> XE preferred (2), no deadlock -> Warning
    private const int DmvServerId = -951002;  // 3 DMV snapshots (fallback), 1 deadlock -> Critical
    private const int NoHistoryServerId = -951003;  // registered, zero collection_log rows ever -> never collected
    private const string XeName = "fleet-reader-e2e-xe";
    private const string DmvName = "fleet-reader-e2e-dmv";
    private const string NoHistoryName = "fleet-reader-e2e-no-history";

    /// <summary>
    /// Regression test for a fleet false-Offline found live on prod-pos-use1-monitor-01 (2026-08-30): a server
    /// with NO row in <c>collection_log</c> at all — the "never collected" bootstrap state — must band Warning
    /// with <c>IsOnline = null</c> and <c>AwaitingFirstCollection = true</c>
    /// (<see cref="ServerCollectionStatusRules.FlagsFor"/>), never <c>FleetHealthBand.Offline</c> with
    /// <c>IsOnline = false</c>. Before the fix, <c>ReadLastCollectionAsync</c>'s dictionary miss fell through
    /// `TryGetValue(..., out var lastColl)` to `default(DateTime)` (0001-01-01) — a value that IS non-null, so
    /// <c>ClassifyFreshness</c> never reached its `NeverCollected` branch and instead computed an age of ~2000
    /// years, which is always past <c>OfflineThreshold</c>. This is exactly the failure mode that showed up live
    /// as a rotating set of actively-collecting servers reading "Offline - no recent collection".
    /// </summary>
    [Fact]
    public async Task GetFleetOverview_ServerWithNoCollectionHistory_ReadsAwaitingFirstCollection_NotOffline()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live fleet-reader test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteSentinelRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);

        var bodySucceeded = false;
        try
        {
            var now = DateTime.UtcNow;

            /* Registered, but no collection_log row is ever inserted for this server_id. */
            await InsertServerAsync(connection, NoHistoryServerId, NoHistoryName, 3, ct);

            var result = await DarlingFleetReader.GetFleetOverviewAsync(postgres, now.AddHours(-1), now, now, cancellationToken: ct);

            var card = result.Cards.Single(c => c.ServerId == NoHistoryServerId);

            Assert.Null(card.IsOnline);
            Assert.True(card.AwaitingFirstCollection);
            Assert.Equal(FleetHealthBand.Warning, card.Band);
            Assert.NotEqual(FleetHealthBand.Offline, card.Band);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteSentinelRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task GetFleetOverview_PerServerFallbackAndBanding_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live fleet-reader test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteSentinelRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);

        var bodySucceeded = false;
        try
        {
            var now = DateTime.UtcNow;
            var at = now.AddMinutes(-5);   // inside the [now-1h, now] card window

            /* Plant representative editions so the read-through card platform can be asserted end-to-end:
               XE server = Azure SQL Database (engine edition 5), DMV server = box Enterprise (edition 3). */
            await InsertServerAsync(connection, XeServerId, XeName, 5, ct);
            await InsertServerAsync(connection, DmvServerId, DmvName, 3, ct);

            /* XE server: 2 XE reports + 1 DMV snapshot -> fallback prefers XE (count 2). */
            await InsertBlockedProcessAsync(connection, XeServerId, XeName, at, ct);
            await InsertBlockedProcessAsync(connection, XeServerId, XeName, at.AddMinutes(1), ct);
            await InsertDmvBlockingAsync(connection, XeServerId, XeName, at, ct);

            /* DMV-only server: 3 DMV snapshots (fallback), 1 deadlock. */
            await InsertDmvBlockingAsync(connection, DmvServerId, DmvName, at, ct);
            await InsertDmvBlockingAsync(connection, DmvServerId, DmvName, at.AddMinutes(1), ct);
            await InsertDmvBlockingAsync(connection, DmvServerId, DmvName, at.AddMinutes(2), ct);
            await InsertDeadlockAsync(connection, DmvServerId, DmvName, at, ct);

            /* Fresh collection for both so freshness reads Online (not Offline). */
            await InsertCollectionLogAsync(connection, XeServerId, XeName, now.AddSeconds(-30), ct);
            await InsertCollectionLogAsync(connection, DmvServerId, DmvName, now.AddSeconds(-30), ct);

            var result = await DarlingFleetReader.GetFleetOverviewAsync(postgres, now.AddHours(-1), now, now, cancellationToken: ct);

            var xe = result.Cards.Single(c => c.ServerId == XeServerId);
            var dmv = result.Cards.Single(c => c.ServerId == DmvServerId);

            /* XE preferred: 2 events, no deadlock -> Warning band. */
            Assert.Equal(2, xe.BlockingCount);
            Assert.Equal(0, xe.DeadlockCount);
            Assert.Equal(FleetHealthBand.Warning, xe.Band);
            Assert.True(xe.IsOnline);

            /* Per-server platform read through to the card (D4): engine edition 5 -> Azure SQL DB. */
            Assert.Equal(5, xe.EngineEdition);
            Assert.True(xe.IsAzureSqlDb);
            Assert.False(xe.IsAzureManagedInstance);

            /* DMV fallback: 3 events + a deadlock -> Critical band. */
            Assert.Equal(3, dmv.BlockingCount);
            Assert.Equal(1, dmv.DeadlockCount);
            Assert.Equal(FleetHealthBand.Critical, dmv.Band);

            /* A box edition (3) is neither Azure flag — the composer keeps the on-prem badges. */
            Assert.Equal(3, dmv.EngineEdition);
            Assert.False(dmv.IsAzureSqlDb);
            Assert.False(dmv.IsAzureManagedInstance);

            /* The Critical sentinel outranks the Warning sentinel in the worst-first ordering. */
            var xeScore = ServerHealthClassifier.FleetHealthScore(xe.Band, xe.ToHealthMetrics());
            var dmvScore = ServerHealthClassifier.FleetHealthScore(dmv.Band, dmv.ToHealthMetrics());
            Assert.True(dmvScore > xeScore);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteSentinelRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task InsertServerAsync(NpgsqlConnection connection, int serverId, string name, int engineEdition, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_engine_edition) VALUES ($1, $2, $3, TRUE, $4)", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(name);
        command.Parameters.AddWithValue(name);
        command.Parameters.AddWithValue(engineEdition);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertBlockedProcessAsync(NpgsqlConnection connection, int serverId, string name, DateTime eventTime, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time) VALUES ($1, $2, $3, $4, $5)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(eventTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(name);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(eventTime, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertDmvBlockingAsync(NpgsqlConnection connection, int serverId, string name, DateTime eventTime, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, server_id, server_name, event_time) VALUES ($1, $2, $3, $4, $5)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(eventTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(name);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(eventTime, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertDeadlockAsync(NpgsqlConnection connection, int serverId, string name, DateTime deadlockTime, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time) VALUES ($1, $2, $3, $4, $5)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(deadlockTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(name);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(deadlockTime, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertCollectionLogAsync(NpgsqlConnection connection, int serverId, string name, DateTime collectionTime, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status) VALUES ($1, $2, $3, 'wait_stats', $4, 'SUCCESS')", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(name);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteSentinelRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        foreach (var table in new[] { "blocked_process_reports", "dmv_blocking_snapshots", "deadlocks", "collection_log", "servers" })
        {
            using var cleanup = new NpgsqlCommand(
                $"DELETE FROM {table} WHERE server_id IN ({XeServerId}, {DmvServerId}, {NoHistoryServerId});", connection);
            await cleanup.ExecuteNonQueryAsync(ct);
        }
    }
}
