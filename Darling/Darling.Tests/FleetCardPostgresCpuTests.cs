/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3267: a PostgreSQL/Aurora target's card carries its instance CPU, and says where the number came from.
///
/// <para><b>The defect.</b> <see cref="DarlingFleetReader"/> read CPU only from
/// <c>v_cpu_utilization_stats</c>, which the SQL Server ring-buffer collector writes and a PostgreSQL target
/// never has a row in. Instance CPU for those targets lands in <c>collect.pg_cpu_utilization</c> from the
/// AWS Performance Insights API (#2719) and nothing on the fleet path looked at it, so every PostgreSQL
/// card read <c>total_cpu_percent: null</c> / <c>cpu_severity: Unknown</c> — permanently, healthy or not —
/// and such a server could only rank in the worst-first list by collector state, never by load.</para>
///
/// <para><b>Why one band over two collectors is correct, recorded rather than assumed.</b> The premise the
/// issue offered was that the cutoffs "presumably apply as-is", and #3268 had just been reverted for
/// shipping an unverified premise, so it was checked from both sides' source instead:
/// <c>CpuUtilizationCollector</c> stores <c>100 - SystemIdle</c> (a host figure including non-database
/// processes, one ring-buffer record a minute) and <c>RdsCpuIngestor</c> asks PI for
/// <c>os.cpuUtilization.total.avg</c> at <c>PeriodInSeconds = 60</c> (a host OS counter, same units, same
/// window). Same quantity, so one ladder. <see cref="ServerHealthClassifier.CpuSeverity"/>'s own doc carries
/// the finding and the two caveats that survive it.</para>
///
/// <para><b>What is pinned here.</b> That the reduction really produces the number (the field case, red
/// against the unfixed reader), that BOTH surfaces that band CPU produce the same answer from the same
/// inputs and do so through the shared decision rather than by agreeing, that no source and 0% cannot read
/// alike, and that the new cross-server read is bounded the way <see cref="DarlingFleetReader.FleetLastCollectionSql"/>'s
/// comment demands of a read over a table that only grows.</para>
/// </summary>
public sealed class FleetCardPostgresCpuTests
{
    private static readonly DateTime Now = new(2026, 9, 10, 12, 0, 0, DateTimeKind.Utc);

    private static FleetServerCard Card(
        string? engineKind,
        double? ringBufferSqlCpu = null,
        double? ringBufferOtherCpu = null,
        double? instanceCpu = null,
        double? acuUtilization = null,
        double? maxConfiguredAcu = null) =>
        DarlingFleetReader.BuildCard(
            new DarlingFleetReader.FleetServerRow(1, "pg-1", "pg-1", null, engineKind, false),
            new DarlingFleetReader.CpuRow(ringBufferSqlCpu, ringBufferOtherCpu),
            new DarlingFleetReader.PgCpuRow(instanceCpu, acuUtilization, maxConfiguredAcu),
            default,
            default,
            default,
            default,
            default,
            Now.AddSeconds(-30),
            default,
            null,
            Now);

    private static ServerSummaryItem ViewerCard(
        string? engineKind,
        double? ringBufferSqlCpu = null,
        double? ringBufferOtherCpu = null,
        double? instanceCpu = null,
        double? acuUtilization = null,
        double? maxConfiguredAcu = null)
    {
        var card = new ServerSummaryItem
        {
            ServerName = "pg-1",
            ServerId = 1,
            CpuPercent = ringBufferSqlCpu,
            OtherProcessCpuPercent = ringBufferOtherCpu,
            InstanceCpuPercent = instanceCpu,
            AcuUtilizationPercent = acuUtilization,
            MaxConfiguredAcu = maxConfiguredAcu,
            IsPostgres = MonitoredEngineKind.IsPostgres(engineKind),
            IsAurora = MonitoredEngineKind.IsAurora(engineKind),
            LastCollectionTime = Now.AddSeconds(-30),
        };
        card.ApplyFreshness(Now);
        return card;
    }

    /* ─────────────────────────── the field case ─────────────────────────── */

    /// <summary>
    /// THE test: an Aurora PostgreSQL target with a current Performance Insights reading gets a real number
    /// and a real band where it used to get null and Unknown. Red against the unfixed reader, which had no
    /// route for this value to arrive by at all.
    ///
    /// <para>The banded figure is 87% of the CONFIGURED ACU ceiling, not 87% CPU — #3281 moved which
    /// percentage this ladder reads, because the raw reading is percent of an allocation that moves. It is
    /// chosen inside the Warning band rather than at a comfortable 20% because a fix that delivered the
    /// number but left it out of <see cref="ServerHealthMetrics.CpuPercentForAlert"/> /
    /// <see cref="ServerHealthMetrics.CapacityUtilizationPercent"/> would pass a value-only assertion and
    /// still leave the card uncoloured — which is most of what #3267 was about. The raw CPU is deliberately
    /// 100 here: a card banded off the raw reading would read Critical on it.</para>
    /// </summary>
    [Fact]
    public void AnAuroraTargetWithAPerformanceInsightsReading_CarriesTheNumberAndTheBand()
    {
        var card = Card(
            MonitoredEngineKind.AuroraPostgres, instanceCpu: 100, acuUtilization: 87, maxConfiguredAcu: 12);

        Assert.Equal(100, card.TotalCpuPercent);
        Assert.Equal(100, card.InstanceCpuPercent);
        Assert.Equal(87, card.AcuUtilizationPercent);
        Assert.Equal(12, card.MaxConfiguredAcu);
        Assert.Equal(HealthSeverity.Warning, card.CpuSeverity);
        Assert.Equal(FleetCpuSource.PerformanceInsights, card.CpuSource);

        /* And it reaches the card's OVERALL band, which is what puts it in the worst-first ranking at all. */
        Assert.Equal(HealthSeverity.Warning, card.OverallMetricSeverity);
        Assert.Equal(FleetHealthBand.Warning, card.Band);

        /* The reason names the figure that DECIDED, and not the raw CPU sitting beside it (#3281). */
        var reason = DarlingFleetReader.BuildReason(card);
        Assert.Contains("Capacity 87% of configured ACU", reason, StringComparison.Ordinal);
        Assert.DoesNotContain("CPU 100%", reason, StringComparison.Ordinal);
    }

    /// <summary>
    /// The per-process fields stay NULL on that card rather than being filled from the total. Performance
    /// Insights publishes the host figure and no breakdown, so a value in either of these would be an
    /// attribution nothing measured — and <c>cpu_source</c> is what a consumer reads instead of inferring
    /// provenance from which fields happen to be null.
    /// </summary>
    [Fact]
    public void ThePerProcessSplitStaysNull_BecausePerformanceInsightsDoesNotPublishOne()
    {
        var card = Card(MonitoredEngineKind.AuroraPostgres, instanceCpu: 87, acuUtilization: 30);

        Assert.Null(card.CpuPercent);
        Assert.Null(card.OtherProcessCpuPercent);
    }

    /* ─────────────────────────── null is not zero ─────────────────────────── */

    /// <summary>
    /// A genuinely idle instance and one with no CPU source do not read alike — the trap this issue names
    /// explicitly. 0% is a measurement (Healthy, source named) and no source is not (Unknown, and a source
    /// arm that says nothing was read).
    /// </summary>
    [Fact]
    public void AnIdleInstanceAndAnUnreadableOne_AreNotTheSameCard()
    {
        var idle = Card(MonitoredEngineKind.AuroraPostgres, instanceCpu: 0, acuUtilization: 0);
        var unread = Card(MonitoredEngineKind.Postgres);

        Assert.Equal(0d, idle.TotalCpuPercent);
        Assert.Equal(HealthSeverity.Healthy, idle.CpuSeverity);
        Assert.Equal(FleetCpuSource.PerformanceInsights, idle.CpuSource);

        Assert.Null(unread.TotalCpuPercent);
        Assert.Equal(HealthSeverity.Unknown, unread.CpuSeverity);
        Assert.Equal(FleetCpuSource.NoSourceForEngine, unread.CpuSource);

        Assert.NotEqual(idle.CpuSource, unread.CpuSource);
        Assert.NotEqual(idle.CpuSeverity, unread.CpuSeverity);
    }

    /// <summary>The same distinction on the viewer's card, where it is a rendered STRING and so the one a
    /// human actually sees: "0%" against "--".</summary>
    [Fact]
    public void TheViewerCardRendersIdleAndUnreadableDifferently()
    {
        Assert.Equal("0%", ViewerCard(MonitoredEngineKind.AuroraPostgres, instanceCpu: 0).CpuDisplay);
        Assert.Equal("--", ViewerCard(MonitoredEngineKind.Postgres).CpuDisplay);
        Assert.Equal("87%", ViewerCard(MonitoredEngineKind.AuroraPostgres, instanceCpu: 87).CpuDisplay);
    }

    /* ─────────────────────────── provenance, all four arms ─────────────────────────── */

    /// <summary>
    /// Every arm of the source classification, including the two that mean "no number" and take opposite
    /// actions: one is a collector to look at, the other is a fact about the engine that no collector,
    /// grant or upgrade changes.
    /// </summary>
    [Theory]
    /* A ring-buffer reading, whatever the engine token says — the SQL Server path is untouched. */
    [InlineData(MonitoredEngineKind.SqlServer, 40.0, 3.0, null, FleetCpuSource.RingBuffer)]
    /* SQL Server on Linux before 2025 CU1: other-process is NULL and it still has a reading. */
    [InlineData(MonitoredEngineKind.SqlServer, 40.0, null, null, FleetCpuSource.RingBuffer)]
    [InlineData(MonitoredEngineKind.AuroraPostgres, null, null, 12.0, FleetCpuSource.PerformanceInsights)]
    /* Aurora with no current reading: the ingestor should be producing one, so this is a gap to chase. */
    [InlineData(MonitoredEngineKind.AuroraPostgres, null, null, null, FleetCpuSource.NotCollected)]
    /* Not-Aurora PostgreSQL: this build collects no instance CPU here at all. */
    [InlineData(MonitoredEngineKind.Postgres, null, null, null, FleetCpuSource.NoSourceForEngine)]
    /* A SQL Server that has not collected yet. Fixable, so NOT the structural arm. */
    [InlineData(MonitoredEngineKind.SqlServer, null, null, null, FleetCpuSource.NotCollected)]
    /* No engine token at all — a row no connect has stamped since V82. Absence is not evidence for either
       engine, so it falls to the arm that claims a gap someone can act on rather than the one that tells
       them not to bother. Same asymmetry as MonitoredEngineKind.IsPostgres and ClassifyPlatform. */
    [InlineData(null, null, null, null, FleetCpuSource.NotCollected)]
    /* A token a NEWER service wrote and this build has never heard of: same reasoning, same arm. */
    [InlineData("some-future-engine", null, null, null, FleetCpuSource.NotCollected)]
    public void TheCpuSourceNamesTheArm(
        string? engineKind, double? sqlCpu, double? otherCpu, double? instanceCpu, FleetCpuSource expected)
    {
        Assert.Equal(expected, Card(engineKind, sqlCpu, otherCpu, instanceCpu).CpuSource);
        Assert.Equal(expected, ViewerCard(engineKind, sqlCpu, otherCpu, instanceCpu).CpuSource);
    }

    /// <summary>
    /// <see cref="FleetCpuSource.NotCollected"/> is the enum's DEFAULT member, so a card assembled by a path
    /// that sets no reading claims nothing rather than landing on an arm that means "measured". The same
    /// discipline <see cref="FleetDeadlockSource"/> needed, and for the same reason: a default that means
    /// "read" inflates a coverage figure silently and compiles.
    /// </summary>
    [Fact]
    public void TheDefaultSourceArm_MeansNothingWasRead()
    {
        Assert.Equal(FleetCpuSource.NotCollected, default(FleetCpuSource));
        Assert.Equal(FleetCpuSource.NotCollected, new FleetServerCard().CpuSource);
    }

    /// <summary>
    /// <c>cpu_source</c> reaches the wire as its STRING name, like every other band on this card: a consumer
    /// switches on a word rather than on an ordinal that adding an arm would move underneath it.
    /// </summary>
    [Fact]
    public void TheCpuSourceSerializesAsItsName()
    {
        var json = System.Text.Json.JsonSerializer.Serialize(
            Card(MonitoredEngineKind.AuroraPostgres, instanceCpu: 87), DarlingFleetReader.JsonOptions);

        JsonAssert.Contains("\"cpu_source\": \"PerformanceInsights\"", json);
        JsonAssert.Contains("\"instance_cpu_percent\": 87", json);
        JsonAssert.Contains("\"total_cpu_percent\": 87", json);
        JsonAssert.Contains("\"cpu_percent\": null", json);
        JsonAssert.Contains("\"other_process_cpu_percent\": null", json);
        JsonAssert.DoesNotContain("\"cpu_source\": 2", json);

        var none = System.Text.Json.JsonSerializer.Serialize(
            Card(MonitoredEngineKind.Postgres), DarlingFleetReader.JsonOptions);

        JsonAssert.Contains("\"cpu_source\": \"NoSourceForEngine\"", none);
        JsonAssert.Contains("\"total_cpu_percent\": null", none);
        JsonAssert.Contains("\"cpu_severity\": \"Unknown\"", none);
    }

    /* ─────────────────────────── the one-ladder rule (#2473) ─────────────────────────── */

    /// <summary>
    /// The two surfaces that band CPU agree, over every combination the arms above cover — the service's
    /// fleet card (<c>get_fleet_overview</c> / <c>/api/fleet</c>) and the viewer's Overview card (which the
    /// FleetView band and the worst-first reason are reduced from). #2473's rule is that a server bands the
    /// same everywhere, and #3267 IS that defect for CPU: one surface read a source the other did not.
    /// </summary>
    [Theory]
    [InlineData(MonitoredEngineKind.AuroraPostgres, null, null, 96.0, null)]
    [InlineData(MonitoredEngineKind.AuroraPostgres, null, null, 87.0, null)]
    [InlineData(MonitoredEngineKind.AuroraPostgres, null, null, 12.0, null)]
    [InlineData(MonitoredEngineKind.AuroraPostgres, null, null, 0.0, null)]
    [InlineData(MonitoredEngineKind.AuroraPostgres, null, null, null, null)]
    /* And the same combinations WITH a capacity reading, which is the figure the band now reads (#3281):
       the raw CPU is held at 100 across all four so the only thing moving is the capacity, and the two
       surfaces have to move together. Without these rows the theory would pass while one surface banded on
       the raw CPU and the other on the ceiling — the exact drift #2473 forbids. */
    [InlineData(MonitoredEngineKind.AuroraPostgres, null, null, 100.0, 96.0)]
    [InlineData(MonitoredEngineKind.AuroraPostgres, null, null, 100.0, 87.0)]
    [InlineData(MonitoredEngineKind.AuroraPostgres, null, null, 100.0, 33.3)]
    [InlineData(MonitoredEngineKind.AuroraPostgres, null, null, 100.0, 0.0)]
    [InlineData(MonitoredEngineKind.Postgres, null, null, null, null)]
    [InlineData(MonitoredEngineKind.SqlServer, 60.0, 40.0, null, null)]
    [InlineData(MonitoredEngineKind.SqlServer, 40.0, null, null, null)]
    [InlineData(MonitoredEngineKind.SqlServer, null, null, null, null)]
    public void BothCpuBandingSurfaces_AgreeOnTheSameServer(
        string? engineKind, double? sqlCpu, double? otherCpu, double? instanceCpu, double? acuUtilization)
    {
        var fleet = Card(engineKind, sqlCpu, otherCpu, instanceCpu, acuUtilization, maxConfiguredAcu: 12);
        var viewer = ViewerCard(engineKind, sqlCpu, otherCpu, instanceCpu, acuUtilization, maxConfiguredAcu: 12);

        Assert.Equal(fleet.TotalCpuPercent, viewer.TotalCpuPercent);
        Assert.Equal(fleet.CpuSeverity, viewer.CpuSeverity);
        Assert.Equal(fleet.CpuSource, viewer.CpuSource);
        Assert.Equal(fleet.AcuUtilizationPercent, viewer.AcuUtilizationPercent);
    }

    /// <summary>
    /// And they agree BY CONSTRUCTION rather than by observation: the two surfaces above are the ONLY
    /// places under <c>Darling/</c> that decide a CPU band, and both reach the shared decision. The theory
    /// above would pass just as happily against two hand-written copies that currently agree, which is
    /// precisely the state #2473 found the collection-status ladder in - three copies agreeing and a fourth
    /// that did not.
    ///
    /// <para>Stated as a CENSUS of the deciding files rather than as a search for a bad copy, because the
    /// failure mode is OMISSION: #3267 is a surface that computed a CPU band while never learning a second
    /// source existed, and no scan for a duplicated expression can see that. A third surface arriving fails
    /// here and has to be given the shared call, which is the decision this forces someone to make. It
    /// fails in both directions - a new decider, or either of these two dropping the call.</para>
    ///
    /// <para>Read from the source tree so it reaches the WPF viewer without this net10.0-windows assembly
    /// resolving a WPF project reference, and stripped with the shared lexer rather than a line prefix
    /// because every doc comment in this family QUOTES the identifiers being counted.</para>
    /// </summary>
    [Fact]
    public void TheOnlyTwoSurfacesThatBandCpu_BothReachTheSharedDecision()
    {
        var deciders = new List<string>();
        var callers = new List<string>();
        var scanned = 0;

        foreach (var file in Directory.EnumerateFiles(Path.Combine(RepoFile.Root, "Darling"), "*.cs", SearchOption.AllDirectories))
        {
            var segments = file.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            if (segments.Contains("bin") || segments.Contains("obj") || segments.Contains("Darling.Tests"))
            {
                continue;
            }

            scanned++;
            var code = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(file));
            var name = Path.GetFileName(file);

            /* Constructing the metric bundle, or calling the CPU ladder directly, is what "decides a CPU
               band" means. Either route counts: the fleet reader builds the bundle, the viewer's card
               property calls the ladder, and a third surface could do either. */
            if (code.Contains("new ServerHealthMetrics", StringComparison.Ordinal)
                || code.Contains("ServerHealthClassifier.CpuSeverity(", StringComparison.Ordinal))
            {
                deciders.Add(name);
            }

            if (code.Contains("FleetCpuProvenance.TotalNonIdleCpuPercent", StringComparison.Ordinal)
                && code.Contains("FleetCpuProvenance.ClassifyCpuSource", StringComparison.Ordinal))
            {
                callers.Add(name);
            }
        }

        /* The scan is the enforcement, so finding almost nothing is a broken scan, not a clean tree. */
        Assert.True(scanned > 50, $"scanned only {scanned} file(s) under Darling/");

        var expected = new[] { "DarlingFleetReader.cs", "ViewerDataService.Overview.cs" };

        Assert.Equal(expected, deciders.Distinct().OrderBy(n => n, StringComparer.Ordinal).ToArray());
        Assert.Equal(expected, callers.Distinct().OrderBy(n => n, StringComparer.Ordinal).ToArray());
    }

    /* ─────────────────────────── the new read's shape ─────────────────────────── */

    /// <summary>
    /// The cross-server read is BOUNDED, for <see cref="DarlingFleetReader.FleetLastCollectionSql"/>'s
    /// reason: <c>pg_cpu_utilization</c> is a hypertable that only grows, and a bare
    /// <c>DISTINCT ON (server_id)</c> over it reads and sorts the whole retained relation on every fleet
    /// call. Both predicates are asserted because they do different jobs — <c>collection_time</c> is the
    /// partition dimension and the only one TimescaleDB can chunk-exclude on, <c>sample_time</c> is what the
    /// reading is ABOUT and so the honest freshness test.
    /// </summary>
    [Fact]
    public void FleetPgCpuSql_IsBoundedOnBothTheChunkKeyAndTheSampleTime()
    {
        var sql = DarlingFleetReader.FleetPgCpuSql;

        Assert.Contains("FROM pg_cpu_utilization", sql, StringComparison.Ordinal);
        Assert.Contains("DISTINCT ON (server_id)", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $1", sql, StringComparison.Ordinal);
        Assert.Contains("sample_time >= $1", sql, StringComparison.Ordinal);

        /* Ordered on the partition column first, with sample_time as the within-cycle tiebreak - the shape
           LatestCpuReadShapeSqlTests pins for the SQL Server twin, earning ordered ChunkAppend. */
        Assert.Contains("ORDER BY server_id, collection_time DESC, sample_time DESC", sql, StringComparison.Ordinal);

        /* PI returns a data point with a NULL value for a period it has no sample for, and the ingestor
           stores it, so the newest ROW is not necessarily the newest MEASUREMENT. */
        Assert.Contains("cpu_percent IS NOT NULL", sql, StringComparison.Ordinal);
    }

    /// <summary>The viewer's per-server twin carries the same bound and the same ordering, so the two
    /// surfaces cannot disagree about which sample is current for the same server.</summary>
    [Fact]
    public void TheViewersPerServerPgCpuRead_HasTheSameShape()
    {
        var sql = ViewerDataService.ServerSummaryPgCpuSql;

        Assert.Contains("FROM pg_cpu_utilization", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("sample_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("cpu_percent IS NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time DESC, sample_time DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Both reads take their staleness window from <c>DarlingPgCpuUtilizationReader.Freshness</c>, the
    /// constant the High CPU alert already uses. Three surfaces answer "where does this server's CPU stand
    /// right now"; two of them holding different windows is a drift of the #2473 kind, where the alert
    /// evaluates a reading the card has already dropped.
    ///
    /// <para>Asserted as a source fact rather than by re-deriving the number, because the failure this
    /// guards against is a second literal appearing, not a wrong value.</para>
    /// </summary>
    [Fact]
    public void BothPgCpuReadsBindTheSharedFreshnessConstant()
    {
        Assert.Equal(TimeSpan.FromMinutes(15), DarlingPgCpuUtilizationReader.Freshness);

        foreach (var path in new[]
        {
            Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingFleetReader.cs"),
            Path.Combine("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.Overview.cs"),
        })
        {
            var code = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile(path));

            Assert.Contains("DarlingPgCpuUtilizationReader.Freshness", code, StringComparison.Ordinal);
            Assert.DoesNotContain("TimeSpan.FromMinutes(15)", code, StringComparison.Ordinal);
        }
    }
}
