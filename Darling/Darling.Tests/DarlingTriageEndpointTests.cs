/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The per-alert triage endpoint (#2710). The load-bearing pin is the map↔dispatch one: every read the
/// triage sections name must actually be served by <see cref="DarlingWebEndpoints.BuildReadDispatch"/> —
/// the #2213 lesson applied here (proven reads wired to a name that doesn't dispatch would render every
/// section as an error while everything compiles). The rest pin the pure glue: the firing-instant anchor
/// resolution and the synthetic query string the sections bind through.
/// </summary>
public sealed class DarlingTriageEndpointTests
{
    private static IEnumerable<DarlingTriageEndpoint.TriageSection> AllMappedSections()
    {
        foreach (var sections in DarlingTriageEndpoint.SectionsByMetric.Values)
        {
            foreach (var section in sections)
            {
                yield return section;
            }
        }

        foreach (var section in DarlingTriageEndpoint.DefaultSections)
        {
            yield return section;
        }

        yield return DarlingTriageEndpoint.CollectionLogSection;
    }

    [Fact]
    public void EveryMappedTriageRead_IsServedByTheReadDispatch()
    {
        var dispatch = DarlingWebEndpoints.BuildReadDispatch();
        var unserved = AllMappedSections()
            .Select(s => s.Read)
            .Distinct(StringComparer.Ordinal)
            .Where(read => !dispatch.ContainsKey(read))
            .OrderBy(read => read, StringComparer.Ordinal)
            .ToArray();

        Assert.True(unserved.Length == 0,
            "triage sections name reads the /api/read dispatch does not serve: " + string.Join(", ", unserved));
    }

    [Fact]
    public void EveryMetricEntry_HasAtLeastOneSection_AndNonBlankTitles()
    {
        foreach (var (metric, sections) in DarlingTriageEndpoint.SectionsByMetric)
        {
            Assert.False(string.IsNullOrWhiteSpace(metric));
            Assert.NotEmpty(sections);
            Assert.All(sections, s => Assert.False(string.IsNullOrWhiteSpace(s.Title)));
        }
    }

    /// <summary>The engine's own metric-name constants must resolve to a REAL mapping, not the thin default —
    /// the two names the engine exports as constants are the cheapest drift alarm available: a renamed metric
    /// otherwise silently downgrades its triage page to the fallback with no failing test anywhere.</summary>
    [Fact]
    public void EngineMetricNameConstants_ResolveToTheirOwnMapping_NotTheFallback()
    {
        Assert.NotSame(DarlingTriageEndpoint.DefaultSections, DarlingTriageEndpoint.SectionsFor(AlertEngine.BlockingWatermarkMetric));
        Assert.NotSame(DarlingTriageEndpoint.DefaultSections, DarlingTriageEndpoint.SectionsFor(AlertEngine.DeadlockWatermarkMetric));
        /* #2711: the PG evaluator fires the SAME "Poison Wait" name, exported as its own constant. */
        Assert.NotSame(DarlingTriageEndpoint.DefaultSections, DarlingTriageEndpoint.SectionsFor(PostgresAlertEvaluator.PoisonWaitMetric));
    }

    [Fact]
    public void SectionsFor_UnknownOrBlankMetric_FallsBackToDefaults()
    {
        Assert.Same(DarlingTriageEndpoint.DefaultSections, DarlingTriageEndpoint.SectionsFor("Some Future Metric"));
        Assert.Same(DarlingTriageEndpoint.DefaultSections, DarlingTriageEndpoint.SectionsFor(null));
        Assert.Same(DarlingTriageEndpoint.DefaultSections, DarlingTriageEndpoint.SectionsFor("  "));
    }

    [Fact]
    public void SectionsFor_MatchesCaseInsensitively_LikeTheHistoryFilter()
    {
        Assert.NotSame(DarlingTriageEndpoint.DefaultSections, DarlingTriageEndpoint.SectionsFor("high cpu"));
    }

    /// <summary>Review catch: resolution rows record <c>resolution.Title</c> — not the firing metric — into
    /// <c>metric_name</c>, and the Alert History Triage link makes every one of them an entry point. Each
    /// alias must land on the SAME section list as its firing metric, so a "CPU Resolved" click-through
    /// carries the CPU drill-down that confirms the recovery instead of the thin fallback.</summary>
    [Fact]
    public void EveryResolutionAlias_SharesItsFiringMetricsSections()
    {
        Assert.NotEmpty(DarlingTriageEndpoint.ResolutionAliases);
        foreach (var (alias, canonical) in DarlingTriageEndpoint.ResolutionAliases)
        {
            Assert.True(DarlingTriageEndpoint.SectionsByMetric.ContainsKey(canonical),
                $"resolution alias '{alias}' names a canonical metric '{canonical}' with no mapping");
            Assert.Same(DarlingTriageEndpoint.SectionsFor(canonical), DarlingTriageEndpoint.SectionsFor(alias));
            Assert.NotSame(DarlingTriageEndpoint.DefaultSections, DarlingTriageEndpoint.SectionsFor(alias));
        }
    }

    /* ---------------- ResolveAnchor (the firing-instant → as_of glue) ---------------- */

    [Fact]
    public void ResolveAnchor_MissingOrGarbage_AnchorsAtNow_WithNoAsOf()
    {
        var now = new DateTime(2026, 8, 31, 12, 0, 0, DateTimeKind.Utc);

        var (anchor, asOf) = DarlingTriageEndpoint.ResolveAnchor(null, now);
        Assert.Equal(now, anchor);
        Assert.Null(asOf);

        (anchor, asOf) = DarlingTriageEndpoint.ResolveAnchor("not-a-time", now);
        Assert.Equal(now, anchor);
        Assert.Null(asOf);
    }

    [Fact]
    public void ResolveAnchor_PastInstant_AnchorsReadsShortlyAfterTheFiring()
    {
        var now = new DateTime(2026, 8, 31, 12, 0, 0, DateTimeKind.Utc);

        var (anchor, asOf) = DarlingTriageEndpoint.ResolveAnchor("2026-08-31T08:00:00Z", now);

        Assert.Equal(new DateTime(2026, 8, 31, 8, 0, 0, DateTimeKind.Utc), anchor);
        /* The window END sits the slack past the firing, so the firing and its aftermath are in-window. */
        Assert.Equal("2026-08-31T08:15:00Z", asOf);
    }

    [Fact]
    public void ResolveAnchor_RecentOrFutureInstant_OmitsAsOf_SoTheToolsEndAtNow()
    {
        var now = new DateTime(2026, 8, 31, 12, 0, 0, DateTimeKind.Utc);

        /* Five minutes ago: at + slack lands within a minute of now — send no anchor. */
        var (_, asOfRecent) = DarlingTriageEndpoint.ResolveAnchor("2026-08-31T11:55:00Z", now);
        Assert.Null(asOfRecent);

        /* A future instant (clock skew on the link builder): clamped, never a future-anchor refusal. */
        var (anchorFuture, asOfFuture) = DarlingTriageEndpoint.ResolveAnchor("2026-08-31T12:30:00Z", now);
        Assert.Equal(now, anchorFuture);
        Assert.Null(asOfFuture);
    }

    /* ---------------- BuildSectionQuery (the synthetic binding string) ---------------- */

    private static DarlingTriageEndpoint.TriageSection Section(params (string Key, string Value)[] parameters)
    {
        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (key, value) in parameters)
        {
            map[key] = value;
        }

        return new DarlingTriageEndpoint.TriageSection("T", "get_blocking", map);
    }

    [Fact]
    public void BuildSectionQuery_InjectsServerAndAsOf_BesideTheFixedParams()
    {
        var query = DarlingTriageEndpoint.BuildSectionQuery(
            Section(("hours", "4"), ("limit", "20")), "SRV-01", "2026-08-31T08:15:00Z");

        Assert.Equal("?server=SRV-01&as_of=2026-08-31T08%3A15%3A00Z&hours=4&limit=20", query);
    }

    [Fact]
    public void BuildSectionQuery_OmitsAbsentServerAndAsOf_SoToolDefaultsApply()
    {
        Assert.Equal("?hours=4", DarlingTriageEndpoint.BuildSectionQuery(Section(("hours", "4")), null, null));
        Assert.Equal("?", DarlingTriageEndpoint.BuildSectionQuery(Section(), null, null));
    }

    [Fact]
    public void BuildSectionQuery_EscapesAServerName_ThatCarriesQueryMetacharacters()
    {
        var query = DarlingTriageEndpoint.BuildSectionQuery(Section(), "srv&x=1 y", null);
        Assert.Equal("?server=srv%26x%3D1%20y", query);
    }

    /* ---------------- #2768: the fleet-level store self-alert family ---------------- */

    /// <summary>The whole bug in one assertion: the label the store self-alerts fire under has to be
    /// RECOGNISED, or the page runs per-server reads against a server that cannot resolve and renders the
    /// three resolver errors the reporter saw. Keyed on the engine's own constant, so a rename on either
    /// side fails here rather than silently regressing the page.</summary>
    [Fact]
    public void IsFleetLevelStoreServer_RecognisesTheEnginesStoreLabel_AndNothingElse()
    {
        Assert.True(DarlingTriageEndpoint.IsFleetLevelStoreServer(DarlingSelfAlertEvaluator.StoreServerLabel));
        /* Trimmed and case-insensitive: the label makes a round trip through a URL before it gets here. */
        Assert.True(DarlingTriageEndpoint.IsFleetLevelStoreServer("  monitor store  "));

        Assert.False(DarlingTriageEndpoint.IsFleetLevelStoreServer("RMVNSQL01\\INST1"));
        Assert.False(DarlingTriageEndpoint.IsFleetLevelStoreServer(null));
        Assert.False(DarlingTriageEndpoint.IsFleetLevelStoreServer("   "));
        /* A real server that merely CONTAINS the label must not be swallowed by the fleet-level path. */
        Assert.False(DarlingTriageEndpoint.IsFleetLevelStoreServer("Monitor Store Replica"));
    }

    /// <summary>Every store self-alert must land on its own mapping, not the per-server fallback — the
    /// fallback is exactly what produced "Could not resolve server" three times per page. The engine exports
    /// the cadence metric as a constant, so that one is pinned by symbol; the rest are pinned by the literal
    /// the engine fires, which is the string the history row and any mute rule key on.</summary>
    [Theory]
    [InlineData("Store Disk Pressure")]
    [InlineData("Store Runtime Upgrade")]
    [InlineData("Compression Job Stuck")]
    [InlineData("Store Disk Pressure Resolved")]
    [InlineData("Store Job Cadence Recovered")]
    [InlineData("Compression Job Recovered")]
    public void EveryStoreSelfAlert_HasItsOwnSections_NotThePerServerFallback(string metric)
    {
        Assert.NotSame(DarlingTriageEndpoint.DefaultSections, DarlingTriageEndpoint.SectionsFor(metric));
    }

    /// <summary>The [Theory] above pins the WIRE strings an operator's history row actually carries. This
    /// pins the same mappings by SYMBOL, so renaming a metric on the engine breaks here instead of silently
    /// downgrading that alert's triage page to the per-server fallback - the drift alarm #2710 established
    /// for the engine's other exported metric-name constants.</summary>
    [Fact]
    public void StoreMetricNameConstants_ResolveToTheirOwnMapping_NotTheFallback()
    {
        foreach (var metric in new[]
                 {
                     DarlingSelfAlertEvaluator.DiskPressureMetric,
                     DarlingSelfAlertEvaluator.DiskPressureResolvedMetric,
                     DarlingSelfAlertEvaluator.StoreUpgradeMetric,
                     DarlingSelfAlertEvaluator.CompressionJobMetric,
                     DarlingSelfAlertEvaluator.JobCadenceMetric,
                 })
        {
            Assert.NotSame(DarlingTriageEndpoint.DefaultSections, DarlingTriageEndpoint.SectionsFor(metric));
        }
    }

    /// <summary>The store family's sections must ALL be fleet-level. A per-server section reached through a
    /// store alert would bind the synthetic label as <c>server</c> and reproduce the original defect for that
    /// one card, which is the regression this pin exists to catch.</summary>
    [Fact]
    public void StoreSelfAlertSections_AreAllFleetLevel_SoNoneBindsTheSyntheticServer()
    {
        foreach (var metric in new[]
                 {
                     DarlingSelfAlertEvaluator.DiskPressureMetric,
                     DarlingSelfAlertEvaluator.StoreUpgradeMetric,
                     DarlingSelfAlertEvaluator.CompressionJobMetric,
                     DarlingSelfAlertEvaluator.JobCadenceMetric,
                 })
        {
            var sections = DarlingTriageEndpoint.SectionsFor(metric);
            Assert.NotEmpty(sections);
            Assert.All(sections, s => Assert.True(s.FleetLevel,
                $"'{metric}' section '{s.Title}' ({s.Read}) is not fleet-level and would bind the synthetic store server."));
        }
    }

    /// <summary>A fleet-level section drops the server even when one is supplied — the reads it names take no
    /// <c>server</c>, and on a store alert the only value available is the unresolvable label. <c>as_of</c>
    /// and the fixed params still bind, so the anchoring behaviour is unchanged.</summary>
    [Fact]
    public void BuildSectionQuery_FleetLevelSection_OmitsTheServer_ButKeepsAsOfAndFixedParams()
    {
        var fleet = new DarlingTriageEndpoint.TriageSection(
            "Store size", "get_store_metrics",
            new Dictionary<string, string>(StringComparer.Ordinal) { ["days_back"] = "30" },
            FleetLevel: true);

        Assert.Equal(
            "?as_of=2026-08-31T08%3A15%3A00Z&days_back=30",
            DarlingTriageEndpoint.BuildSectionQuery(fleet, DarlingSelfAlertEvaluator.StoreServerLabel, "2026-08-31T08:15:00Z"));

        /* And the per-server default is untouched — the same section shape WITH FleetLevel off still binds it. */
        var perServer = new DarlingTriageEndpoint.TriageSection(
            "Store size", "get_store_metrics",
            new Dictionary<string, string>(StringComparer.Ordinal) { ["days_back"] = "30" });

        Assert.Equal(
            "?server=SRV-01&days_back=30",
            DarlingTriageEndpoint.BuildSectionQuery(perServer, "SRV-01", null));
    }

    /// <summary>The per-server families must NOT have been swept into the fleet-level path — the reporter
    /// confirmed deadlocks/blocking/High CPU render fully today, and they have to keep doing so.</summary>
    [Fact]
    public void PerServerSections_StayPerServer_SoTheyStillBindTheirServer()
    {
        foreach (var metric in new[] { "High CPU", "Deadlocks Detected", "Blocking Detected", "Volume Free Space" })
        {
            Assert.All(DarlingTriageEndpoint.SectionsFor(metric), s => Assert.False(s.FleetLevel,
                $"'{metric}' section '{s.Title}' ({s.Read}) became fleet-level and would stop binding its server."));
        }

        Assert.All(DarlingTriageEndpoint.DefaultSections, s => Assert.False(s.FleetLevel));
        Assert.False(DarlingTriageEndpoint.CollectionLogSection.FleetLevel);
    }
}
