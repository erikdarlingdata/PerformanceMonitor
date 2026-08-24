/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.RegularExpressions;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #2564: the plan-capture readiness collector. The assertions are about the DISTINCTIONS it has to draw,
/// not its wording — a test that pinned the sentences would fail on every improvement to them and say
/// nothing about the only property that matters, which is that a reader can tell which remedy applies.
/// </summary>
public class PgPlanCaptureReadinessCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static CollectorContext MakeContext(int major = 16)
        => new()
        {
            ServerId = 42,
            ServerName = "pg-target",
            CollectionTime = new DateTime(2026, 8, 24, 12, 0, 0, DateTimeKind.Utc),
            Deltas = s_deltas,
            Target = new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql,
                PostgresMajorVersion = major,
            },
            ExcludedDatabases = Array.Empty<string>(),
        };

    /// <summary>
    /// The store table must not collide with a pg_catalog object: pg_catalog is searched first, so a
    /// colliding name breaks CREATE INDEX with 42809 and makes unqualified reads resolve to the MONITORING
    /// store's own copy.
    /// </summary>
    [Fact]
    public void NameAndTargetTable_AreTheContract_AndShadowNoCatalogObject()
    {
        Assert.Equal("pg_plan_capture_readiness", PgPlanCaptureReadinessCollector.Instance.Name);
        Assert.Equal("pg_plan_capture_readiness", PgPlanCaptureReadinessCollector.Instance.TargetTable);
        Assert.Equal(CollectorTargetEngine.PostgreSql, PgPlanCaptureReadinessCollector.Instance.TargetEngine);
        Assert.NotEqual("pg_settings", PgPlanCaptureReadinessCollector.Instance.TargetTable);
        Assert.NotEqual("pg_available_extensions", PgPlanCaptureReadinessCollector.Instance.TargetTable);
    }

    /// <summary>
    /// Every PostgreSQL target including standbys. A replica can carry a different parameter group from its
    /// writer, and gating this to writers would hide exactly that divergence.
    /// </summary>
    [Theory]
    [InlineData(13)]
    [InlineData(16)]
    [InlineData(17)]
    public void AppliesTo_EveryPostgresTarget(int major)
    {
        var target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = major };
        Assert.True(PgPlanCaptureReadinessCollector.Instance.AppliesTo(target));
        Assert.True(CollectorCatalog.AppliesTo(PgPlanCaptureReadinessCollector.Instance, target));
    }

    /// <summary>And never at a SQL Server target — the composed gate stops the dialect being sent at all.</summary>
    [Fact]
    public void AppliesTo_NeverASqlServerTarget()
        => Assert.False(CollectorCatalog.AppliesTo(
            PgPlanCaptureReadinessCollector.Instance,
            new CollectorTargetInfo { Engine = CollectorTargetEngine.SqlServer, SqlMajorVersion = 16 }));

    /// <summary>
    /// Every <c>current_setting</c> must use the two-argument MISSING_OK form. Reading a GUC that does not
    /// exist because the library was never loaded is the NORMAL case for this collector, and the
    /// one-argument form raises 42704 — which would turn its entire purpose into an error every cycle.
    /// </summary>
    [Fact]
    public void EveryCurrentSetting_UsesTheMissingOkForm()
    {
        var sql = PgPlanCaptureReadinessCollector.Instance.BuildQuery(MakeContext()).Text;

        var calls = Regex.Matches(sql, @"current_setting\(([^)]*)\)");
        Assert.NotEmpty(calls);
        foreach (Match call in calls)
        {
            Assert.Contains(", true", call.Groups[1].Value, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The GUC value is never cast to a number. <c>current_setting</c> renders some settings WITH their
    /// unit, so a cast is how a collector starts failing on one major version and not another — the reason
    /// <c>observed</c> is text and interpretation happens downstream.
    /// </summary>
    [Fact]
    public void TheObservedValue_IsNeverCastToANumber()
    {
        var sql = PgPlanCaptureReadinessCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.DoesNotMatch(new Regex(@"current_setting\([^)]*\)\s*::\s*(int|integer|bigint|numeric|double)"), sql);
        Assert.DoesNotMatch(new Regex(@"pg_size_bytes\s*\(\s*current_setting"), sql);
    }

    /// <summary>
    /// The four facets are separate rows, and that IS the feature. Collapsing them would produce the one
    /// thing this collector exists to prevent: a single "plans unavailable" that tells nobody what to do,
    /// when the remedies are a parameter-group change plus a reboot, a single setting, and a platform
    /// limitation respectively.
    /// </summary>
    [Fact]
    public void TheFourFacets_AreSeparateRows()
    {
        var sql = PgPlanCaptureReadinessCollector.Instance.BuildQuery(MakeContext()).Text;

        foreach (var facet in new[] { "library_loaded", "capture_threshold", "extension_available", "plan_text_setting" })
        {
            Assert.Contains($"'{facet}'::text", sql, StringComparison.Ordinal);
        }

        /* Four SELECTs joined by three UNION ALLs — one row per facet, not a wide row. */
        Assert.Equal(3, Regex.Matches(sql, @"\bUNION ALL\b").Count);
    }

    /// <summary>
    /// The trap the issue was filed about: <c>log_min_duration = -1</c> is loaded-and-capturing-nothing,
    /// which from outside looks identical to not-loaded but has a completely different remedy. The query
    /// must treat it as UNSATISFIED and must distinguish it from the setting being absent entirely.
    /// </summary>
    [Fact]
    public void ACaptureThresholdOfMinusOne_IsNotSatisfied_AndIsDistinctFromAbsent()
    {
        var sql = PgPlanCaptureReadinessCollector.Instance.BuildQuery(MakeContext()).Text;

        /* -1 fails the satisfied test rather than passing as "a value is set". */
        Assert.Contains("<> '-1'", sql, StringComparison.Ordinal);

        /* And an absent GUC reports as absent rather than being folded into the -1 case, so the reader can
           tell "loaded but switched off" from "never loaded". */
        Assert.Contains("(setting absent - library not loaded)", sql, StringComparison.Ordinal);
        Assert.Contains("IS NOT NULL", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// It must not claim plans ARE captured. Library loaded, capture configured, and plans being readable by
    /// us are three separate facts (#2566/#2567 own the last one), and conflating them is how a readiness
    /// probe starts lying about a capability.
    /// </summary>
    [Fact]
    public void ItReportsReadiness_NeverThatPlansAreBeingCaptured()
    {
        var columns = PgPlanCaptureReadinessCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();

        Assert.Contains("is_satisfied", columns);
        Assert.DoesNotContain("plans_captured", columns);
        Assert.DoesNotContain("plan_count", columns);
        Assert.DoesNotContain("plan_xml", columns);
    }

    /// <summary>
    /// The remedy travels with the observation. A read that reconstructed it later would drift from what the
    /// collector actually saw, and the remedy is specific to both the facet and the platform.
    /// </summary>
    [Fact]
    public void EachFacet_CarriesItsOwnRemedy()
    {
        var columns = PgPlanCaptureReadinessCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();
        Assert.Contains("detail", columns);
        Assert.Contains("observed", columns);

        var sql = PgPlanCaptureReadinessCollector.Instance.BuildQuery(MakeContext()).Text;

        /* The Aurora/RDS instruction specifically — it is a CLUSTER parameter group and a writer reboot, not
           a SET, and that misconception is the one worth heading off. */
        Assert.Contains("CLUSTER", sql, StringComparison.Ordinal);
        Assert.Contains("REBOOT", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The remedy must depend on what was OBSERVED, not be one fixed sentence per facet. Measured against a
    /// live PostgreSQL 17: with the threshold at 0 the first draft still said "log_min_duration = -1 means
    /// ... capturing NOTHING" beside <c>is_satisfied = true</c>, so the row contradicted itself. Three of the
    /// four states that facet reaches were being given the wrong advice.
    /// </summary>
    [Fact]
    public void TheCaptureThresholdRemedy_DependsOnWhatWasObserved()
    {
        var sql = PgPlanCaptureReadinessCollector.Instance.BuildQuery(MakeContext()).Text;

        /* A CASE over the observed value, not a literal. */
        Assert.Contains("CASE", sql, StringComparison.Ordinal);
        Assert.Contains("WHEN current_setting('auto_explain.log_min_duration', true) IS NULL", sql, StringComparison.Ordinal);
        Assert.Contains("WHEN current_setting('auto_explain.log_min_duration', true) = '-1'", sql, StringComparison.Ordinal);

        /* And the satisfied arm must not repeat the -1 sentence, which is the exact contradiction found. */
        var elseArm = sql[sql.IndexOf("ELSE 'auto_explain is loaded and capturing", StringComparison.Ordinal)..];
        Assert.DoesNotContain("capturing NOTHING", elseArm[..200], StringComparison.Ordinal);
    }

    /// <summary>
    /// <c>observed</c> is stored as the server's own rendering, and this is why: measured on PostgreSQL 17,
    /// <c>auto_explain.log_min_duration = 250</c> comes back as <b><c>250ms</c></b> — the GUC carries its
    /// unit. A numeric column or a cast would raise invalid-input-syntax and fail the whole collection, which
    /// is the <c>track_activity_query_size</c> defect this codebase has already paid for once.
    /// </summary>
    [Fact]
    public void TheObservedColumn_IsText_BecauseGucsRenderWithTheirUnit()
    {
        var observed = PgPlanCaptureReadinessCollector.Instance.PayloadColumns.Single(c => c.Name == "observed");
        Assert.Equal(CollectorColumnType.Varchar, observed.Type);
    }

    /// <summary>
    /// The library check is boundary-aware, not a substring. <c>shared_preload_libraries</c> is a
    /// comma-separated list, and <c>LIKE '%auto_explain%'</c> reports true for any library whose name merely
    /// CONTAINS it — measured against PostgreSQL 17, both <c>my_auto_explain_shim</c> and
    /// <c>auto_explain_extra</c> false-positived under the substring form and are correctly rejected under
    /// the boundary form. This facet's whole job is to be trustworthy about loaded-versus-not.
    /// </summary>
    [Fact]
    public void TheLibraryCheck_IsBoundaryAware_NotASubstringMatch()
    {
        var sql = PgPlanCaptureReadinessCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.DoesNotContain("LIKE '%auto_explain%'", sql, StringComparison.Ordinal);
        Assert.Contains(@"~ '(^|,)\s*auto_explain\s*(,|$)'", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Catalog reads are schema-qualified, matching every other PostgreSQL collector here. pg_catalog is
    /// searched implicitly but not necessarily FIRST, so an unqualified read can resolve to an object a user
    /// created in a schema earlier in the monitoring login's search_path — which for this collector would
    /// mean fabricating an answer about whether plan capture is possible at all.
    /// </summary>
    [Fact]
    public void CatalogReads_AreSchemaQualified()
    {
        var sql = PgPlanCaptureReadinessCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.DoesNotMatch(new Regex(@"FROM\s+pg_available_extensions"), sql);
        Assert.Contains("pg_catalog.pg_available_extensions", sql, StringComparison.Ordinal);
    }

    /// <summary>Every output column is aliased — an unaliased expression comes back named after the
    /// function, which makes the query undebuggable in psql, the one tool anyone reaches for.</summary>
    [Fact]
    public void EveryOutputColumn_IsAliased()
    {
        var sql = PgPlanCaptureReadinessCollector.Instance.BuildQuery(MakeContext()).Text;
        var payload = PgPlanCaptureReadinessCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();

        /* The first SELECT carries the aliases; the UNION ALL branches inherit them by position, which is
           how PostgreSQL names a union's columns. */
        foreach (var name in payload)
        {
            Assert.Contains($"AS {name}", sql, StringComparison.Ordinal);
        }
    }
}
