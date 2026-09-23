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
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The per-KEY baseline seam (#3691 lane 33): <c>PgBaselineProvider.GetBaselineAsync</c> gains a five-argument
/// overload keying a series on (server, metric, key), resolved through the third seam
/// <c>ResolveKeyedBaselineQuery</c> and bound as <c>$7</c> after the Q6 clock parameters; the PostgreSQL provider
/// declares two keyed arms for the statement family, consumed by lane 34's share detector and lane 39's keyed plan
/// regression. Since #3901 a keyed compute is a member SET (<c>GetBaselinesAsync</c>; the five-argument overload is a
/// set of one): <c>$7</c> is a <c>text[]</c>, each arm reads its table once for the set and runs the one scaffold per
/// member. What is pinned here is the CONTRACT: the unkeyed overload byte-identical, two keys two caches, the seventh
/// bind only on a keyed compute, the SQL Server store declaring no keyed metric, the two arms' shapes, and the
/// cardinality note's arithmetic. The live class below drives the real provider over planted
/// <c>pg_statement_stats</c> for two statements with different shares and reads two distinct buckets;
/// <c>PgStatementKeyedSetLiveTests</c> holds the set to the per-statement arms it replaced.
/// </summary>
public sealed class PgBaselineProviderKeyedTests
{
    private static string ProviderSource => RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgBaselineProvider.cs");

    /* ───────────────────────── (1) the overload and the cache identity ───────────────────────── */

    /// <summary>
    /// The public lookup is exactly two overloads, and the four-argument one is a delegation with <c>key: null</c> —
    /// not a second body that could drift. Every pre-existing caller (36 sites across the detectors) resolves to it
    /// unchanged, because <c>DateTime</c> and <c>string</c> in the third position cannot be confused.
    /// </summary>
    [Fact]
    public void TheUnkeyedOverload_DelegatesWithANullKey_AndTheSurfaceIsExactlyTwoOverloads()
    {
        var overloads = typeof(PgBaselineProvider).GetMethods(BindingFlags.Public | BindingFlags.Instance)
            .Where(m => m.Name == "GetBaselineAsync")
            .OrderBy(m => m.GetParameters().Length)
            .ToList();
        Assert.Equal(2, overloads.Count);
        Assert.Equal(new[] { typeof(int), typeof(string), typeof(DateTime), typeof(CancellationToken) }, overloads[0].GetParameters().Select(p => p.ParameterType).ToArray());
        Assert.Equal(new[] { typeof(int), typeof(string), typeof(string), typeof(DateTime), typeof(CancellationToken) }, overloads[1].GetParameters().Select(p => p.ParameterType).ToArray());
        Assert.Equal("key", overloads[1].GetParameters()[2].Name);
        Assert.Same(typeof(Task<BaselineBucket>), overloads[0].ReturnType);
        Assert.Same(typeof(Task<BaselineBucket>), overloads[1].ReturnType);

        var code = CSharpSourceWalker.StripCommentsAndStrings(ProviderSource);
        Assert.Contains("=> GetBaselineAsync(serverId, metricName, key: null, analysisTime, cancellationToken);", code, StringComparison.Ordinal);
        /* The lookup body exists once: the keyed overload's. */
        Assert.Single(Regex.Matches(code, Regex.Escape("cached.Clock.LocalKey(analysisTime)")));
    }

    [Fact]
    public void CacheKeyFor_KeepsTheUnkeyedIdentityByteIdentical_AndAppendsTheKeyAsAThirdSegment()
    {
        Assert.Equal("7:pg_tps", PgBaselineProvider.CacheKeyFor(7, MetricNames.PgTps, null));
        Assert.Equal("7:pg_statement_share:9001", PgBaselineProvider.CacheKeyFor(7, MetricNames.PgStatementShare, "9001"));
        Assert.Equal("7:pg_statement_share:9002", PgBaselineProvider.CacheKeyFor(7, MetricNames.PgStatementShare, "9002"));
        /* "" is a key, not the absence of one — "no key" is spelled null and nothing else. */
        Assert.Equal("7:pg_statement_share:", PgBaselineProvider.CacheKeyFor(7, MetricNames.PgStatementShare, ""));

        /* Two keys, two entries; keyed and unkeyed of one metric, two entries; and InvalidateCache's server-prefix
           sweep finds every one of them. */
        var identities = new[]
        {
            PgBaselineProvider.CacheKeyFor(7, MetricNames.PgStatementMeanMs, null),
            PgBaselineProvider.CacheKeyFor(7, MetricNames.PgStatementMeanMs, "9001"),
            PgBaselineProvider.CacheKeyFor(7, MetricNames.PgStatementMeanMs, "9002"),
        };
        Assert.Equal(3, identities.Distinct(StringComparer.Ordinal).Count());
        Assert.All(identities, id => Assert.StartsWith("7:", id, StringComparison.Ordinal));
        Assert.DoesNotContain(identities, id => id.StartsWith("70:", StringComparison.Ordinal));

        var code = CSharpSourceWalker.StripCommentsAndStrings(ProviderSource);
        Assert.Contains("var cacheKey = CacheKeyFor(serverId, metricName, key);", code, StringComparison.Ordinal);
        Assert.Contains("var cacheKey = CacheKeyFor(serverId, metricName, key: null);", code, StringComparison.Ordinal);
        /* #3901: a set's misses are cached one entry per key, under the same identity a single-key compute uses —
           through Store since #3941, which files the entry under that key in this provider's cache (and, on success,
           the process's shared tier under the same key). */
        Assert.Contains("Store(serverId, CacheKeyFor(serverId, metricName, key), entry);", code, StringComparison.Ordinal);
        Assert.Contains("_cache[cacheKey] = entry;", code[code.IndexOf("private void Store(", StringComparison.Ordinal)..], StringComparison.Ordinal);
    }

    /* ───────────────────────── (2) the seam and the bind ───────────────────────── */

    /// <summary>
    /// The keyed seam is a protected virtual beside the other two, defaults to null, and a keyed compute resolves
    /// through it ALONE — never falling back to the unkeyed arm, which would serve the population's buckets under a
    /// member's key. The SQL Server store declares no keyed metric: for every declared name the base answers null.
    /// </summary>
    [Fact]
    public void TheKeyedSeam_IsProtectedVirtual_DefaultsToNullForEveryMetric_AndAKeyedComputeResolvesThroughItAlone()
    {
        var seam = typeof(PgBaselineProvider).GetMethod("ResolveKeyedBaselineQuery", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(seam);
        Assert.True(seam!.IsVirtual && seam.IsFamily, "ResolveKeyedBaselineQuery must be protected virtual — the PostgreSQL provider overrides it");
        Assert.Same(typeof(string), Nullable.GetUnderlyingType(seam.ReturnType) ?? seam.ReturnType);

        using var neverOpened = NpgsqlDataSource.Create("Host=localhost;Database=never-opened");
        var sqlServer = new ExposingProvider(neverOpened);
        foreach (var metric in AllDeclaredMetricNames())
        {
            Assert.Null(sqlServer.KeyedQueryFor(metric));
        }
        Assert.Null(sqlServer.KeyedQueryFor(MetricNames.PgStatementShare));
        Assert.Null(PgBaselineProvider.GetBaselineQuery(MetricNames.PgStatementShare));

        var code = CSharpSourceWalker.StripCommentsAndStrings(ProviderSource);
        Assert.Contains("var query = keys is null ? ResolveBaselineQuery(metricName) : ResolveKeyedBaselineQuery(metricName);", code, StringComparison.Ordinal);
    }

    /// <summary>
    /// The seventh parameter is bound after the sixth, only when keys were passed, and exactly once — so an unkeyed
    /// compute binds precisely what it bound before the seam (the SQL Server pass is byte-identical at the wire),
    /// and a keyed compute binds its keys as the <c>text[]</c> the arms cast (#3901: the whole member set in one
    /// statement). The successor/legacy supply swap is skipped on a keyed compute: it compares against the unkeyed
    /// successor text and could never match.
    /// </summary>
    [Fact]
    public void TheBind_AddsTheSeventhParameterOnlyOnAKeyedCompute_AfterTheThreeClockParameters()
    {
        const string seventhBind = "cmd.Parameters.AddWithValue(keys.ToArray());";
        var code = CSharpSourceWalker.StripCommentsAndStrings(ProviderSource);
        var sixth = code.IndexOf("cmd.Parameters.AddWithValue(clock.OffsetAfterMinutes);", StringComparison.Ordinal);
        var seventh = code.IndexOf(seventhBind, StringComparison.Ordinal);
        Assert.True(sixth > 0 && seventh > sixth, "the keys must be bound AFTER $6");
        Assert.Single(Regex.Matches(code, Regex.Escape(seventhBind)));
        /* The per-key bind is gone: a scalar $7 would be one statement per member again. */
        Assert.DoesNotContain("cmd.Parameters.AddWithValue(key);", code, StringComparison.Ordinal);

        var between = code[(sixth + "cmd.Parameters.AddWithValue(clock.OffsetAfterMinutes);".Length)..seventh];
        Assert.Contains("if (keys is not null)", between, StringComparison.Ordinal);
        Assert.DoesNotContain("AddWithValue", between, StringComparison.Ordinal);

        Assert.Contains("if (keys is null)", code, StringComparison.Ordinal);
        var swap = code.IndexOf("query = await ChooseSupplyAsync(", StringComparison.Ordinal);
        var guard = code.LastIndexOf("if (keys is null)", swap, StringComparison.Ordinal);
        Assert.True(guard > 0 && swap - guard < 120, "ChooseSupplyAsync must sit under the keys-is-null guard");

        /* The rows come back filed by member, read by NAME so the eight robust ordinals stay the reader's contract. */
        Assert.Contains("var memberOrdinal = keys is null ? -1 : reader.GetOrdinal(\"member\");", ProviderSource, StringComparison.Ordinal);

        /* One classified catch still — the keyed compute degrades through the same one (AnalysisShutdownResidueTests
           pins the count too; stated here so the seam's own file says it). */
        Assert.Single(Regex.Matches(ProviderSource, Regex.Escape("when (!AnalysisShutdown.IsExpectedAbandon(ex, cancellationToken))")));
    }

    /* ───────────────────────── (3) the two PostgreSQL keyed arms ───────────────────────── */

    [Fact]
    public void ThePostgresProvider_DeclaresExactlyTheTwoStatementArms_AndTheNameIsPgPrefixed()
    {
        Assert.Equal("pg_statement_share", MetricNames.PgStatementShare);

        var keyed = AllDeclaredMetricNames().Where(m => PgTargetBaselineProvider.GetPgTargetKeyedBaselineQuery(m) is not null).OrderBy(m => m, StringComparer.Ordinal).ToList();
        Assert.Equal(new[] { MetricNames.PgStatementMeanMs, MetricNames.PgStatementShare }, keyed);

        /* The share has NO unkeyed arm (the server's share of itself is 1.0); the mean keeps lane 27's server-wide
           unkeyed arm beside the keyed one, so lane 27's detector can switch without a new name. */
        Assert.Null(PgTargetBaselineProvider.GetPgTargetBaselineQuery(MetricNames.PgStatementShare));
        Assert.NotNull(PgTargetBaselineProvider.GetPgTargetBaselineQuery(MetricNames.PgStatementMeanMs));
        Assert.DoesNotContain("$7", PgTargetBaselineProvider.GetPgTargetBaselineQuery(MetricNames.PgStatementMeanMs)!, StringComparison.Ordinal);

        var provider = new PgTargetBaselineProvider(NpgsqlDataSource.Create("Host=localhost;Database=never-opened"));
        Assert.IsAssignableFrom<PgBaselineProvider>(provider);
        var overridden = typeof(PgTargetBaselineProvider).GetMethod("ResolveKeyedBaselineQuery", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.DeclaredOnly);
        Assert.NotNull(overridden);
        var root = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetBaselineProvider.cs"));
        Assert.Contains("protected override string? ResolveKeyedBaselineQuery(string metricName) => GetPgTargetKeyedBaselineQuery(metricName);", root, StringComparison.Ordinal);
        Assert.Contains("MetricNames.PgStatementShare => StatementShareKeyedBaselineQuery(),", root, StringComparison.Ordinal);
        Assert.Contains("MetricNames.PgStatementMeanMs => StatementMeanMsKeyedBaselineQuery(),", root, StringComparison.Ordinal);
    }

    /// <summary>
    /// Both arms: the unkeyed contract (clean CTE, the one scaffold, half-open window on <c>$1..$3</c>, stored deltas,
    /// no bare clock) plus the member SET (#3901): <c>$7</c> unnested with its positions, the table read ONCE for the
    /// whole set by the per-statement arm's own <c>GROUP BY collection_time</c> with one <c>FILTER</c> per member slot,
    /// and each member's <c>clean</c> handed to the one scaffold through <c>PerMemberScaffold</c> — the read that fails
    /// here on the old per-statement shape (a scalar <c>$7::BIGINT</c> and one statement per member) and on the
    /// grouped-by-statement set shape measured slower at scale. The share's numerator is its own rows' sum beside the
    /// collection's total, and a collection this statement sat out is a ZERO sample; the mean's idle collection is no
    /// sample at all — the two rules the arm docs give.
    /// </summary>
    [Fact]
    public void TheTwoKeyedArms_CarryTheUnkeyedContractPlusTheMemberSet_AndReadTheirTableOnce()
    {
        Assert.Equal(8, PgBaselineProvider.KeyedSetWidth);
        var tail = PgBaselineProvider.RobustTierScaffold + PgBaselineProvider.PerMemberScaffoldClose;
        static string Slot(int i) => $"queryid = ($7::BIGINT[])[{i}]";
        foreach (var metric in new[] { MetricNames.PgStatementShare, MetricNames.PgStatementMeanMs })
        {
            var sql = PgTargetBaselineProvider.GetPgTargetKeyedBaselineQuery(metric)!;
            Assert.EndsWith(tail, sql, StringComparison.Ordinal);
            Assert.Single(Regex.Matches(sql, Regex.Escape(PgBaselineProvider.PerMemberScaffoldHead)));
            var head = sql.IndexOf(PgBaselineProvider.PerMemberScaffoldHead, StringComparison.Ordinal);
            var own = sql[..head];
            var clean = sql[(head + PgBaselineProvider.PerMemberScaffoldHead.Length)..^tail.Length];

            /* ONE read of the table per compute, whatever the member count — the whole point of #3901 — and it is the
               per-statement arm's read: grouped by collection alone, so it keeps the parallel streaming aggregate. */
            Assert.Single(Regex.Matches(sql, @"\bFROM\s+pg_statement_stats\b"));
            Assert.Contains("FROM pg_statement_stats", own, StringComparison.Ordinal);
            Assert.Contains("server_id = $1 AND collection_time >= $2 AND collection_time < $3", own, StringComparison.Ordinal);
            Assert.Single(Regex.Matches(own, @"GROUP BY collection_time\r?\n"));
            Assert.DoesNotMatch(new Regex(@"GROUP BY [^\r\n]*queryid"), own);
            Assert.Contains("FROM unnest($7::TEXT[]) WITH ORDINALITY AS k(member_key, n)", own, StringComparison.Ordinal);

            /* One member predicate per slot, exactly as many slots as the provider packs keys into, cast from the array —
               no scalar key anywhere. */
            for (var i = 1; i <= PgBaselineProvider.KeyedSetWidth; i++)
                Assert.Contains("FILTER (WHERE " + Slot(i) + ")", own, StringComparison.Ordinal);
            Assert.DoesNotContain(Slot(PgBaselineProvider.KeyedSetWidth + 1), sql, StringComparison.Ordinal);
            Assert.DoesNotContain("$7::BIGINT)", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("= $7", sql, StringComparison.Ordinal);

            Assert.Contains("delta_total_exec_time_ms", own, StringComparison.Ordinal);
            Assert.Contains("DOUBLE PRECISION", own, StringComparison.Ordinal);
            /* The member reads its OWN slot of what the arm read once, and nothing but that. */
            Assert.Contains("[mem.member]", clean, StringComparison.Ordinal);
            Assert.Contains("FROM per_collection", clean, StringComparison.Ordinal);
            Assert.DoesNotContain("pg_statement_stats", clean, StringComparison.Ordinal);
            foreach (var part in new[] { own, clean })
            {
                Assert.DoesNotContain("EXTRACT", part, StringComparison.OrdinalIgnoreCase);
                Assert.DoesNotContain("now(", part, StringComparison.OrdinalIgnoreCase);
                Assert.DoesNotContain("CURRENT_TIMESTAMP", part, StringComparison.OrdinalIgnoreCase);
                Assert.DoesNotContain("LAG(", part, StringComparison.OrdinalIgnoreCase);   /* stored deltas, never re-differenced */
                Assert.DoesNotMatch(new Regex(@"\bFROM\s+v_"), part);
            }
        }

        /* The share: the per-statement arm's numerator per slot, its denominator and busy-collection rule once. */
        var share = PgTargetBaselineProvider.GetPgTargetKeyedBaselineQuery(MetricNames.PgStatementShare)!;
        Assert.Contains("SUM(delta_total_exec_time_ms)::DOUBLE PRECISION AS total_ms", share, StringComparison.Ordinal);
        Assert.Contains("CAST(SUM(delta_total_exec_time_ms) FILTER (WHERE " + Slot(1) + ") AS DOUBLE PRECISION)", share, StringComparison.Ordinal);
        Assert.Contains("] AS stmt_ms", share, StringComparison.Ordinal);
        Assert.Contains("HAVING SUM(delta_total_exec_time_ms) > 0", share, StringComparison.Ordinal);
        Assert.Contains("coalesce(stmt_ms[mem.member], 0) / total_ms AS v", share, StringComparison.Ordinal);
        Assert.DoesNotContain("WHERE", share[share.IndexOf(PgBaselineProvider.PerMemberScaffoldHead, StringComparison.Ordinal)..share.IndexOf(PgBaselineProvider.RobustTierScaffold, StringComparison.Ordinal)], StringComparison.Ordinal);
        Assert.DoesNotContain("delta_calls", share, StringComparison.Ordinal);
        Assert.DoesNotContain("ANY(", share, StringComparison.Ordinal);   /* the total needs every statement's rows */

        /* The mean: lane 27's per-call quantity and no-calls rule, per slot. The slot expression IS the server-wide arm's
           expression with the member's FILTER on both sums; the rule rides beside it as the member's calls sum. */
        var mean = PgTargetBaselineProvider.GetPgTargetKeyedBaselineQuery(MetricNames.PgStatementMeanMs)!;
        var serverWide = PgTargetBaselineProvider.GetPgTargetBaselineQuery(MetricNames.PgStatementMeanMs)!;
        const string perCall = "SUM(delta_total_exec_time_ms)::DOUBLE PRECISION / SUM(delta_calls)";
        Assert.Contains(perCall + " AS mean_ms", serverWide, StringComparison.Ordinal);
        Assert.Contains("HAVING SUM(delta_calls) > 0", serverWide, StringComparison.Ordinal);
        for (var i = 1; i <= PgBaselineProvider.KeyedSetWidth; i++)
        {
            var filter = " FILTER (WHERE " + Slot(i) + ")";
            var keyedPerCall = perCall
                .Replace("SUM(delta_total_exec_time_ms)::DOUBLE PRECISION", "CAST(SUM(delta_total_exec_time_ms)" + filter + " AS DOUBLE PRECISION)", StringComparison.Ordinal)
                .Replace("SUM(delta_calls)", "SUM(delta_calls)" + filter, StringComparison.Ordinal);
            Assert.Contains("CASE WHEN SUM(delta_calls)" + filter + " > 0 THEN " + keyedPerCall + " END", mean, StringComparison.Ordinal);
        }
        Assert.Contains("] AS mean_ms", mean, StringComparison.Ordinal);
        Assert.Contains("] AS calls", mean, StringComparison.Ordinal);
        Assert.Contains("AND   queryid = ANY($7::BIGINT[])", mean, StringComparison.Ordinal);
        Assert.Contains("WHERE calls[mem.member] > 0", mean, StringComparison.Ordinal);
        Assert.Contains("SELECT collection_time, mean_ms[mem.member] AS v", mean, StringComparison.Ordinal);
    }

    /* ───────────────────────── (4) the cardinality note ───────────────────────── */

    [Fact]
    public void TheCardinalityNote_FiresAboveTheBar_OncePerCachePeriod_AndTheBarIsUnmeasured()
    {
        Assert.Equal(500, PgBaselineProvider.KeyedBaselineCacheWarnCount);
        var now = new DateTime(2026, 9, 20, 12, 0, 0, DateTimeKind.Utc);

        Assert.False(PgBaselineProvider.ShouldWarnKeyedCardinality(0, null, now));
        Assert.False(PgBaselineProvider.ShouldWarnKeyedCardinality(500, null, now));          /* at the bar: bounded */
        Assert.True(PgBaselineProvider.ShouldWarnKeyedCardinality(501, null, now));           /* over, never noted */
        Assert.False(PgBaselineProvider.ShouldWarnKeyedCardinality(501, now.AddMinutes(-10), now));   /* noted this period */
        Assert.True(PgBaselineProvider.ShouldWarnKeyedCardinality(501, now - PgBaselineProvider.CacheTtl, now));   /* a period later */
        Assert.True(PgBaselineProvider.ShouldWarnKeyedCardinality(5_000, now.AddDays(-1), now));

        /* Lineage on the bar, in the six lines above it, in the unmeasured shape; the note is a Warning (a consumer
           defect), and it is raised only from the keyed insert path. */
        var source = ProviderSource;
        var bar = source.IndexOf("internal const int KeyedBaselineCacheWarnCount = 500;", StringComparison.Ordinal);
        Assert.True(bar > 0);
        var above = source[..bar];
        var lines = above.Split('\n');
        Assert.Contains(lines.TakeLast(7), line => line.Contains("Chosen, not measured", StringComparison.Ordinal) || line.Contains("calibrate against", StringComparison.Ordinal));
        var code = CSharpSourceWalker.StripCommentsAndStrings(source).Replace("\r\n", "\n", StringComparison.Ordinal);
        Assert.Contains("_logger?.LogWarning(", code, StringComparison.Ordinal);
        /* #3901: raised once per keyed compute, right after the set's entries are stored, inside the keyed path — and
           never on the unkeyed one. */
        Assert.Single(Regex.Matches(code, Regex.Escape("NoteKeyedCardinality(serverId, metricName);")));
        var keyedStart = code.IndexOf("Task<Dictionary<string, CachedBaseline>> GetOrComputeKeyedBaselinesAsync(", StringComparison.Ordinal);
        var keyedEnd = code.IndexOf("\n    private ", keyedStart, StringComparison.Ordinal);
        Assert.True(keyedStart > 0 && keyedEnd > keyedStart, "the keyed cache path moved");
        var keyedPath = code[keyedStart..keyedEnd];
        var stored = keyedPath.IndexOf("Store(serverId, CacheKeyFor(serverId, metricName, key), entry);", StringComparison.Ordinal);
        var noted = keyedPath.IndexOf("NoteKeyedCardinality(serverId, metricName);", StringComparison.Ordinal);
        Assert.True(stored > 0 && noted > stored && noted - stored < 200, "the note is raised right after the keyed entries are stored");

        var unkeyedStart = code.IndexOf("Task<CachedBaseline> GetOrComputeBaselinesAsync(", StringComparison.Ordinal);
        var unkeyedPath = code[unkeyedStart..code.IndexOf("\n    private ", unkeyedStart, StringComparison.Ordinal)];
        Assert.Contains("Store(serverId, cacheKey, entry);", unkeyedPath, StringComparison.Ordinal);
        Assert.DoesNotContain("NoteKeyedCardinality(", unkeyedPath, StringComparison.Ordinal);
    }

    /* ───────────────────────── helpers ───────────────────────── */

    private static IEnumerable<string> AllDeclaredMetricNames() =>
        typeof(MetricNames).GetFields(BindingFlags.Public | BindingFlags.Static | BindingFlags.FlattenHierarchy)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string))
            .Select(f => (string)f.GetRawConstantValue()!);

    /// <summary>The base provider with its protected seam exposed — a data source is built, never opened.</summary>
    private sealed class ExposingProvider : PgBaselineProvider
    {
        public ExposingProvider(NpgsqlDataSource postgres) : base(postgres) { }
        public string? KeyedQueryFor(string metricName) => ResolveKeyedBaselineQuery(metricName);
    }
}

/// <summary>
/// The live proof for <see cref="PgBaselineProviderKeyedTests"/>, in the <c>live-postgres</c> collection so the
/// shared store is established before it runs and the residue check runs after it (#1862, #1873). Also the parse
/// check for the two keyed arms: <c>DarlingPgReadSqlParsesLiveTests</c> sweeps <c>DarlingPg*Reader</c> types, not
/// the providers, so executing both arms through the real provider here is where their text first meets a server.
/// </summary>
[Collection("live-postgres")]
public sealed class PgBaselineProviderKeyedLiveTests
{
    private const int ServerId = -3691_33;
    private const string ServerName = "lane33-keyed-baseline-e2e";
    private const long StatementA = 9001;
    private const long StatementB = 9002;
    private const int Tuesday = (int)DayOfWeek.Tuesday;

    /// <summary>
    /// Two statements, three Tuesdays, twelve five-minute collections in the 12h hour each: A runs 100 calls × 8 ms
    /// (800 ms), B 50 calls × 4 ms (200 ms) — shares 0.8 / 0.2, per-call means 8 / 4 — except the first collection
    /// of each Tuesday, where B did not run (A's share 1.0, B's 0 — a ZERO sample for the share, NO sample for B's
    /// mean). The analysis time is a Tuesday 12:30, so the lookup lands on the (12, Tue) bucket. Two keys → two
    /// distinct Full buckets from two computes; the unkeyed share call is Empty (no arm); a keyed call for a metric
    /// with no keyed shape is Empty; a non-numeric key fails the cast inside the one classified catch and is Empty;
    /// the SQL Server provider over the same store answers Empty to every keyed call; and the keyed entry count is
    /// what the cardinality note would report.
    /// </summary>
    [Fact]
    public async Task Live_TwoStatements_AreTwoSeries_AndEveryCrossArmCallIsEmpty()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live keyed-baseline test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            var analysisTime = new DateTime(2026, 3, 10, 12, 30, 0, DateTimeKind.Unspecified);   // a Tuesday
            foreach (var tuesday in new[] { new DateTime(2026, 2, 17), new DateTime(2026, 2, 24), new DateTime(2026, 3, 3) })
            {
                for (var i = 0; i < 12; i++)
                {
                    var at = tuesday.AddHours(12).AddMinutes(i * 5);
                    await PlantAsync(connection, at, StatementA, calls: 100, totalMs: 800, ct);
                    if (i > 0)
                    {
                        await PlantAsync(connection, at, StatementB, calls: 50, totalMs: 200, ct);
                    }
                }
            }

            var provider = new PgTargetBaselineProvider(postgres);

            var shareA = await provider.GetBaselineAsync(ServerId, MetricNames.PgStatementShare, StatementA.ToString(System.Globalization.CultureInfo.InvariantCulture), analysisTime, ct);
            var shareB = await provider.GetBaselineAsync(ServerId, MetricNames.PgStatementShare, StatementB.ToString(System.Globalization.CultureInfo.InvariantCulture), analysisTime, ct);

            Assert.Equal(BaselineTier.Full, shareA.Tier);
            Assert.Equal((12, Tuesday), (shareA.HourOfDay, shareA.DayOfWeek));
            Assert.Equal(36L, shareA.SampleCount);
            Assert.Equal(3L, shareA.DistinctDays);
            Assert.Equal((33 * 0.8 + 3 * 1.0) / 36, shareA.Mean, 0.0001);
            Assert.Equal(0.8, shareA.Median, 0.0001);

            Assert.Equal(BaselineTier.Full, shareB.Tier);
            Assert.Equal((12, Tuesday), (shareB.HourOfDay, shareB.DayOfWeek));
            Assert.Equal(36L, shareB.SampleCount);                 /* the collections B sat out are ZERO samples */
            Assert.Equal((33 * 0.2 + 3 * 0.0) / 36, shareB.Mean, 0.0001);
            Assert.Equal(0.2, shareB.Median, 0.0001);
            Assert.NotEqual(shareA.Mean, shareB.Mean);

            var meanA = await provider.GetBaselineAsync(ServerId, MetricNames.PgStatementMeanMs, StatementA.ToString(System.Globalization.CultureInfo.InvariantCulture), analysisTime, ct);
            var meanB = await provider.GetBaselineAsync(ServerId, MetricNames.PgStatementMeanMs, StatementB.ToString(System.Globalization.CultureInfo.InvariantCulture), analysisTime, ct);
            Assert.Equal(36L, meanA.SampleCount);
            Assert.Equal(8.0, meanA.Mean, 0.0001);
            Assert.Equal(33L, meanB.SampleCount);                  /* the collections B sat out are NO sample for its mean */
            Assert.Equal(4.0, meanB.Mean, 0.0001);

            /* Lane 27's unkeyed arm of the same name still answers the unkeyed call: the SERVER-WIDE per-call mean,
               1000 ms / 150 calls where both ran, 8 where only A did. */
            var serverWide = await provider.GetBaselineAsync(ServerId, MetricNames.PgStatementMeanMs, analysisTime, ct);
            Assert.Equal(36L, serverWide.SampleCount);
            Assert.Equal((33 * (1000.0 / 150) + 3 * 8.0) / 36, serverWide.Mean, 0.0001);

            /* Cross-arm calls are "no baseline", never the other arm's buckets. */
            AssertEmpty(await provider.GetBaselineAsync(ServerId, MetricNames.PgStatementShare, analysisTime, ct));
            AssertEmpty(await provider.GetBaselineAsync(ServerId, MetricNames.PgTps, StatementA.ToString(System.Globalization.CultureInfo.InvariantCulture), analysisTime, ct));
            AssertEmpty(await provider.GetBaselineAsync(ServerId, MetricNames.PgStatementShare, "not-a-queryid", analysisTime, ct));

            /* Four keyed series computed (two metrics × two statements); the no-arm call (pg_tps keyed) and the failed
               cast each cached their empty compute as a keyed entry too, exactly as an unkeyed no-arm or failed compute
               caches its empty — the note counts entries, not successes, because a consumer asking for a thousand
               statements that have no arm is as unbounded as one that has. The unkeyed lookups are not keyed entries. */
            Assert.Equal(6, provider.KeyedEntryCount);

            /* The SQL Server provider over the same store: no keyed metric declared, so every keyed call is Empty. */
            var sqlServer = new PgBaselineProvider(postgres);
            AssertEmpty(await sqlServer.GetBaselineAsync(ServerId, MetricNames.PgStatementShare, StatementA.ToString(System.Globalization.CultureInfo.InvariantCulture), analysisTime, ct));
            AssertEmpty(await sqlServer.GetBaselineAsync(ServerId, MetricNames.Cpu, "anything", analysisTime, ct));
            Assert.Equal(2, sqlServer.KeyedEntryCount);

            /* And a cache hit is a cache hit: the same key again computes nothing new (the entry count holds). */
            var shareAAgain = await provider.GetBaselineAsync(ServerId, MetricNames.PgStatementShare, StatementA.ToString(System.Globalization.CultureInfo.InvariantCulture), analysisTime, ct);
            Assert.Equal(shareA.Mean, shareAAgain.Mean);
            Assert.Equal(6, provider.KeyedEntryCount);
            provider.InvalidateCache(ServerId);
            Assert.Equal(0, provider.KeyedEntryCount);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await CleanupAsync(cleanup, cleanupCt);
            });
        }
    }

    /// <summary><c>BaselineBucket.Empty</c> is a fresh instance per read, so "no baseline" is a shape, not a reference:
    /// the flat sentinel with nothing in it.</summary>
    private static void AssertEmpty(BaselineBucket bucket)
    {
        Assert.Equal(BaselineTier.Flat, bucket.Tier);
        Assert.Equal(0L, bucket.SampleCount);
        Assert.Equal(0L, bucket.DistinctDays);
        Assert.Equal(0.0, bucket.Mean);
        Assert.Equal(-1, bucket.HourOfDay);
    }

    private static async Task PlantAsync(NpgsqlConnection connection, DateTime at, long queryId, long calls, double totalMs, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_statement_stats
    (collection_id, collection_time, server_id, server_name, queryid, database_id, user_id, toplevel,
     calls, total_exec_time_ms, max_exec_time_ms, rows_returned, shared_blks_hit, shared_blks_read,
     temp_blks_read, temp_blks_written, wal_bytes, delta_calls, delta_total_exec_time_ms, delta_rows, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, 16384, 10, TRUE, 0, 0, 50.0, 10, 10, 5, 0, 0, 0, $6, $7, $6, 300)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(queryId);
        command.Parameters.AddWithValue(calls);
        command.Parameters.AddWithValue(totalMs);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task CleanupAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand($"DELETE FROM pg_statement_stats WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
