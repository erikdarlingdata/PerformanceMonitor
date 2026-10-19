/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3901: a PostgreSQL-target pass read the statement family's KEYED baselines one statement at a time — the share
/// detector for each of its top statements, the plan-regression detector for each flipped one — and each read took
/// the server's whole 30-day slice of <c>pg_statement_stats</c>, because the family's only index is (server_id,
/// collection_time) and the statement predicate is a heap filter. On the production PostgreSQL store that was about
/// 70 % of a cold <c>analyze_server</c>. The fix reads each metric ONCE for the whole candidate set
/// (<c>PgBaselineProvider.GetBaselinesAsync</c>, <c>$7</c> a <c>text[]</c>) and runs the one robust scaffold per member.
///
/// <para>These are the source-level pins: the set surface, both detectors asking once ahead of their loops, and the
/// per-member wrapper being the scaffold verbatim. <see cref="PgStatementKeyedSetLiveTests"/> holds the set arms to the
/// per-statement arms they replaced, row for row, and counts the statements a detector pass issues.</para>
/// </summary>
public sealed class PgStatementKeyedSetTests
{
    [Fact]
    public void TheSetSurface_TakesTheKeysAsACollection_AndReturnsTheSharedBucketPerKey()
    {
        var method = typeof(PgBaselineProvider).GetMethod(nameof(PgBaselineProvider.GetBaselinesAsync));
        Assert.NotNull(method);
        Assert.Equal(
            new[] { typeof(int), typeof(string), typeof(IReadOnlyCollection<string>), typeof(DateTime), typeof(CancellationToken) },
            method!.GetParameters().Select(p => p.ParameterType).ToArray());
        /* The same shared bucket the singles return (SharedBaselineModelPinTests' rule), one per key. */
        Assert.Equal(typeof(Task<IReadOnlyDictionary<string, BaselineBucket>>), method.ReturnType);
        Assert.Equal("PerformanceMonitor.Analysis", typeof(BaselineBucket).Assembly.GetName().Name);
    }

    /// <summary>
    /// The regression pin at the detector seam: both keyed detectors turn their candidates into keys and ask for the
    /// whole set in ONE call BEFORE the loop that grades them, then look each bucket up by the same key string. A keyed
    /// <c>GetBaselineAsync</c> anywhere in either file — five arguments, the per-statement read — fails here; the plan
    /// detector's remaining single lookup is the unkeyed cold fallback (four arguments).
    /// </summary>
    [Fact]
    public void BothKeyedDetectors_AskForTheirWholeCandidateSetOnce_AheadOfTheLoop()
    {
        foreach (var (file, metric, lookup) in new[]
        {
            ("PgTargetAnomalyDetector.Queries.cs", "MetricNames.PgStatementShare", "var baseline = baselines[key];"),
            ("PgTargetAnomalyDetector.Plans.cs", "MetricNames.PgStatementMeanMs", "var keyed = keyedBaselines[key];"),
        })
        {
            var code = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", file))
                .Replace("\r\n", "\n", StringComparison.Ordinal);

            var setRead = code.IndexOf(
                "_baselineProvider.GetBaselinesAsync(\n                context.ServerId, " + metric + ", keys, context.TimeRangeStart, context.CancellationToken);",
                StringComparison.Ordinal);
            var loop = code.IndexOf("foreach (var candidate in candidates)", StringComparison.Ordinal);
            Assert.True(setRead > 0, $"{file}: the candidate set is not read through GetBaselinesAsync");
            Assert.True(loop > setRead, $"{file}: the set read must precede the candidate loop");
            Assert.Single(Regex.Matches(code, Regex.Escape("_baselineProvider.GetBaselinesAsync(")));
            Assert.Single(Regex.Matches(code, Regex.Escape("foreach (var candidate in candidates)")));

            /* The keys are the candidates' queryids rendered invariant — the same strings the loop looks up by. */
            Assert.Contains("var keys = candidates.ConvertAll(candidate => candidate.QueryId.ToString(CultureInfo.InvariantCulture));", code, StringComparison.Ordinal);
            Assert.Contains("var key = candidate.QueryId.ToString(CultureInfo.InvariantCulture);", code, StringComparison.Ordinal);
            Assert.True(code.IndexOf(lookup, StringComparison.Ordinal) > loop, $"{file}: each candidate's bucket is looked up inside the loop");

            foreach (Match call in Regex.Matches(code, @"_baselineProvider\.GetBaselineAsync\(([^;]*)\);"))
            {
                Assert.Equal(4, call.Groups[1].Value.Split(',').Length);
            }
        }
    }

    [Fact]
    public void PerMemberScaffold_IsTheOneScaffoldVerbatim_WithTheMemberRidingLast()
    {
        var sql = PgBaselineProvider.PerMemberScaffold("\n    SELECT collection_time, v FROM somewhere");
        Assert.StartsWith(PgBaselineProvider.PerMemberScaffoldHead, sql, StringComparison.Ordinal);
        Assert.EndsWith(PgBaselineProvider.RobustTierScaffold + PgBaselineProvider.PerMemberScaffoldClose, sql, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(sql, Regex.Escape(PgBaselineProvider.RobustTierScaffold)));

        /* The eight robust columns come out of the scaffold untouched and the member rides LAST, so the reader's ordinals
           0..7 mean the same thing for a keyed statement as for an unkeyed one; the member set is the arm's `members`. */
        var head = PgBaselineProvider.PerMemberScaffoldHead.Replace("\r\n", "\n", StringComparison.Ordinal);
        Assert.Equal("\nSELECT per_member.*, mem.member\nFROM members AS mem\nCROSS JOIN LATERAL (\nWITH clean AS (", head);
        Assert.Equal("\n) AS per_member", PgBaselineProvider.PerMemberScaffoldClose.Replace("\r\n", "\n", StringComparison.Ordinal));

        /* The wrapper's aliases never collide with the scaffold's own (t, m, k, keyed, tier_*): `mem` and `per_member`. */
        Assert.DoesNotMatch(new Regex(@"\bAS\s+(t|m|k)\b"), PgBaselineProvider.PerMemberScaffoldHead + PgBaselineProvider.PerMemberScaffoldClose);
        Assert.DoesNotContain(" mem", PgBaselineProvider.RobustTierScaffold, StringComparison.Ordinal);
        Assert.DoesNotContain("per_member", PgBaselineProvider.RobustTierScaffold, StringComparison.Ordinal);
    }
}

/// <summary>
/// The live proof for <see cref="PgStatementKeyedSetTests"/>, in the <c>live-postgres</c> collection so the shared store
/// is established before it runs and the residue check runs after it (#1862, #1873). Three questions, each on planted
/// <c>pg_statement_stats</c> built to hit every rule the arms state: are the set arms' rows the per-statement arms' rows
/// for every member; does the provider read a set once, compute only its misses and hand each key its own buckets;
/// and does a real detector pass issue one keyed read per metric however many candidates it grades.
/// </summary>
[Collection("live-postgres")]
public sealed class PgStatementKeyedSetLiveTests
{
    private const int ServerId = -3901_01;
    private const string ServerName = "3901-keyed-set";

    /* Thursday 2026-03-12 10:30 UTC: the 30-day window straddles the US spring-forward (2026-03-08 07:00 UTC). */
    private static readonly DateTime AnalysisTime = new(2026, 3, 12, 10, 30, 0, DateTimeKind.Unspecified);
    private static readonly DateTime SpringForward = new(2026, 3, 8, 7, 0, 0, DateTimeKind.Unspecified);

    private const long StatementA = 9_001;                     /* every collection, two rows (two databases) */
    private const long StatementB = -8_886_085_649_470_066_503; /* a real-looking signed id; idle every third collection */
    private const long StatementC = 42;                        /* time without calls, and calls with a NULL time */
    private const long StatementD = 7;                         /* only 09:00–17:59 UTC: absent, not zero, otherwise */
    private const long StatementE = 1_234_567;                 /* only in the window's first week */
    private const long StatementF = 777;                       /* never ran: a member with no rows at all */
    private const long Other = 5_555;                          /* everyone else, with the NULL-queryid rows */

    private static readonly string[] Keys =
        new[] { StatementA, StatementB, StatementC, StatementD, StatementE, StatementF }.Select(Invariant).ToArray();

    /* The per-statement arms #3901 replaced, verbatim as shipped at e507aa2d (PgTargetBaselineProvider.Statements.cs),
       each followed by the one scaffold #3901 did not touch — the oracle the set arms are held to. */
    private const string PerStatementShareArm = @"
WITH per_collection AS (
    SELECT collection_time,
           SUM(delta_total_exec_time_ms)::DOUBLE PRECISION AS total_ms,
           CAST(SUM(delta_total_exec_time_ms) FILTER (WHERE queryid = $7::BIGINT) AS DOUBLE PRECISION) AS stmt_ms
    FROM pg_statement_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    GROUP BY collection_time
    HAVING SUM(delta_total_exec_time_ms) > 0
),
clean AS (
    SELECT collection_time, coalesce(stmt_ms, 0) / total_ms AS v
    FROM per_collection
),";

    private const string PerStatementMeanArm = @"
WITH per_collection AS (
    SELECT collection_time,
           SUM(delta_total_exec_time_ms)::DOUBLE PRECISION / SUM(delta_calls) AS mean_ms
    FROM pg_statement_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    AND   queryid = $7::BIGINT
    GROUP BY collection_time
    HAVING SUM(delta_calls) > 0
),
clean AS (
    SELECT collection_time, mean_ms AS v
    FROM per_collection
),";

    /// <summary>
    /// The SQL itself, both metrics, every member: the set arm's rows for member <c>i</c> are the per-statement arm's rows
    /// for <c>Keys[i - 1]</c> — same tiers, same sample and day counts, same medians and MADs, and the same means and
    /// standard deviations up to float8 summation order (AVG and STDDEV_SAMP accumulate in whatever order rows reach
    /// them, which neither statement fixes; everything order-free is compared exactly). Under a DST-straddling clock, so
    /// the per-member scaffold is shown keying on the target's local hour exactly as the per-statement one did. The
    /// planted rows hit every rule the arms state: two rows for one statement in one collection (pooled), a statement
    /// idle in some collections (a ZERO share sample, NO mean sample), calls of zero beside time (no mean sample), calls
    /// beside a NULL time (a mean sample whose value is NULL — counted, never averaged — which is why the set arm keys its
    /// no-calls rule on the calls and not on the value), a statement absent outside business hours and another absent
    /// after its first week, a member that never ran, NULL
    /// queryids and NULL deltas in the denominator, collections where nothing ran (no sample for anyone), and rows on
    /// both sides of the half-open window.
    /// </summary>
    [Fact]
    public async Task Live_TheSetArms_ReturnThePerStatementArmsRows_ForEveryMember()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the #3901 set-arm equivalence test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ServerId, ct);

        var bodySucceeded = false;
        try
        {
            await PlantStatementsAsync(connection, ServerId, ServerName, AnalysisTime.AddDays(-31), AnalysisTime.AddHours(1), ct);

            var compared = 0;
            foreach (var (metric, perStatement) in new[] { (MetricNames.PgStatementShare, PerStatementShareArm), (MetricNames.PgStatementMeanMs, PerStatementMeanArm) })
            {
                var set = await ReadRowsAsync(connection, PgTargetBaselineProvider.GetPgTargetKeyedBaselineQuery(metric)!, Keys, ct);
                Assert.All(set, row => Assert.InRange(row.Member, 1, Keys.Length));

                for (var member = 1; member <= Keys.Length; member++)
                {
                    var expected = await ReadRowsAsync(connection, perStatement + PgBaselineProvider.RobustTierScaffold, Keys[member - 1], ct);
                    var actual = set.Where(row => row.Member == member).ToList();
                    AssertSameRows(expected, actual, $"{metric} member {member} ({Keys[member - 1]})");
                    compared += expected.Count;
                }
            }

            Assert.True(compared > 1_000, $"only {compared} tier rows were compared — the planted history is not exercising the scaffold");

            /* The rules, stated as outcomes on the planted data (both arms agree on them, per the loop above). */
            var share = await ReadRowsAsync(connection, PgTargetBaselineProvider.GetPgTargetKeyedBaselineQuery(MetricNames.PgStatementShare)!, Keys, ct);
            var mean = await ReadRowsAsync(connection, PgTargetBaselineProvider.GetPgTargetKeyedBaselineQuery(MetricNames.PgStatementMeanMs)!, Keys, ct);
            static BucketRow Flat(List<BucketRow> rows, int member) => Assert.Single(rows, r => r.Member == member && r.Hour == -1 && r.Dow == -1);

            /* F never ran: every busy collection is a zero share sample, and it has no mean sample at all. */
            var fShare = Flat(share, 6);
            Assert.Equal(0.0, fShare.Mean);
            Assert.Equal(Flat(share, 1).Count, fShare.Count);
            Assert.DoesNotContain(mean, r => r.Member == 6);
            /* B sat out every third collection: a zero share sample (same count as A), but no mean sample. */
            Assert.Equal(Flat(share, 1).Count, Flat(share, 2).Count);
            Assert.True(Flat(mean, 2).Count < Flat(share, 2).Count);
            /* D is absent outside business hours, E after its first week: both still a sample in every busy collection. */
            Assert.Equal(Flat(share, 1).Count, Flat(share, 4).Count);
            Assert.Equal(Flat(share, 1).Count, Flat(share, 5).Count);
            Assert.True(Flat(mean, 5).Count < Flat(mean, 1).Count / 3);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, ServerId, cleanupCt));
        }
    }

    /// <summary>
    /// The provider over the same planted store: a set of six is ONE keyed statement (counted off Npgsql's own command
    /// activities, parented under this test's root so no other test's commands are counted), and each key's bucket is
    /// the bucket a single-key lookup on a fresh provider returns — the member numbering maps every row to its own key.
    /// A second set that repeats two cached keys and adds one new key issues one statement for the one miss; an empty
    /// set issues none; a repeated key is asked for once; a set wider than the arms' <c>KeyedSetWidth</c> member slots
    /// is read that many keys at a time and still hands every key its own buckets; a set with a key that is not a number
    /// fails as a set, into the one classified catch, and caches "no baseline" for its members exactly as the
    /// single-key lookup does.
    /// </summary>
    [Fact]
    public async Task Live_TheProvider_ReadsASetOnce_ComputesOnlyItsMisses_AndHandsEachKeyItsOwnBuckets()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the #3901 provider set test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ServerId, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await PlantStatementsAsync(connection, ServerId, ServerName, AnalysisTime.AddDays(-31), AnalysisTime.AddHours(1), ct);

            foreach (var metric in new[] { MetricNames.PgStatementShare, MetricNames.PgStatementMeanMs })
            {
                var provider = new PgTargetBaselineProvider(postgres);
                var (set, setReads) = await CountingKeyedReadsAsync(() => provider.GetBaselinesAsync(ServerId, metric, Keys, AnalysisTime, ct));
                Assert.Equal(1, setReads);
                Assert.Equal(Keys.Length, set.Count);
                Assert.Equal(Keys.Length, provider.KeyedEntryCount);

                var singles = new PgTargetBaselineProvider(postgres);
                var singleReads = 0;
                foreach (var key in Keys)
                {
                    var (single, reads) = await CountingKeyedReadsAsync(() => singles.GetBaselineAsync(ServerId, metric, key, AnalysisTime, ct));
                    singleReads += reads;
                    AssertSameBucket(single, set[key], $"{metric} {key}");
                }

                Assert.Equal(Keys.Length, singleReads);
                /* The planted members really are different series, so a mis-mapped member could not pass the loop above:
                   every key with a baseline has a mean no other key has (F has no mean-ms rows, so no bucket there). */
                var withBaseline = set.Values.Where(bucket => bucket.SampleCount > 0).ToList();
                Assert.Equal(metric == MetricNames.PgStatementMeanMs ? Keys.Length - 1 : Keys.Length, withBaseline.Count);
                Assert.Equal(withBaseline.Count, withBaseline.Select(bucket => bucket.Mean).Distinct().Count());

                /* Two cached keys and one new one: ONE statement, for the miss; the cached buckets are the same objects. */
                var extra = Invariant(Other);
                var (again, againReads) = await CountingKeyedReadsAsync(() => provider.GetBaselinesAsync(ServerId, metric, new[] { Keys[0], extra, Keys[1], Keys[0] }, AnalysisTime, ct));
                Assert.Equal(1, againReads);
                Assert.Equal(3, again.Count);
                Assert.Same(set[Keys[0]], again[Keys[0]]);
                Assert.Same(set[Keys[1]], again[Keys[1]]);
                Assert.Equal(Keys.Length + 1, provider.KeyedEntryCount);

                var (empty, emptyReads) = await CountingKeyedReadsAsync(() => provider.GetBaselinesAsync(ServerId, metric, Array.Empty<string>(), AnalysisTime, ct));
                Assert.Empty(empty);
                Assert.Equal(0, emptyReads);

                /* Wider than the arms' member slots: packed KeyedSetWidth at a time, one read each, and every key still
                   gets its own buckets — the member numbering restarts per read. */
                var wide = Keys.Concat(new[] { Invariant(Other) }).Concat(Enumerable.Range(1, PgBaselineProvider.KeyedSetWidth).Select(i => Invariant(900_000 + i))).ToArray();
                var packed = new PgTargetBaselineProvider(postgres);
                var (wideSet, wideReads) = await CountingKeyedReadsAsync(() => packed.GetBaselinesAsync(ServerId, metric, wide, AnalysisTime, ct));
                Assert.Equal((wide.Length + PgBaselineProvider.KeyedSetWidth - 1) / PgBaselineProvider.KeyedSetWidth, wideReads);
                Assert.Equal(wide.Length, wideSet.Count);
                foreach (var key in wide)
                    AssertSameBucket(await singles.GetBaselineAsync(ServerId, metric, key, AnalysisTime, ct), wideSet[key], $"{metric} {key} (wide)");
            }

            /* A key that is not a number fails the set's cast: every member of THAT set reads "no baseline" — the
               single-key lookup's long-standing answer for the same key — and nothing is thrown at the caller. */
            var poisoned = new PgTargetBaselineProvider(postgres);
            var badSet = await poisoned.GetBaselinesAsync(ServerId, MetricNames.PgStatementShare, new[] { Keys[0], "not-a-queryid" }, AnalysisTime, ct);
            Assert.All(badSet.Values, bucket => Assert.Equal(0L, bucket.SampleCount));
            Assert.Equal(0L, (await poisoned.GetBaselineAsync(ServerId, MetricNames.PgStatementShare, "not-a-queryid", AnalysisTime, ct)).SampleCount);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, ServerId, cleanupCt));
        }
    }

    /// <summary>
    /// The seam: a real <see cref="PgTargetAnomalyDetector"/> pass over a store where the share detector has five
    /// candidates and the plan-regression detector two flipped statements. Before #3901 that pass issued one keyed read
    /// of <c>pg_statement_stats</c> per candidate — seven; it now issues exactly one per metric.
    /// </summary>
    [Fact]
    public async Task Live_ADetectorPass_IssuesOneKeyedReadPerMetric_ForItsWholeCandidateSet()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the #3901 detector seam test.");

        const int serverId = -3901_02;
        const string serverName = "3901-keyed-set-pass";
        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, serverId, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            var windowStart = AnalysisTime;
            var windowEnd = AnalysisTime.AddHours(4);
            await PlantStatementsAsync(connection, serverId, serverName, windowStart.AddDays(-31), windowEnd, ct);

            /* The whole-pass gate reads pg_database_stats over the 30 days before the window end. */
            for (var hour = 0; hour < 24; hour++)
                await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, serverId, serverName, windowStart.AddHours(-hour), ct);

            /* Two statements captured under two plans each inside the window: the plan detector's flip set. */
            foreach (var queryId in new[] { StatementA, StatementC })
            {
                await PlantCaptureAsync(connection, serverId, serverName, windowStart.AddMinutes(20), queryId, "aaaaaaaaaaaaaaaa", ct);
                await PlantCaptureAsync(connection, serverId, serverName, windowStart.AddMinutes(140), queryId, "bbbbbbbbbbbbbbbb", ct);
            }

            var context = new AnalysisContext
            {
                ServerId = serverId,
                ServerName = serverName,
                TimeRangeStart = windowStart,
                TimeRangeEnd = windowEnd,
                ServerUtcOffset = TimeSpan.Zero,
            };

            var detector = new PgTargetAnomalyDetector(postgres, new PgTargetBaselineProvider(postgres));
            var (_, commands) = await CapturingCommandsAsync(() => detector.DetectAnomaliesAsync(context));

            /* Precondition: the pass really had several candidates for each keyed metric. */
            Assert.Contains(commands, c => c.Contains("LIMIT $4", StringComparison.Ordinal) && c.Contains("CROSS JOIN collections AS c", StringComparison.Ordinal));
            var keyedReads = commands.Where(IsKeyedStatementRead).ToList();
            Assert.Equal(2, keyedReads.Count);
            Assert.Single(keyedReads, c => c.Contains("] AS stmt_ms", StringComparison.Ordinal));
            Assert.Single(keyedReads, c => c.Contains("] AS mean_ms", StringComparison.Ordinal));

            /* And the candidate sets those two reads covered, re-derived from the detectors' own window reads: five
               statements for the share, two flipped ones for the mean — seven per-statement reads before #3901. */
            var shareCandidates = await CountAsync(connection, PgTargetAnomalyDetector.StatementShareWindowSql, context, PgTargetFactCollector.TopStatementCount, ct);
            var flipCandidates = await CountAsync(connection, PgTargetAnomalyDetector.StatementMeanKeyedWindowSql, context, PgTargetFactCollector.TopStatementCount, ct);
            Assert.Equal(5, shareCandidates);
            Assert.Equal(2, flipCandidates);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, serverId, cleanupCt));
        }
    }

    /* ───────────────────────── reading ───────────────────────── */

    private readonly record struct BucketRow(int Hour, int Dow, double? Mean, double? StdDev, long Count, long Days, double? Median, double? Mad, long Member);

    /// <summary>One bucket statement, bound exactly as the provider binds it, with the DST-straddling clock: the key as
    /// text for a per-statement arm, the keys as text[] for a set arm (whose rows carry their member).</summary>
    private static async Task<List<BucketRow>> ReadRowsAsync(NpgsqlConnection connection, string sql, object key, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 300 };
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(AnalysisTime.AddDays(-BaselineMath.BaselineWindowDays));
        command.Parameters.AddWithValue(AnalysisTime);
        command.Parameters.AddWithValue(SpringForward);
        command.Parameters.AddWithValue(-300);
        command.Parameters.AddWithValue(-240);
        command.Parameters.AddWithValue(key);

        var rows = new List<BucketRow>();
        using var reader = await command.ExecuteReaderAsync(ct);
        var member = (key is string[]) ? reader.GetOrdinal("member") : -1;
        while (await reader.ReadAsync(ct))
        {
            double? Real(int ordinal) => reader.IsDBNull(ordinal) ? null : reader.GetDouble(ordinal);
            rows.Add(new BucketRow(
                Convert.ToInt32(reader.GetValue(0)), Convert.ToInt32(reader.GetValue(1)),
                Real(2), Real(3), Convert.ToInt64(reader.GetValue(4)), Convert.ToInt64(reader.GetValue(5)), Real(6), Real(7),
                member < 0 ? 0 : Convert.ToInt64(reader.GetValue(member))));
        }

        return rows;
    }

    private static void AssertSameRows(List<BucketRow> expected, List<BucketRow> actual, string what)
    {
        Assert.True(expected.Count == actual.Count, $"{what}: {expected.Count} tier rows per statement, {actual.Count} for the member");
        var byTier = actual.ToDictionary(r => (r.Hour, r.Dow));
        foreach (var e in expected)
        {
            Assert.True(byTier.TryGetValue((e.Hour, e.Dow), out var a), $"{what}: tier ({e.Hour}, {e.Dow}) missing from the set");
            Assert.Equal(e.Count, a.Count);
            Assert.Equal(e.Days, a.Days);
            Assert.Equal(e.Median, a.Median);
            Assert.Equal(e.Mad, a.Mad);
            AssertSameReal(e.Mean, a.Mean, $"{what} ({e.Hour}, {e.Dow}) mean");
            AssertSameReal(e.StdDev, a.StdDev, $"{what} ({e.Hour}, {e.Dow}) stddev");
        }
    }

    /// <summary>Equal, or equal to float8 summation order: a relative 1e-12 (DARLING01's real rows differed by at most
    /// 4.5e-14). NULL only where the other is NULL (a one-sample tier's STDDEV_SAMP).</summary>
    private static void AssertSameReal(double? expected, double? actual, string what)
    {
        Assert.True(expected.HasValue == actual.HasValue, $"{what}: {expected} vs {actual}");
        if (expected is not { } x || actual is not { } y || x == y)
            return;
        Assert.True(Math.Abs(x - y) <= 1e-12 * Math.Max(Math.Abs(x), Math.Abs(y)), $"{what}: {x:R} vs {y:R}");
    }

    private static void AssertSameBucket(BaselineBucket expected, BaselineBucket actual, string what)
    {
        Assert.Equal(expected.Tier, actual.Tier);
        Assert.Equal((expected.HourOfDay, expected.DayOfWeek), (actual.HourOfDay, actual.DayOfWeek));
        Assert.Equal(expected.SampleCount, actual.SampleCount);
        Assert.Equal(expected.DistinctDays, actual.DistinctDays);
        Assert.Equal(expected.Median, actual.Median);
        Assert.Equal(expected.Mad, actual.Mad);
        AssertSameReal(expected.Mean, actual.Mean, what + " mean");
        AssertSameReal(expected.StdDev, actual.StdDev, what + " stddev");
    }

    /* ───────────────────────── counting ───────────────────────── */

    /// <summary>A keyed bucket read of the statement table: the only statements that read <c>pg_statement_stats</c> AND
    /// bind a member key. The per-statement arms (<c>$7::BIGINT</c>) and the set arms (<c>$7::TEXT[]</c>) both match, so
    /// the count is of reads, whatever their shape.</summary>
    private static bool IsKeyedStatementRead(string sql) =>
        Regex.IsMatch(sql, @"\bFROM\s+pg_statement_stats\b") && sql.Contains("$7", StringComparison.Ordinal);

    private static async Task<(T Result, int KeyedReads)> CountingKeyedReadsAsync<T>(Func<Task<T>> body)
    {
        var (result, commands) = await CapturingCommandsAsync(body);
        return (result, commands.Count(IsKeyedStatementRead));
    }

    /// <summary>
    /// Every command text Npgsql executed inside <paramref name="body"/>, off its own tracing activities (source
    /// <c>Npgsql</c>, the statement in <c>db.query.text</c>), kept only when they descend from this call's root
    /// activity — so commands other tests run concurrently on the same store are never counted.
    /// </summary>
    private static async Task<(T Result, List<string> Commands)> CapturingCommandsAsync<T>(Func<Task<T>> body)
    {
        using var source = new ActivitySource("Darling.Tests.3901");
        var captured = new ConcurrentQueue<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = s => s.Name == "Npgsql" || s.Name == source.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = captured.Enqueue,
        };
        ActivitySource.AddActivityListener(listener);

        T result;
        ActivityTraceId trace;
        using (var root = source.StartActivity("keyed-set-probe"))
        {
            Assert.NotNull(root);
            trace = root!.TraceId;
            result = await body();
        }

        var commands = captured
            .Where(a => a.Source.Name == "Npgsql" && a.TraceId == trace)
            .Select(a => a.TagObjects.Select(t => t.Value as string).FirstOrDefault(v => v is not null && v.Contains("SELECT", StringComparison.OrdinalIgnoreCase)))
            .Where(text => text is not null)
            .Select(text => text!)
            .ToList();
        return (result, commands);
    }

    private static async Task<int> CountAsync(NpgsqlConnection connection, string sql, AnalysisContext context, int limit, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue(context.ServerId);
        command.Parameters.AddWithValue(context.TimeRangeStart);
        command.Parameters.AddWithValue(context.TimeRangeEnd);
        command.Parameters.AddWithValue(limit);
        var count = 0;
        using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            if (!reader.IsDBNull(0)) count++;
        return count;
    }

    /* ───────────────────────── planting ───────────────────────── */

    private static string Invariant(long queryId) => queryId.ToString(CultureInfo.InvariantCulture);

    /// <summary>
    /// A collection every fifteen minutes from <paramref name="from"/> to <paramref name="to"/>, set-based, one row per
    /// (statement, database) as the collector stores it. The values are arithmetic on the collection's index, so the
    /// buckets have a spread, and every rule the arms state has rows that exercise it (see the equivalence test's doc).
    /// </summary>
    private static async Task PlantStatementsAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime from, DateTime to, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_statement_stats
    (collection_id, collection_time, server_id, server_name, queryid, database_id, user_id, toplevel,
     calls, total_exec_time_ms, max_exec_time_ms, rows_returned, shared_blks_hit, shared_blks_read,
     temp_blks_read, temp_blks_written, wal_bytes, delta_calls, delta_total_exec_time_ms, delta_rows, sample_interval_seconds)
SELECT $5 + (ROW_NUMBER() OVER ()),
       c.at, $1, $2,
       s.queryid, s.database_id, 10, TRUE,
       0, 0, 50.0, 10, 10, 5, 0, 0, 0,
       s.calls,
       CASE WHEN c.i % 50 = 0 THEN s.ms * 0 ELSE s.ms END,
       s.calls,
       900
FROM (
    SELECT g.i, $3::timestamp + g.i * INTERVAL '15 minutes' AS at
    FROM generate_series(0, (EXTRACT(EPOCH FROM ($4::timestamp - $3::timestamp)) / 900)::int) AS g(i)
) AS c
CROSS JOIN LATERAL (VALUES
    ($6::bigint,  16384::bigint, (10 + c.i % 7)::bigint,                                   (100 + (c.i * 37) % 50)::bigint),
    ($6,          16385,         (1 + c.i % 3)::bigint,                                    (5 + (c.i * 11) % 13)::bigint),
    ($7,          16384,         CASE WHEN c.i % 3 = 0 THEN 0 ELSE 5 + c.i % 4 END::bigint, CASE WHEN c.i % 3 = 0 THEN 0 ELSE (c.i * 13) % 200 END::bigint),
    ($8,          16384,         (c.i % 4)::bigint,                                        CASE WHEN c.i % 11 = 0 THEN NULL ELSE 3 + (c.i * 7) % 29 END::bigint),
    ($9,          16384,         (2 + c.i % 5)::bigint,                                    (20 + (c.i * 3) % 41)::bigint),
    ($10,         16384,         (1 + c.i % 2)::bigint,                                    (60 + (c.i * 19) % 90)::bigint),
    ($11,         16384,         20::bigint,                                               (400 + (c.i * 17) % 300)::bigint),
    (NULL,        16384,         3::bigint,                                                CASE WHEN c.i % 7 = 0 THEN NULL ELSE 5 END::bigint)
) AS s(queryid, database_id, calls, ms)
WHERE (s.queryid IS DISTINCT FROM $9 OR EXTRACT(HOUR FROM c.at) BETWEEN 9 AND 17)
AND   (s.queryid IS DISTINCT FROM $10 OR c.at < $3::timestamp + INTERVAL '8 days')", connection) { CommandTimeout = 300 };
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(from);
        command.Parameters.AddWithValue(to);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(StatementA);
        command.Parameters.AddWithValue(StatementB);
        command.Parameters.AddWithValue(StatementC);
        command.Parameters.AddWithValue(StatementD);
        command.Parameters.AddWithValue(StatementE);
        command.Parameters.AddWithValue(Other);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantCaptureAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime at, long queryId, string planHash, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_plan_capture
    (collection_id, collection_time, server_id, server_name, query_id, plan_hash, duration_ms, node_count, top_node_type, plan_json)
VALUES ($1, $2, $3, $4, $5, $6, 25.0, 5, 'Hash Join', '{""Plan"": {""Node Type"": ""redacted""}}')", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(queryId);
        command.Parameters.AddWithValue(planHash);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, int serverId, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_statement_stats WHERE server_id = {serverId}; " +
            $"DELETE FROM pg_plan_capture WHERE server_id = {serverId}; " +
            $"DELETE FROM pg_database_stats WHERE server_id = {serverId};", connection) { CommandTimeout = 300 };
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
