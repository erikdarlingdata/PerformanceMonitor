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
/// #3653 item 12 (Q6): hour-of-week baseline buckets key on the TARGET's local clock, not on the UTC the
/// collectors stamp <c>collection_time</c> with. Erik's ruling, verbatim: "Hour-of-week buckets key on the
/// target's local clock; existing baselines re-bucket once. The UTC smear is the DST defect itself."
///
/// <para>Three layers, in the order a reader should trust them. (1) The pure math in the shared
/// <see cref="BaselineLocalClock"/> — the fixtures Erik asked for: a UTC−5 server with rows at Tuesday 23:00Z is
/// Tuesday 18h; Wednesday 03:00Z at UTC−5 is TUESDAY 22h (the dow is the local date's); a 30-day window straddling
/// the 2026-03-08 spring-forward puts Tue 2026-02-24 22:00Z (EST) and Tue 2026-03-10 21:00Z (EDT) in the SAME
/// Tue-17h bucket, where UTC keying split them 22h/21h — that split IS the smear the ruling names; a NULL zone id
/// degrades to the fixed offset, an unresolvable one logs once and does the same. (2) The census: every bucket
/// statement either ends in the ONE scaffold or extracts from the ONE local-time expression, and no bare
/// <c>EXTRACT(… FROM collection_time)</c> survives anywhere — necessary because NEITHER engine complains when a
/// statement ignores <c>$4..$6</c> (measured on PostgreSQL 17 through Npgsql 9 and on DuckDB 1.5.5: a statement
/// referencing only <c>$1..$3</c> with six bound runs), so an arm that bypassed the scaffold would key on UTC
/// silently. (3) The live proof (<c>DARLING_TEST_PG</c>): the real <c>PgBaselineProvider</c> over the real
/// <c>cpu_utilization_stats</c> and <c>blocked_process_baseline</c> supplies, with a <c>server_properties</c> row
/// carrying the zone id, lands the EST and EDT rows in one bucket and looks the analysis time up in it.</para>
/// </summary>
public sealed class LocalClockBucketKeyTests
{
    /* US Eastern, 2026: spring-forward Sunday 2026-03-08 07:00Z (−05:00 → −04:00), fall-back Sunday 2026-11-01 06:00Z. */
    private const string EasternWindowsId = "Eastern Standard Time";
    private const string EasternIanaId = "America/New_York";
    private static readonly DateTime SpringForward2026 = new(2026, 3, 8, 7, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime FallBack2026 = new(2026, 11, 1, 6, 0, 0, DateTimeKind.Unspecified);

    /* Sunday=0 — EXTRACT(DOW) in both engines and System.DayOfWeek agree. */
    private const int Tuesday = (int)DayOfWeek.Tuesday;

    /* ───────────────────────── (1) the pure math ───────────────────────── */

    [Fact]
    public void FixedOffset_UtcMinus5_TuesdayTwentyThreeZ_IsTuesdayEighteenLocal()
    {
        var clock = new BaselineLocalClock().Resolve(timeZoneId: null, utcOffsetMinutes: -300,
            windowStartUtc: new DateTime(2026, 1, 20), windowEndUtc: new DateTime(2026, 2, 19));

        Assert.False(clock.HasTransition);
        Assert.Equal(-300, clock.OffsetBeforeMinutes);
        Assert.Equal(-300, clock.OffsetAfterMinutes);
        Assert.Equal((18, Tuesday), clock.LocalKey(new DateTime(2026, 2, 17, 23, 0, 0)));

        /* The finding's baseline_bucket metadata is the bucket's key through BaselineContextFormatter — the same
           two fields PgAnomalyDetector copies from the bucket (baseline_hour, baseline_dow) — so it now reads the
           server's hour, not the collector's. */
        var (hour, dow) = clock.LocalKey(new DateTime(2026, 2, 17, 23, 0, 0));
        var context = BaselineContextFormatter.FormatBaselineContext(new Dictionary<string, double>
        {
            ["baseline_hour"] = hour,
            ["baseline_dow"] = dow,
        });
        Assert.NotNull(context);
        Assert.Equal("Tue 18:00", context!["bucket"]);
        Assert.NotEqual("Tue 23:00", context["bucket"]);
    }

    [Fact]
    public void DayRollover_WednesdayThreeZ_AtUtcMinus5_IsTuesdayTwentyTwo_FromTheLocalDate()
    {
        var clock = LocalClockWindow.FixedOffset(new DateTime(2026, 3, 20), -300);

        /* 2026-03-04 is a Wednesday; 03:00Z is 22:00 the previous evening at UTC−5. */
        Assert.Equal(DayOfWeek.Wednesday, new DateTime(2026, 3, 4).DayOfWeek);
        Assert.Equal((22, Tuesday), clock.LocalKey(new DateTime(2026, 3, 4, 3, 0, 0)));
        Assert.Equal(new DateTime(2026, 3, 3, 22, 0, 0), clock.ToLocal(new DateTime(2026, 3, 4, 3, 0, 0)));
    }

    [Theory]
    [InlineData(EasternWindowsId)]
    [InlineData(EasternIanaId)]
    public void DstStraddle_SpringForward_EstAndEdtRowsShareTheLocalBucket_WhereUtcSplitThem(string zoneId)
    {
        var analysisTime = new DateTime(2026, 3, 10, 21, 30, 0);
        var windowStart = analysisTime.AddDays(-BaselineMath.BaselineWindowDays);
        Assert.True(windowStart < SpringForward2026 && SpringForward2026 < analysisTime, "the window must straddle the transition");

        var clock = new BaselineLocalClock().Resolve(zoneId, utcOffsetMinutes: -300, windowStart, analysisTime);

        Assert.True(clock.HasTransition);
        Assert.Equal(SpringForward2026, clock.TransitionAtUtc);
        Assert.Equal(DateTimeKind.Unspecified, clock.TransitionAtUtc.Kind); // Npgsql maps it to `timestamp` like the bounds
        Assert.Equal(-300, clock.OffsetBeforeMinutes);
        Assert.Equal(-240, clock.OffsetAfterMinutes);

        var estRow = new DateTime(2026, 2, 24, 22, 0, 0); // Tue, before the change: 17:00 EST
        var edtRow = new DateTime(2026, 3, 10, 21, 0, 0); // Tue, after the change: 17:00 EDT
        Assert.Equal((17, Tuesday), clock.LocalKey(estRow));
        Assert.Equal((17, Tuesday), clock.LocalKey(edtRow));
        Assert.Equal(clock.LocalKey(estRow), clock.LocalKey(edtRow));

        /* The smear the ruling names: on UTC these two rows were different buckets. */
        Assert.NotEqual(estRow.Hour, edtRow.Hour);
        Assert.Equal((22, Tuesday), LocalClockWindow.Utc(analysisTime).LocalKey(estRow));
        Assert.Equal((21, Tuesday), LocalClockWindow.Utc(analysisTime).LocalKey(edtRow));

        /* One hour later on the EDT side is the NEXT local bucket, not a second sample of 17h. */
        Assert.Equal((18, Tuesday), clock.LocalKey(new DateTime(2026, 3, 10, 22, 0, 0)));

        /* And the lookup the provider makes for the analysis time itself. */
        Assert.Equal((17, Tuesday), clock.LocalKey(analysisTime));
    }

    [Fact]
    public void DstStraddle_FallBack_IsTheSameStepFunctionTheOtherWay()
    {
        var analysisTime = new DateTime(2026, 11, 3, 21, 30, 0);
        var clock = new BaselineLocalClock().Resolve(EasternWindowsId, -240, analysisTime.AddDays(-30), analysisTime);

        Assert.Equal(FallBack2026, clock.TransitionAtUtc);
        Assert.Equal(-240, clock.OffsetBeforeMinutes);
        Assert.Equal(-300, clock.OffsetAfterMinutes);
        /* A row stamped exactly AT the transition is on the new side — the SQL's `< $4 THEN before` agrees. */
        Assert.Equal(-300, clock.OffsetMinutesAt(FallBack2026));
        Assert.Equal(-240, clock.OffsetMinutesAt(FallBack2026.AddSeconds(-1)));
    }

    [Fact]
    public void NoTransitionInWindow_ZoneResolvesToOneOffset_BeforeEqualsAfter()
    {
        var analysisTime = new DateTime(2026, 7, 15, 12, 0, 0);
        var clock = new BaselineLocalClock().Resolve(EasternWindowsId, -300 /* a stale winter snapshot */, analysisTime.AddDays(-30), analysisTime);

        Assert.False(clock.HasTransition);
        /* The ZONE's answer, not the snapshot's: July is EDT whatever the stored offset says. */
        Assert.Equal(-240, clock.OffsetBeforeMinutes);
        Assert.Equal(-240, clock.OffsetAfterMinutes);
        Assert.Equal(analysisTime, clock.TransitionAtUtc);
    }

    [Fact]
    public void NullZoneAndNullOffset_IsUtcKeying_ThePreQ6Behaviour()
    {
        var analysisTime = new DateTime(2026, 3, 10, 21, 30, 0);
        var clock = new BaselineLocalClock().Resolve(null, null, analysisTime.AddDays(-30), analysisTime);

        Assert.Equal(LocalClockWindow.Utc(analysisTime), clock);
        Assert.Equal((21, Tuesday), clock.LocalKey(analysisTime));
    }

    [Fact]
    public void UnresolvableZoneId_FallsBackToTheStoredOffset_AndNotesItOnce()
    {
        var notes = new List<string>();
        var resolver = new BaselineLocalClock(notes.Add);
        var analysisTime = new DateTime(2026, 3, 10, 21, 30, 0);

        var first = resolver.Resolve("Not/A_Zone", -300, analysisTime.AddDays(-30), analysisTime);
        var second = resolver.Resolve("Not/A_Zone", -300, analysisTime.AddDays(-29), analysisTime.AddDays(1));

        Assert.Equal(LocalClockWindow.FixedOffset(analysisTime, -300), first);
        Assert.False(second.HasTransition);
        Assert.Equal(-300, second.OffsetAfterMinutes);

        var note = Assert.Single(notes);
        Assert.Contains("'Not/A_Zone' is not resolvable on this host", note, StringComparison.Ordinal);
        Assert.Contains("using the offset in force at the snapshot (UTC offset -05:00)", note, StringComparison.Ordinal);
        Assert.Contains("off by an hour across a DST transition", note, StringComparison.Ordinal);

        /* A different unresolvable id is a different statement about the host. */
        resolver.Resolve("Also/Not_A_Zone", 60, analysisTime.AddDays(-30), analysisTime);
        Assert.Equal(2, notes.Count);
    }

    [Fact]
    public void WindowThatEndsBeforeItStarts_IsRefused()
        => Assert.Throws<ArgumentException>(() => new BaselineLocalClock().Resolve(null, 0, new DateTime(2026, 3, 10), new DateTime(2026, 3, 9)));

    /* ───────────────────────── (2) the census ───────────────────────── */

    /// <summary>
    /// Every metric name the shared model declares, so a new arm in EITHER provider is censused the day it is
    /// declared and not the day someone remembers.
    /// </summary>
    private static IEnumerable<string> AllDeclaredMetricNames() =>
        typeof(MetricNames).GetFields(BindingFlags.Public | BindingFlags.Static | BindingFlags.FlattenHierarchy)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string))
            .Select(f => (string)f.GetRawConstantValue()!);

    private static IEnumerable<(string Owner, string Metric, string Sql)> EveryBucketStatement()
    {
        foreach (var metric in AllDeclaredMetricNames())
        {
            if (PgBaselineProvider.GetBaselineQuery(metric) is { } sql) yield return ("PgBaselineProvider", metric, sql);
            if (PgBaselineProvider.GetLegacyBaselineQuery(metric) is { } legacy) yield return ("PgBaselineProvider.Legacy", metric, legacy);
            if (PgTargetBaselineProvider.GetPgTargetBaselineQuery(metric) is { } pg) yield return ("PgTargetBaselineProvider", metric, pg);
        }
    }

    [Fact]
    public void TheLocalTimeExpression_IsTheSharedOne_AndBindsTheThreeClockParameters()
    {
        Assert.Equal(BaselineLocalClock.LocalCollectionTimeSql, PgBaselineProvider.LocalCollectionTime);
        Assert.Equal(
            "(collection_time + (CASE WHEN collection_time < $4 THEN $5 ELSE $6 END) * INTERVAL '1' MINUTE)",
            BaselineLocalClock.LocalCollectionTimeSql);

        /* hh, dw AND the distinct-day date all come off the shifted time — a scaffold that shifted the hour but
           dated the UTC day would count the wrong days for an evening bucket. */
        Assert.Equal(3, Regex.Matches(PgBaselineProvider.RobustTierScaffold, Regex.Escape(PgBaselineProvider.LocalCollectionTime)).Count);
        Assert.Contains("EXTRACT(HOUR FROM " + PgBaselineProvider.LocalCollectionTime + ")::INT AS hh", PgBaselineProvider.RobustTierScaffold, StringComparison.Ordinal);
        Assert.Contains("EXTRACT(DOW FROM " + PgBaselineProvider.LocalCollectionTime + ")::INT AS dw", PgBaselineProvider.RobustTierScaffold, StringComparison.Ordinal);
        Assert.Contains(PgBaselineProvider.LocalCollectionTime + "::DATE AS d", PgBaselineProvider.RobustTierScaffold, StringComparison.Ordinal);
    }

    /// <summary>
    /// The enforcement the engines do not provide. Every statement <c>ComputeBucketsAsync</c> can run — the SQL
    /// Server arms, the legacy arms, and every PostgreSQL-target arm the derived provider answers with — keys on
    /// the local-time expression: by ending in the one scaffold, or (the two event-family arms) by extracting from
    /// it by hand. And no arm anywhere still extracts from bare <c>collection_time</c>, which would run without
    /// complaint and key on UTC.
    /// </summary>
    [Fact]
    public void EveryBucketStatement_KeysOnTheLocalClock_AndNoneOnBareCollectionTime()
    {
        var statements = EveryBucketStatement().ToList();
        Assert.True(statements.Count >= 17, $"the census found only {statements.Count} bucket statements — an arm went missing");

        var bareKey = new Regex(@"EXTRACT\s*\(\s*(HOUR|DOW)\s+FROM\s+collection_time\s*\)", RegexOptions.IgnoreCase);
        var bareDate = new Regex(@"(?<![\w.])collection_time::DATE", RegexOptions.IgnoreCase);
        var ownExtract = new HashSet<string>(StringComparer.Ordinal) { MetricNames.Blocking, MetricNames.Deadlock };

        foreach (var (owner, metric, sql) in statements)
        {
            var endsInScaffold = sql.EndsWith(PgBaselineProvider.RobustTierScaffold, StringComparison.Ordinal);
            var extractsLocal = sql.Contains("EXTRACT(HOUR FROM " + PgBaselineProvider.LocalCollectionTime + ")", StringComparison.Ordinal)
                             && sql.Contains("EXTRACT(DOW FROM " + PgBaselineProvider.LocalCollectionTime + ")", StringComparison.Ordinal);

            Assert.True(endsInScaffold || extractsLocal, $"{owner}[{metric}] keys on neither the scaffold nor the local-time expression:\n{sql}");
            Assert.Equal(ownExtract.Contains(metric) && owner == "PgBaselineProvider", !endsInScaffold);
            Assert.DoesNotMatch(bareKey, sql);
            Assert.DoesNotMatch(bareDate, sql);
            Assert.Contains("$3", sql, StringComparison.Ordinal);
            foreach (var parameter in new[] { "$4", "$5", "$6" })
                Assert.Contains(parameter, sql, StringComparison.Ordinal);
        }

        /* The two hand-extracting arms shift the DATE the per-day mean divides by, too. */
        foreach (var metric in ownExtract)
        {
            var sql = PgBaselineProvider.GetBaselineQuery(metric)!;
            Assert.Equal(3, Regex.Matches(sql, Regex.Escape(PgBaselineProvider.LocalCollectionTime + "::DATE")).Count);
        }
    }

    /// <summary>
    /// The provider text still ends the way the pre-Q6 pins say it does: the same scaffold appended to every
    /// arm, the same bindings — the change is INSIDE the scaffold, which is what lets every derived arm inherit
    /// it. Pinned so a future "simplification" that moves the key back into the arms shows up here.
    /// </summary>
    [Fact]
    public void TheClockRead_IsTheStoresIdiom_AndLiteCarriesTheSameTextOverItsView()
    {
        var pg = PgBaselineProvider.ServerClockSql;
        Assert.Contains("SELECT utc_offset_minutes, time_zone_id", pg, StringComparison.Ordinal);
        Assert.Contains("FROM server_properties", pg, StringComparison.Ordinal);
        Assert.Contains("utc_offset_minutes IS NOT NULL", pg, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time DESC", pg, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", pg, StringComparison.Ordinal);
        Assert.Contains("server_id = $1", pg, StringComparison.Ordinal);

        /* Lite: the same statement over v_server_properties, one token apart — the same lockstep the finding
           stores' recurrence read keeps. */
        var lite = RepoFile.ReadRepoFileLf("Lite", "Analysis", "BaselineProvider.cs");
        var expectedLite = pg.Replace("\r\n", "\n", StringComparison.Ordinal).Replace("FROM server_properties", "FROM v_server_properties", StringComparison.Ordinal);
        Assert.Contains(expectedLite, lite, StringComparison.Ordinal);
    }

    /// <summary>
    /// Lite's scaffold and event arms went through the identical substitution: the SAME <c>keyed</c> CTE text
    /// (the tier math below it differs by dialect — DuckDB's native median()/mad() — and always has) and the SAME
    /// alias to the SAME shared constant. Source pin, because Darling.Tests cannot reference the Lite assembly.
    /// </summary>
    [Fact]
    public void LiteScaffoldAndEventArms_TookTheIdenticalSubstitution()
    {
        var lite = RepoFile.ReadRepoFileLf("Lite", "Analysis", "BaselineProvider.cs");
        var darlingSource = RepoFile.ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Analysis", "PgBaselineProvider.cs");

        /* Source against source: the compiled const has the alias EXPANDED, the two files spell it as
           `" + LocalCollectionTime + @"` — so the pin reads the Darling SOURCE's keyed CTE and finds it in Lite's. */
        var keyedStart = darlingSource.IndexOf("keyed AS (\n    SELECT v,", StringComparison.Ordinal);
        var keyedEnd = darlingSource.IndexOf("    FROM clean\n)", keyedStart, StringComparison.Ordinal);
        Assert.True(keyedStart >= 0 && keyedEnd > keyedStart, "the Darling scaffold's keyed CTE moved");
        var keyedCte = darlingSource[keyedStart..keyedEnd];
        Assert.Contains("EXTRACT(HOUR FROM \" + LocalCollectionTime + @\")::INT AS hh", keyedCte, StringComparison.Ordinal);
        Assert.Contains("EXTRACT(DOW FROM \" + LocalCollectionTime + @\")::INT AS dw", keyedCte, StringComparison.Ordinal);
        Assert.Contains("\" + LocalCollectionTime + @\"::DATE AS d", keyedCte, StringComparison.Ordinal);
        Assert.Contains(keyedCte, lite, StringComparison.Ordinal);

        Assert.Contains("internal const string LocalCollectionTime = BaselineLocalClock.LocalCollectionTimeSql;", lite, StringComparison.Ordinal);
        Assert.DoesNotMatch(new Regex(@"EXTRACT\s*\(\s*(HOUR|DOW)\s+FROM\s+collection_time\s*\)", RegexOptions.IgnoreCase), lite);
        Assert.DoesNotMatch(new Regex(@"(?<![\w.])collection_time::DATE"), lite);
        /* Two event arms × (hour, dow, three dates) = 10 hand references, plus the scaffold's 3, plus the alias's own line. */
        Assert.True(Regex.Matches(lite, @"\+ LocalCollectionTime \+").Count >= 13, "Lite's event arms or scaffold lost a LocalCollectionTime reference");

        /* And both providers bind the three clock parameters after the window bounds, in the same order. */
        var darling = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgBaselineProvider.cs");
        foreach (var (source, spelling) in new[]
        {
            (darling, "cmd.Parameters.AddWithValue(AsNaive(clock.TransitionAtUtc));"),
            (darling, "cmd.Parameters.AddWithValue(clock.OffsetBeforeMinutes);"),
            (darling, "cmd.Parameters.AddWithValue(clock.OffsetAfterMinutes);"),
            (lite, "cmd.Parameters.Add(new DuckDBParameter { Value = clock.TransitionAtUtc });"),
            (lite, "cmd.Parameters.Add(new DuckDBParameter { Value = clock.OffsetBeforeMinutes });"),
            (lite, "cmd.Parameters.Add(new DuckDBParameter { Value = clock.OffsetAfterMinutes });"),
            (darling, "cached.Clock.LocalKey(analysisTime)"),
            (lite, "cached.Clock.LocalKey(analysisTime)"),
        })
        {
            Assert.Contains(spelling, source, StringComparison.Ordinal);
        }
        /* The old lookup is gone from both: `analysisTime.Hour` as a key was the C# half of the smear. */
        Assert.DoesNotContain("var hourOfDay = analysisTime.Hour;", darling, StringComparison.Ordinal);
        Assert.DoesNotContain("var hourOfDay = analysisTime.Hour;", lite, StringComparison.Ordinal);
    }

}

/// <summary>
/// (3) The live proof for <see cref="LocalClockBucketKeyTests"/>, in the <c>live-postgres</c> collection so the
/// shared store is established before it runs and the residue check runs after it (#1862, #1873).
/// </summary>
[Collection("live-postgres")]
public sealed class LocalClockBucketKeyLiveTests
{
    private const string EasternWindowsId = "Eastern Standard Time";
    private const int Tuesday = (int)DayOfWeek.Tuesday;

    /// <summary>
    /// The real provider over the real supplies, with a <c>server_properties</c> row carrying the zone id: CPU
    /// (the scaffold path, reading the raw hypertable) and Blocking (the hand-EXTRACT six-column path, reading the
    /// <c>blocked_process_baseline</c> supply through the plain fallback view). Six CPU rows at Tue 22:00Z before the
    /// spring-forward and six at Tue 21:00Z after it — 17:00 on the server both times — become ONE Tue-17h bucket of
    /// twelve samples over two distinct local days, and the analysis time 2026-03-10 21:30Z looks that bucket up.
    /// Under the pre-Q6 text the same rows were two six-sample buckets (22h, 21h) and the lookup read the 21h one.
    /// A row at Wed 03:00Z lands in Tue 22h with a TUESDAY date. Two blocking reports at the same pair of instants
    /// are one Tue-17h event bucket of 2 events over 2 days — mean 1.0/day, not two 1-day buckets.
    /// </summary>
    [Fact]
    public async Task Live_TheProvider_KeysCpuAndBlockingOnTheServersClock_AcrossTheSpringForward()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live local-clock baseline test.");

        var ct = TestContext.Current.CancellationToken;
        const int serverId = -3653_12;
        const string serverName = "q6-local-clock-e2e";

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupAsync(connection, serverId, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            var analysisTime = new DateTime(2026, 3, 10, 21, 30, 0, DateTimeKind.Unspecified);
            var estTuesday = new DateTime(2026, 2, 24, 22, 0, 0, DateTimeKind.Unspecified); // 17:00 EST
            var edtTuesday = new DateTime(2026, 3, 10, 21, 0, 0, DateTimeKind.Unspecified); // 17:00 EDT
            var wednesdayThreeZ = new DateTime(2026, 3, 4, 3, 0, 0, DateTimeKind.Unspecified); // Tue 22:00 EST

            /* The server's clock as the on-load collector stores it: the offset in force at the snapshot and the
               engine's own zone id. The snapshot is from the EST side; the zone knows the window crosses over. */
            await ExecAsync(connection, ct,
                "INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, utc_offset_minutes, time_zone_id) VALUES ($1, $2, $3, $4, $5, $6)",
                CollectionIdGenerator.Next(), estTuesday, serverId, serverName, -300, EasternWindowsId);

            for (var i = 0; i < 6; i++)
            {
                foreach (var (at, cpuPercent) in new[] { (estTuesday.AddMinutes(i * 5), 40 + i), (edtTuesday.AddMinutes(i * 5), 60 + i) })
                {
                    await ExecAsync(connection, ct,
                        "INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                        CollectionIdGenerator.Next(), at, serverId, serverName, at, cpuPercent, 5);
                }
            }
            /* Ten rows across 03:00Z–03:45Z — every one of them 22:xx on Tuesday the 3rd at UTC−5 — so the bucket
               clears BaselineMath.CollapseThreshold and SelectBucket returns IT rather than collapsing to the flat
               tier and hiding which hour the rows were keyed to. */
            for (var i = 0; i < 10; i++)
            {
                var at = wednesdayThreeZ.AddMinutes(i * 5);
                await ExecAsync(connection, ct,
                    "INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                    CollectionIdGenerator.Next(), at, serverId, serverName, at, 99, 5);
            }

            foreach (var at in new[] { estTuesday, edtTuesday })
            {
                await ExecAsync(connection, ct,
                    "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, database_name, blocked_spid, blocking_spid, wait_time_ms, lock_mode, blocked_sql_text, blocking_sql_text, blocked_process_report_xml, contentious_object) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
                    CollectionIdGenerator.Next(), at, serverId, serverName, at, "appdb", 61, 62, 5000L, "X", "UPDATE t", "UPDATE t", "<blocked-process-report/>", "dbo.t");
            }

            /* The blocking supply must exist — plain fallback views, as the sibling tests do. */
            await TimescaleSupport.EnsureBaselineFallbackViewsAsync(connection, null, ct);

            var provider = new PgBaselineProvider(postgres);

            var cpu = await provider.GetBaselineAsync(serverId, MetricNames.Cpu, analysisTime, ct);
            Assert.Equal(BaselineTier.Full, cpu.Tier);
            Assert.Equal(17, cpu.HourOfDay);
            Assert.Equal(Tuesday, cpu.DayOfWeek);
            Assert.Equal(12L, cpu.SampleCount);       // both Tuesdays, one bucket
            Assert.Equal(2L, cpu.DistinctDays);       // two LOCAL Tuesdays
            Assert.Equal((40 + 41 + 42 + 43 + 44 + 45 + 60 + 61 + 62 + 63 + 64 + 65) / 12.0, cpu.Mean, 0.001);

            /* And the label the finding would carry. */
            var label = BaselineContextFormatter.FormatBaselineContext(new Dictionary<string, double>
            {
                ["baseline_hour"] = cpu.HourOfDay,
                ["baseline_dow"] = cpu.DayOfWeek,
            });
            Assert.Equal("Tue 17:00", label!["bucket"]);

            /* The rollover rows: Wednesday 03:xxZ is Tuesday 22h on the server, dated TUESDAY. A lookup at
               Wednesday 03:50Z (after the last row — the window end is exclusive) asks for (22, Tue) and finds the ten rows there — not at UTC's (3, Wed), which
               holds nothing. */
            provider.ClearCache();
            var rollover = await provider.GetBaselineAsync(serverId, MetricNames.Cpu, new DateTime(2026, 3, 4, 3, 50, 0, DateTimeKind.Unspecified), ct);
            Assert.Equal(BaselineTier.Full, rollover.Tier);
            Assert.Equal(22, rollover.HourOfDay);
            Assert.Equal(Tuesday, rollover.DayOfWeek);
            Assert.Equal(10L, rollover.SampleCount);
            Assert.Equal(1L, rollover.DistinctDays);
            Assert.Equal(99.0, rollover.Mean, 0.001);

            /* The hand-EXTRACT arm, read at the SQL level. The event family counts DAYS as samples (a 30-day
               window holds at most five Tuesdays), so SelectBucket always collapses it and a provider-level read
               cannot show which key the rows took — the arm's real text with the provider's real bindings can.
               Local clock: ONE (17, Tue) row, two events over two days. UTC ($5 = $6 = 0): the same two events
               are two one-day rows, (22, Tue) and (21, Tue) — the smear, in the six-column shape. */
            var blockingSql = PgBaselineProvider.GetBaselineQuery(MetricNames.Blocking)!;
            var local = await ReadEventRowsAsync(connection, blockingSql, serverId, analysisTime, new DateTime(2026, 3, 8, 7, 0, 0, DateTimeKind.Unspecified), -300, -240, ct);
            var row = Assert.Single(local);
            Assert.Equal((17, Tuesday), (row.Hour, row.Dow));
            Assert.Equal(2L, row.DistinctDays);
            Assert.Equal(1.0, row.Mean, 0.001);

            var utc = await ReadEventRowsAsync(connection, blockingSql, serverId, analysisTime, analysisTime, 0, 0, ct);
            Assert.Equal(2, utc.Count);
            Assert.Contains(utc, r => r.Hour == 22 && r.Dow == Tuesday && r.DistinctDays == 1);
            Assert.Contains(utc, r => r.Hour == 21 && r.Dow == Tuesday && r.DistinctDays == 1);

            /* And the provider still serves the family without complaint (flat, as the day-count thresholds demand). */
            provider.ClearCache();
            var blocking = await provider.GetBaselineAsync(serverId, MetricNames.Blocking, analysisTime, ct);
            Assert.NotEqual(BaselineTier.Full, blocking.Tier);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await CleanupAsync(cleanup, serverId, cleanupCt);
                foreach (var (_, view) in TimescaleSupport.BaselineAggregates)
                {
                    using var drop = new NpgsqlCommand(TimescaleSupport.DropBaselineFallbackViewSql(view), cleanup);
                    await drop.ExecuteNonQueryAsync(cleanupCt);
                }
            });
        }
    }

    private static async Task<List<(int Hour, int Dow, double Mean, long DistinctDays)>> ReadEventRowsAsync(
        NpgsqlConnection connection, string sql, int serverId, DateTime analysisTime, DateTime transitionAtUtc, int before, int after, System.Threading.CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(analysisTime.AddDays(-BaselineMath.BaselineWindowDays));
        command.Parameters.AddWithValue(analysisTime);
        command.Parameters.AddWithValue(transitionAtUtc);
        command.Parameters.AddWithValue(before);
        command.Parameters.AddWithValue(after);
        var rows = new List<(int, int, double, long)>();
        using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            rows.Add((Convert.ToInt32(reader.GetValue(0)), Convert.ToInt32(reader.GetValue(1)), Convert.ToDouble(reader.GetValue(2)), Convert.ToInt64(reader.GetValue(5))));
        return rows;
    }

    private static async Task CleanupAsync(NpgsqlConnection connection, int serverId, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM cpu_utilization_stats WHERE server_id = {serverId}; " +
            $"DELETE FROM blocked_process_reports WHERE server_id = {serverId}; " +
            $"DELETE FROM server_properties WHERE server_id = {serverId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct, string sql, params object[] values)
    {
        using var command = new NpgsqlCommand(sql, connection);
        foreach (var value in values)
            command.Parameters.AddWithValue(value);
        await command.ExecuteNonQueryAsync(ct);
    }
}
