using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3653 item 12 (Q6), the Lite half: hour-of-week baseline buckets key on the TARGET's local clock, not on the
/// UTC the collectors stamp <c>collection_time</c> with. Erik's ruling, verbatim: "Hour-of-week buckets key on
/// the target's local clock; existing baselines re-bucket once. The UTC smear is the DST defect itself."
///
/// <para>Three layers. (1) The shared pure math (<see cref="BaselineLocalClock"/>), the same fixtures
/// Darling.Tests runs — the resolver is ONE file both SKUs call, and it executes in both test assemblies so a
/// host difference (ICU, tzdata) shows up wherever it is. (2) The Lite census: every arm of the real
/// <c>BaselineProvider.GetBaselineQuery</c> keys on the one local-time expression and none on bare
/// <c>collection_time</c> — necessary because DuckDB does not reject a statement that ignores <c>$4..$6</c>
/// (measured on 1.5.5), so a bypassing arm would key on UTC silently. (3) The real provider over the real DuckDB
/// schema: a <c>server_properties</c> row at UTC−5 puts Tuesday-23:00Z CPU rows in the Tuesday-18h bucket and the
/// finding label reads "Tue 18:00"; with the zone id, EST and EDT rows across the 2026 spring-forward share one
/// bucket where UTC split them.</para>
/// </summary>
public class LocalClockBucketKeyTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string EasternWindowsId = "Eastern Standard Time";
    private static readonly DateTime SpringForward2026 = new(2026, 3, 8, 7, 0, 0, DateTimeKind.Unspecified);
    private const int Tuesday = (int)DayOfWeek.Tuesday;

    private const int ServerId = -3653_12;
    private readonly DuckDbInitializer _duckDb;
    private readonly BaselineProvider _provider;
    private DuckDBConnection? _seedConn;
    private long _nextId = -1;

    public LocalClockBucketKeyTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _provider = new BaselineProvider(_duckDb);
        BaselineProvider.CacheTtl = TimeSpan.FromMilliseconds(1);
    }

    public void Dispose() => _seedConn?.Dispose();

    /* ───────────────────────── (1) the pure math ───────────────────────── */

    [Fact]
    public void FixedOffset_UtcMinus5_TuesdayTwentyThreeZ_IsTuesdayEighteenLocal_AndTheLabelSaysSo()
    {
        var clock = new BaselineLocalClock().Resolve(null, -300, new DateTime(2026, 1, 20), new DateTime(2026, 2, 19));

        Assert.False(clock.HasTransition);
        Assert.Equal((-300, -300), (clock.OffsetBeforeMinutes, clock.OffsetAfterMinutes));
        var (hour, dow) = clock.LocalKey(new DateTime(2026, 2, 17, 23, 0, 0));
        Assert.Equal((18, Tuesday), (hour, dow));

        var context = BaselineContextFormatter.FormatBaselineContext(new Dictionary<string, double>
        {
            ["baseline_hour"] = hour,
            ["baseline_dow"] = dow,
        });
        Assert.Equal("Tue 18:00", context!["bucket"]);
    }

    [Fact]
    public void DayRollover_WednesdayThreeZ_AtUtcMinus5_IsTuesdayTwentyTwo_FromTheLocalDate()
        => Assert.Equal((22, Tuesday), LocalClockWindow.FixedOffset(new DateTime(2026, 3, 20), -300).LocalKey(new DateTime(2026, 3, 4, 3, 0, 0)));

    [Fact]
    public void DstStraddle_SpringForward_EstAndEdtRowsShareTheLocalBucket_WhereUtcSplitThem()
    {
        var analysisTime = new DateTime(2026, 3, 10, 21, 30, 0);
        var clock = new BaselineLocalClock().Resolve(EasternWindowsId, -300, analysisTime.AddDays(-BaselineMath.BaselineWindowDays), analysisTime);

        Assert.Equal(SpringForward2026, clock.TransitionAtUtc);
        Assert.Equal((-300, -240), (clock.OffsetBeforeMinutes, clock.OffsetAfterMinutes));

        var estRow = new DateTime(2026, 2, 24, 22, 0, 0);
        var edtRow = new DateTime(2026, 3, 10, 21, 0, 0);
        Assert.Equal((17, Tuesday), clock.LocalKey(estRow));
        Assert.Equal((17, Tuesday), clock.LocalKey(edtRow));
        Assert.Equal((18, Tuesday), clock.LocalKey(new DateTime(2026, 3, 10, 22, 0, 0)));
        Assert.Equal((17, Tuesday), clock.LocalKey(analysisTime));

        /* The smear: UTC keyed the same two rows 22h and 21h. */
        Assert.NotEqual(LocalClockWindow.Utc(analysisTime).LocalKey(estRow), LocalClockWindow.Utc(analysisTime).LocalKey(edtRow));
    }

    [Fact]
    public void NoTransition_NullZone_UnresolvableZone_AllDegradeHonestly()
    {
        var analysisTime = new DateTime(2026, 7, 15, 12, 0, 0);
        var summer = new BaselineLocalClock().Resolve(EasternWindowsId, -300, analysisTime.AddDays(-30), analysisTime);
        Assert.False(summer.HasTransition);
        Assert.Equal(-240, summer.OffsetAfterMinutes); // the zone's July answer, not the stale snapshot's

        Assert.Equal(LocalClockWindow.FixedOffset(analysisTime, -300), new BaselineLocalClock().Resolve(null, -300, analysisTime.AddDays(-30), analysisTime));
        Assert.Equal(LocalClockWindow.Utc(analysisTime), new BaselineLocalClock().Resolve(null, null, analysisTime.AddDays(-30), analysisTime));

        var notes = new List<string>();
        var resolver = new BaselineLocalClock(notes.Add);
        Assert.Equal(LocalClockWindow.FixedOffset(analysisTime, -300), resolver.Resolve("Not/A_Zone", -300, analysisTime.AddDays(-30), analysisTime));
        resolver.Resolve("Not/A_Zone", -300, analysisTime.AddDays(-30), analysisTime);
        var note = Assert.Single(notes);
        Assert.Contains("'Not/A_Zone' is not resolvable on this host", note, StringComparison.Ordinal);
        Assert.Contains("off by an hour across a DST transition", note, StringComparison.Ordinal);
    }

    /* ───────────────────────── (2) the census ───────────────────────── */

    private static IEnumerable<string> AllDeclaredMetricNames() =>
        typeof(MetricNames).GetFields(BindingFlags.Public | BindingFlags.Static | BindingFlags.FlattenHierarchy)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string))
            .Select(f => (string)f.GetRawConstantValue()!);

    [Fact]
    public void EveryLiteBucketStatement_KeysOnTheLocalClock_AndNoneOnBareCollectionTime()
    {
        Assert.Equal(BaselineLocalClock.LocalCollectionTimeSql, BaselineProvider.LocalCollectionTime);
        Assert.Equal(3, Regex.Matches(BaselineProvider.RobustTierScaffold, Regex.Escape(BaselineProvider.LocalCollectionTime)).Count);

        var statements = AllDeclaredMetricNames()
            .Select(m => (Metric: m, Sql: BaselineProvider.GetBaselineQuery(m)))
            .Where(t => t.Sql is not null)
            .ToList();
        Assert.Equal(11, statements.Count); // Lite's eleven arms — a twelfth arrives censused

        var bareKey = new Regex(@"EXTRACT\s*\(\s*(HOUR|DOW)\s+FROM\s+collection_time\s*\)", RegexOptions.IgnoreCase);
        var bareDate = new Regex(@"(?<![\w.])collection_time::DATE", RegexOptions.IgnoreCase);
        var ownExtract = new HashSet<string>(StringComparer.Ordinal) { MetricNames.Blocking, MetricNames.Deadlock };

        foreach (var (metric, sql) in statements)
        {
            var endsInScaffold = sql!.EndsWith(BaselineProvider.RobustTierScaffold, StringComparison.Ordinal);
            var extractsLocal = sql.Contains("EXTRACT(HOUR FROM " + BaselineProvider.LocalCollectionTime + ")", StringComparison.Ordinal)
                             && sql.Contains("EXTRACT(DOW FROM " + BaselineProvider.LocalCollectionTime + ")", StringComparison.Ordinal);

            Assert.True(endsInScaffold || extractsLocal, $"[{metric}] keys on neither the scaffold nor the local-time expression:\n{sql}");
            Assert.Equal(ownExtract.Contains(metric), !endsInScaffold);
            Assert.DoesNotMatch(bareKey, sql);
            Assert.DoesNotMatch(bareDate, sql);
            foreach (var parameter in new[] { "$3", "$4", "$5", "$6" })
                Assert.Contains(parameter, sql, StringComparison.Ordinal);
        }

        foreach (var metric in ownExtract)
            Assert.Equal(3, Regex.Matches(BaselineProvider.GetBaselineQuery(metric)!, Regex.Escape(BaselineProvider.LocalCollectionTime + "::DATE")).Count);

        Assert.Contains("FROM v_server_properties", BaselineProvider.ServerClockSql, StringComparison.Ordinal);
        Assert.Contains("utc_offset_minutes IS NOT NULL", BaselineProvider.ServerClockSql, StringComparison.Ordinal);
    }

    /* ───────────────────────── (3) the real provider over the real schema ───────────────────────── */

    /// <summary>
    /// Fixture 1 through the product: a server whose <c>server_properties</c> row says UTC−5 and nothing about a
    /// zone, twelve CPU rows on two Tuesdays at 23:xxZ. Pre-Q6 the bucket was (23, Tue) and the label "Tue 23:00";
    /// now the rows key to (18, Tue), the analysis time 23:30Z looks up (18, Tue), and the label is "Tue 18:00".
    /// The UTC-keyed lookup finds nothing at (18, Tue) — the same rows, the same store, the other key. The lookup is
    /// the NEXT Tuesday's: the window ends on the analysis hour (#3941), so the analysis day's own 23Z rows are never
    /// in it.
    /// </summary>
    [Fact]
    public async Task Provider_OffsetOnly_TuesdayTwentyThreeZRows_AreTheTuesdayEighteenBucket()
    {
        await SeedServerClockAsync(-300, timeZoneId: null);
        var tuesdays = new[] { new DateTime(2026, 2, 10, 23, 0, 0), new DateTime(2026, 2, 17, 23, 0, 0) };
        foreach (var tuesday in tuesdays)
            for (var i = 0; i < 6; i++)
                await SeedCpuAsync(tuesday.AddMinutes(i * 5), 50 + i);

        var analysisTime = new DateTime(2026, 2, 24, 23, 30, 0);
        var bucket = await _provider.GetBaselineAsync(ServerId, MetricNames.Cpu, analysisTime);

        Assert.Equal(BaselineTier.Full, bucket.Tier);
        Assert.Equal(18, bucket.HourOfDay);
        Assert.Equal(Tuesday, bucket.DayOfWeek);
        Assert.Equal(12L, bucket.SampleCount);
        Assert.Equal(2L, bucket.DistinctDays);

        var label = BaselineContextFormatter.FormatBaselineContext(new Dictionary<string, double>
        {
            ["baseline_hour"] = bucket.HourOfDay,
            ["baseline_dow"] = bucket.DayOfWeek,
        });
        Assert.Equal("Tue 18:00", label!["bucket"]);
    }

    /// <summary>
    /// Fixture 3 through the product, with the DST straddle: the zone id is present, the window crosses
    /// 2026-03-08 07:00Z, and rows at Tue 22:00Z (EST) and Tue 21:00Z (EDT) — both 17:00 on the server — are ONE
    /// twelve-sample (17, Tue) bucket over two local days. Rows at Wednesday 03:xxZ are (22, Tue), dated Tuesday.
    /// Both lookups are a week after the rows they find: the window ends on the analysis hour (#3941), so the analysis
    /// day's own rows in that hour are never in it.
    /// </summary>
    [Fact]
    public async Task Provider_ZoneId_EstAndEdtRowsAcrossTheSpringForward_ShareOneLocalBucket()
    {
        await SeedServerClockAsync(-300, EasternWindowsId);
        var estTuesday = new DateTime(2026, 2, 24, 22, 0, 0);
        var edtTuesday = new DateTime(2026, 3, 10, 21, 0, 0);
        var wednesdayThreeZ = new DateTime(2026, 3, 4, 3, 0, 0);
        for (var i = 0; i < 6; i++)
        {
            await SeedCpuAsync(estTuesday.AddMinutes(i * 5), 40 + i);
            await SeedCpuAsync(edtTuesday.AddMinutes(i * 5), 60 + i);
        }
        for (var i = 0; i < 10; i++)
            await SeedCpuAsync(wednesdayThreeZ.AddMinutes(i * 5), 99);

        var analysisTime = new DateTime(2026, 3, 17, 21, 30, 0);
        var seventeen = await _provider.GetBaselineAsync(ServerId, MetricNames.Cpu, analysisTime);
        Assert.Equal(BaselineTier.Full, seventeen.Tier);
        Assert.Equal((17, Tuesday), (seventeen.HourOfDay, seventeen.DayOfWeek));
        Assert.Equal(12L, seventeen.SampleCount);
        Assert.Equal(2L, seventeen.DistinctDays);
        Assert.Equal((40 + 41 + 42 + 43 + 44 + 45 + 60 + 61 + 62 + 63 + 64 + 65) / 12.0, seventeen.Mean, 0.001);

        _provider.ClearCache();
        var rollover = await _provider.GetBaselineAsync(ServerId, MetricNames.Cpu, new DateTime(2026, 3, 11, 2, 50, 0)); // Tue 22:50 EDT, the next Tuesday's 22h
        Assert.Equal(BaselineTier.Full, rollover.Tier);
        Assert.Equal((22, Tuesday), (rollover.HourOfDay, rollover.DayOfWeek));
        Assert.Equal(10L, rollover.SampleCount);
        Assert.Equal(1L, rollover.DistinctDays);
        Assert.Equal(99.0, rollover.Mean, 0.001);
    }

    /// <summary>
    /// The pre-Q6 posture, unchanged where it should be: a server with no <c>server_properties</c> row keys on UTC,
    /// exactly as every server did before — the 23:xxZ rows are the (23, Tue) bucket.
    /// </summary>
    [Fact]
    public async Task Provider_NoServerPropertiesRow_KeysOnUtc_AsBefore()
    {
        var tuesday = new DateTime(2026, 2, 17, 23, 0, 0);
        for (var i = 0; i < 10; i++)
            await SeedCpuAsync(tuesday.AddMinutes(i * 5), 50);

        var bucket = await _provider.GetBaselineAsync(ServerId, MetricNames.Cpu, tuesday.AddDays(7).AddMinutes(50)); // the next Tuesday's 23h: the window ends on the analysis hour (#3941)
        Assert.Equal((23, Tuesday), (bucket.HourOfDay, bucket.DayOfWeek));
        Assert.Equal(10L, bucket.SampleCount);
    }

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private async Task SeedCpuAsync(DateTime time, int cpuValue)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO cpu_utilization_stats
            (collection_id, collection_time, server_id, server_name, sample_time,
             sqlserver_cpu_utilization, other_process_cpu_utilization)
            VALUES ($1, $2, $3, 'TestServer', $4, $5, 2)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
        cmd.Parameters.Add(new DuckDBParameter { Value = time });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = time });
        cmd.Parameters.Add(new DuckDBParameter { Value = cpuValue });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>The server's clock as the on-load collector stores it (AsOfWindowAnchorTests' seed, plus v42's zone id).</summary>
    private async Task SeedServerClockAsync(int utcOffsetMinutes, string? timeZoneId)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO server_properties
            (collection_id, collection_time, server_id, server_name,
             edition, product_version, product_level, engine_edition,
             cpu_count, hyperthread_ratio, physical_memory_mb, utc_offset_minutes, time_zone_id)
            VALUES ($1, $2, $3, 'TestServer', 'Developer Edition', '16.0.4150.1', 'RTM', 3, 8, 1, 16384, $4, $5)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
        cmd.Parameters.Add(new DuckDBParameter { Value = new DateTime(2026, 1, 1) });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = utcOffsetMinutes });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)timeZoneId ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }
}
