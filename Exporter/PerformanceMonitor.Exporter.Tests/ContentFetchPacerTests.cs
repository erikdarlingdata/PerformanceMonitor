using System.Reflection;
using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

public class ContentFetchPacerTests
{
    [Theory]
    [InlineData(0, 50)]
    [InlineData(10, 50)]
    [InlineData(50, 50)]
    [InlineData(75, 75)]
    [InlineData(500, 500)]
    [InlineData(2000, 2000)]
    [InlineData(3000, 2000)]
    [InlineData(10000, 2000)]
    public void SleepMsFor_clamps_between_50_and_2000(double callElapsedMs, int expected)
    {
        Assert.Equal(expected, ContentFetchPacer.SleepMsFor(callElapsedMs));
    }

    [Theory]
    [InlineData(10, 100)]   // clamp to floor 50, then doubled
    [InlineData(75, 150)]
    [InlineData(1000, 2000)]
    [InlineData(3000, 4000)] // clamp to ceiling 2000, then doubled
    public void SleepMsFor_doubles_when_allowCompressed_is_set(double callElapsedMs, int expected)
    {
        Assert.Equal(expected, ContentFetchPacer.SleepMsFor(callElapsedMs, allowCompressed: true));
    }

    [Fact]
    public async Task RunAsync_sleeps_for_the_calls_own_elapsed_time()
    {
        var elapsed = new Queue<long>([0, 300]); // 300 ticks elapsed if frequency == 1000/sec below isn't used directly; see clock below
        var ticks = 0L;
        long TimestampNow() => ticks;
        var sleeps = new List<int>();
        Task Sleep(int ms, CancellationToken ct)
        {
            sleeps.Add(ms);
            return Task.CompletedTask;
        }

        var pacer = new ContentFetchPacer(allowCompressed: false, timestampNow: TimestampNow, sleep: Sleep);

        // Simulate a call that takes exactly Stopwatch.Frequency/10 ticks (i.e. 100ms) by
        // advancing the fake clock inside the call body.
        var result = await pacer.RunAsync(() =>
        {
            ticks += System.Diagnostics.Stopwatch.Frequency / 10; // 100 ms worth of ticks
            return Task.FromResult(42);
        }, CancellationToken.None);

        Assert.Equal(42, result);
        Assert.Single(sleeps);
        Assert.Equal(100, sleeps[0]);
    }

    [Fact]
    public async Task RunAsync_clamps_a_fast_call_to_the_50ms_floor()
    {
        var ticks = 0L;
        long TimestampNow() => ticks;
        var sleeps = new List<int>();
        Task Sleep(int ms, CancellationToken ct)
        {
            sleeps.Add(ms);
            return Task.CompletedTask;
        }

        var pacer = new ContentFetchPacer(allowCompressed: false, timestampNow: TimestampNow, sleep: Sleep);

        await pacer.RunAsync(() => Task.FromResult(1), CancellationToken.None); // 0ms elapsed

        Assert.Equal(50, sleeps[0]);
    }

    [Fact]
    public async Task RunAsync_doubles_the_sleep_when_allowCompressed()
    {
        var ticks = 0L;
        long TimestampNow() => ticks;
        var sleeps = new List<int>();
        Task Sleep(int ms, CancellationToken ct)
        {
            sleeps.Add(ms);
            return Task.CompletedTask;
        }

        var pacer = new ContentFetchPacer(allowCompressed: true, timestampNow: TimestampNow, sleep: Sleep);

        await pacer.RunAsync(() =>
        {
            ticks += System.Diagnostics.Stopwatch.Frequency / 10; // 100 ms
            return Task.FromResult(1);
        }, CancellationToken.None);

        Assert.Equal(200, sleeps[0]);
    }

    [Fact]
    public void PgStatementTextExporter_PaceSleepMs_delegates_to_the_shared_pacer()
    {
        Assert.Equal(ContentFetchPacer.SleepMsFor(75), PgStatementTextExporter.PaceSleepMs(75));
        Assert.Equal(ContentFetchPacer.SleepMsFor(9999), PgStatementTextExporter.PaceSleepMs(9999));
    }

    /// <summary>
    /// Every SQL constant/builder in <see
    /// cref="SqlBuilder"/> tags its statement with a `/* darling-export:&lt;name&gt; */`
    /// comment, so pg_stat_statements deltas and any logged statement can be attributed to
    /// this exporter rather than confused with Darling's own traffic. This enumerates every
    /// `public const string` and every zero/one-arg builder that returns a SQL-shaped string
    /// and asserts the tag is present, so a future SqlBuilder addition without a tag fails
    /// this test instead of shipping untagged.
    /// </summary>
    [Fact]
    public void Every_SqlBuilder_statement_carries_the_exporter_tag()
    {
        var type = typeof(SqlBuilder);

        // Exemptions: not SQL text sent to the store as a full statement text on their own.
        var exempt = new HashSet<string>
        {
            nameof(SqlBuilder.QuoteIdentifier), // identifier fragment, not a statement
        };

        var missing = new List<string>();

        foreach (var field in type.GetFields(BindingFlags.Public | BindingFlags.Static).Where(f => f.FieldType == typeof(string)))
        {
            // Runs against the run-scoped DuckDB cache, never against the store -- no store
            // statement to tag (see the H1-H3 class summaries in DimensionTableExporter /
            // ProcedurePlanExporter for why these exist only as DuckDB queries / test oracles).
            if (exempt.Contains(field.Name) || field.Name.EndsWith("DuckDbSql", StringComparison.Ordinal))
            {
                continue;
            }

            var value = (string)field.GetValue(null)!;
            if (!value.Contains("/* darling-export:", StringComparison.Ordinal))
            {
                missing.Add($"field {field.Name}");
            }
        }

        // Zero-argument builder methods that return SQL text directly (not via a switch that
        // just returns one of the already-checked constants).
        Assert.Contains("/* darling-export:", SqlBuilder.BuildQueryStoreDedupSql("query_store_raw"), StringComparison.Ordinal);
        Assert.Contains("/* darling-export:", SqlBuilder.BuildRawSelect("perfmon_stats", ["collection_time"]), StringComparison.Ordinal);

        var optionalTable = OptionalTables.All.First(t => t.Kind == OptionalTableKind.Window);
        Assert.Contains("/* darling-export:", SqlBuilder.BuildOptionalSelect(optionalTable, [optionalTable.TimeColumn!]), StringComparison.Ordinal);

        Assert.Empty(missing);
    }
}
