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

}
