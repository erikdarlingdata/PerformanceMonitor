using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

/// <summary>
/// Unit tests for the straggler guard's retry/wait loop (docs/hourly-export.md §1.5), using
/// the count-injected overload so no database and no real sleep is needed.
/// </summary>
public class StragglerGuardTests
{
    [Fact]
    public async Task Clean_first_check_returns_immediately_with_no_wait()
    {
        var waits = new List<TimeSpan>();
        var result = await StragglerGuard.CheckAsync(
            _ => Task.FromResult(0L),
            CancellationToken.None,
            (delay, _) => { waits.Add(delay); return Task.CompletedTask; });

        Assert.Equal(0, result.LastCount);
        Assert.Equal(0, result.WaitSeconds);
        Assert.Empty(waits);
    }

    [Fact]
    public async Task A_straggler_that_clears_on_the_second_check_records_one_60_second_wait()
    {
        var counts = new Queue<long>([1, 0]);
        var waits = new List<TimeSpan>();
        var result = await StragglerGuard.CheckAsync(
            _ => Task.FromResult(counts.Dequeue()),
            CancellationToken.None,
            (delay, _) => { waits.Add(delay); return Task.CompletedTask; });

        Assert.Equal(0, result.LastCount);
        Assert.Equal(60, result.WaitSeconds);
        Assert.Single(waits);
    }

    [Fact]
    public async Task A_straggler_that_never_clears_still_proceeds_after_5_tries_and_never_waits_more_than_5_minutes()
    {
        var checks = 0;
        var waits = new List<TimeSpan>();
        var result = await StragglerGuard.CheckAsync(
            _ => { checks++; return Task.FromResult(3L); },
            CancellationToken.None,
            (delay, _) => { waits.Add(delay); return Task.CompletedTask; });

        // MaxRetries = 5, so 6 checks total (attempts 0..5), with a wait after every check except the last.
        Assert.Equal(StragglerGuard.MaxRetries + 1, checks);
        Assert.Equal(3, result.LastCount);
        Assert.Equal(300, result.WaitSeconds); // 5 waits of 60s each, never more than 5 minutes total
        Assert.Equal(5, waits.Count);
    }

    [Fact]
    public async Task The_guard_never_throws_even_when_stragglers_persist()
    {
        var result = await StragglerGuard.CheckAsync(
            _ => Task.FromResult(99L),
            CancellationToken.None,
            (_, _) => Task.CompletedTask);

        Assert.Equal(99, result.LastCount);
    }
}
