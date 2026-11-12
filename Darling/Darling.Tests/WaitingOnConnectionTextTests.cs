using System;
using PerformanceMonitor.Common;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/// <summary>#7590: the Collection Health sentence for a server whose connect failed ends the error part with one period, whether or not the stored error text already ends with one.</summary>
public sealed class WaitingOnConnectionTextTests
{
    private static readonly DateTime Since = new(2026, 11, 4, 17, 58, 0, DateTimeKind.Utc);
    private static readonly DateTime Now = new(2026, 11, 4, 20, 58, 0, DateTimeKind.Utc);

    [Theory]
    [InlineData("Login failed for user 'pm_monitor'.")]
    [InlineData("Login failed for user 'pm_monitor'")]
    public void An_error_text_with_or_without_its_own_period_gets_exactly_one(string error)
    {
        var text = ServerHealthClassifier.WaitingOnConnectionText(Since, error, Now.AddMinutes(4), Now);

        Assert.DoesNotContain("..", text);
        Assert.Contains("Login failed for user 'pm_monitor'. Next retry at 2026-11-04 21:02:00 UTC.", text);
    }
}
