using System;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8549: the picker names a picked range the moment it is picked, while the Query Store by Duration grid and slicer keep the old
/// range's result for as long as the read takes, and a pick made during a refresh is only queued. The status bar must say the
/// picked range is loading, and a load with another refresh queued behind it must not stamp "refreshed" for the window it read.
/// </summary>
public sealed class ViewerRangeRefreshStatusTests
{
    [Theory]
    [InlineData(30 * 24, "Past 30 days")]
    [InlineData(4, "Past 4 hours")]
    public void ThePick_IsAnnouncedAsLoading_UnderTheRangeTheHeaderNames(int hours, string headerName)
    {
        var spec = TimeRangeSpec.Relative(TimeSpan.FromHours(hours));
        Assert.Equal(headerName, spec.Name);

        var status = ViewerRangeRefreshStatus.Loading("alpha", spec);

        Assert.Contains("alpha", status, StringComparison.Ordinal);
        Assert.Contains(headerName, status, StringComparison.Ordinal);
        Assert.Contains("loading", status, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("refreshed", status, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ALoadWithARefreshQueuedBehindIt_DoesNotStampRefreshed_AndALoneLoadDoes()
    {
        Assert.False(ViewerRangeRefreshStatus.StampsRefreshed(refreshQueued: true));
        Assert.True(ViewerRangeRefreshStatus.StampsRefreshed(refreshQueued: false));
    }
}
