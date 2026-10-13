using System;
using PerformanceMonitorLite.Services;
using Xunit;
namespace Lite.Tests;

/// <summary>
/// #5644 W13, the Lite twin of the Darling suite's test of the same name: a trace-flag list with none enabled and a
/// running-job snapshot with no job running are sources whose zero is the normal answer, so the collection-health row does not
/// say a persistent zero "needs a look". Lite's row reads the same shared classifier.
/// </summary>
public sealed class ZeroIsNormalCollectorTests
{
    private static CollectorHealthRow Zero(string collector) => new()
    {
        CollectorName = collector,
        TotalRuns = 400,
        SuccessCount = 400,
        RowsStored = 0,
        LastSuccessTime = DateTime.UtcNow.AddMinutes(-1),
        LastRunTime = DateTime.UtcNow.AddMinutes(-1),
    };

    [Theory]
    [InlineData("trace_flags")]
    [InlineData("running_jobs")]
    public void AZeroFromASourceThatIsEmptyByDefault_IsNotSentToALook(string collector)
    {
        var finding = Zero(collector).OutputFinding;

        Assert.NotNull(finding);
        Assert.Contains("normal answer", finding, StringComparison.Ordinal);
        Assert.DoesNotContain("needs a look", finding, StringComparison.Ordinal);
    }

    [Fact]
    public void ASnapshotCollectorNotOnTheList_StillNeedsALook()
    {
        var finding = Zero("database_scoped_config").OutputFinding;

        Assert.NotNull(finding);
        Assert.Contains("needs a look", finding, StringComparison.Ordinal);
    }
}
