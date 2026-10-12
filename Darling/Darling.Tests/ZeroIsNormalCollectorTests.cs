using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;
namespace Darling.Tests;

/// <summary>
/// #5644 W13: Collection Health said a persistent zero "needs a look" for a trace-flag list with none enabled and for a
/// running-job snapshot with no job running, sources whose zero is the normal answer. They now get their own sentence, and
/// the set that earns it is pinned against the catalog.
/// </summary>
public sealed class ZeroIsNormalCollectorTests
{
    private static CollectorHealth Zero(string collector, long notes = 0, long errors = 0) => new()
    {
        CollectorName = collector,
        TotalRuns = 400,
        SuccessCount = 400 - errors,
        ErrorCount = errors,
        RowsStored = 0,
        NoteCount = notes,
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
        Assert.Contains("needs no action", finding, StringComparison.Ordinal);
        Assert.DoesNotContain("needs a look", finding, StringComparison.Ordinal);
        /* It is not an event capture, so the event sentence's claim is not made of it. */
        Assert.DoesNotContain("only when an event occurs", finding, StringComparison.Ordinal);
    }

    [Fact]
    public void ASnapshotCollectorNotOnTheList_StillNeedsALook()
    {
        /* The positive control for the test above: the same row shape on a collector whose zero is not a normal answer. */
        var finding = Zero("database_scoped_config").OutputFinding;

        Assert.NotNull(finding);
        Assert.Contains("needs a look", finding, StringComparison.Ordinal);
        Assert.DoesNotContain("normal answer", finding, StringComparison.Ordinal);
    }

    [Fact]
    public void ANoteOrAFault_KeepsItsOwnSentence_OverTheNormalZero()
    {
        var noted = Zero("trace_flags", notes: 3).OutputFinding;
        Assert.NotNull(noted);
        Assert.DoesNotContain("normal answer", noted, StringComparison.Ordinal);
        Assert.Contains("recorded a note", noted, StringComparison.Ordinal);

        var faulted = Zero("running_jobs", errors: 5).OutputFinding;
        Assert.NotNull(faulted);
        Assert.DoesNotContain("normal answer", faulted, StringComparison.Ordinal);
        Assert.Contains("UNABLE to read", faulted, StringComparison.Ordinal);
    }

    [Fact]
    public void TheZeroIsNormalSet_IsReal_AndDisjointFromTheEventSet()
    {
        var catalog = new HashSet<string>(CollectorCatalog.All.Select(c => c.Name), StringComparer.OrdinalIgnoreCase);
        var set = CollectorHealthClassifier.ZeroIsNormalCollectorNamesForPinning;

        Assert.NotEmpty(set);
        foreach (var name in set)
        {
            Assert.Contains(name, catalog);
            Assert.True(CollectorHealthClassifier.IsZeroNormalCollector(name), name);
            /* The event sentence is checked first and says something else, so an overlap would never reach this one. */
            Assert.False(CollectorHealthClassifier.IsEventCollector(name), name);
        }

        foreach (var described in new[] { "trace_flags", "running_jobs" })
        {
            Assert.True(CollectorHealthClassifier.IsZeroNormalCollector(described), described);
        }

        Assert.False(CollectorHealthClassifier.IsZeroNormalCollector("wait_stats"));
        Assert.False(CollectorHealthClassifier.IsZeroNormalCollector(null));
    }
}
