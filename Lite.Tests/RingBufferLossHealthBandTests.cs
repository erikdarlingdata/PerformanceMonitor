using System;
using PerformanceMonitor.Common;
using Xunit;

namespace Lite.Tests;

/* #8146: collection health bands a run that lost ring-buffer events and explains it. */
public class RingBufferLossHealthBandTests
{
    private const string Evicted = "ring_buffer_evicted=12 ring_buffer_loss_window_s=60 ring_buffer_events_limit=1000";
    private const string Dropped = "xe_session_dropped=7 ring_buffer_loss_window_s=60 ring_buffer_events_limit=1000";
    private const string Unknown = "ring_buffer_loss_unknown=1 ring_buffer_loss_window_s=60";
    private const string Truncated = "ring_buffer_truncated=1 ring_buffer_events_hidden=773 newest_visible_event_age_s=19380";

    private static string Band(string? latestRunNote) =>
        CollectorHealthClassifier.Classify(
            totalRuns: 20, successCount: 20, errorCount: 0, permissionDeniedCount: 0, extensionMissingCount: 0,
            abandonedCount: 0, hoursSinceLastSuccess: 0.05, hoursSinceLastRun: 0.05, frequencyMinutes: 5,
            latestRunNote: latestRunNote, trailingSuccessRuns: 20);

    [Fact]
    public void LatestRunEvicted_BandsWarning() => Assert.Equal(CollectorHealthClassifier.Warning, Band(Evicted));

    [Fact]
    public void LatestRunDropped_BandsWarning() => Assert.Equal(CollectorHealthClassifier.Warning, Band(Dropped));

    [Fact]
    public void LatestRunUnknownOnly_StaysHealthyWithNote()
    {
        Assert.Equal(CollectorHealthClassifier.Healthy, Band(Unknown));
        var note = CollectorHealthClassifier.FormatCollectionNote(Unknown, noteCount: 20, totalRuns: 20);
        Assert.Contains("restarted", note, StringComparison.Ordinal);
    }

    [Fact]
    public void RunNote_Evicted_StartsWithLatestRunAndSentence()
    {
        var note = CollectorHealthClassifier.FormatCollectionNote(Evicted, noteCount: 20, totalRuns: 20);
        Assert.StartsWith("latest run: ", note, StringComparison.Ordinal);
        Assert.Contains(RingBufferLossNote.Describe(Evicted)!, note, StringComparison.Ordinal);
        Assert.Contains(Evicted, note, StringComparison.Ordinal);
    }

    [Fact]
    public void RunNote_TruncationAndLoss_JoinsTruncationFirst()
    {
        var both = Truncated + " " + Evicted;
        var note = CollectorHealthClassifier.FormatCollectionNote(both, noteCount: 20, totalRuns: 20);
        var t = RingBufferTruncationNote.Describe(both)!;
        var l = RingBufferLossNote.Describe(both)!;
        Assert.StartsWith("latest run: " + t + "; " + l, note, StringComparison.Ordinal);

        var some = CollectorHealthClassifier.FormatCollectionNote(Evicted, noteCount: 3, totalRuns: 20);
        Assert.StartsWith("latest noted run: " + RingBufferLossNote.Describe(Evicted), some, StringComparison.Ordinal);
    }
}
