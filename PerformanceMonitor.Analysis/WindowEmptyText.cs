using System;
using System.Globalization;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The text of the "nothing was measured in this window" answer that Lite's and Darling's analysis services give
/// when the collector observed no time in the analysis window (#3524, #3538 A2). One composer for both products, so
/// the two cannot word it differently, and so what the message says about WHY can follow what the coverage witness
/// knows (#6132).
///
/// <para>Two different situations reach this answer. If no collection landed in the window at all, nothing in the
/// window shows whether collection is still down, so the text says what is known (nothing landed) and points at
/// collection health. If a collection did land but too few for a rate (a first collection after a long outage is
/// the usual one: a rate needs two), collection is running again, and the text says when it came back and that the
/// answer will fill in as the window collects. Calling that case "stopped or broken" was wrong the moment the
/// collector had resumed.</para>
/// </summary>
public static class WindowEmptyText
{
    /// <summary>
    /// Appended by the MCP tools after the window-empty message, to point at collection health. It is wrong for a window
    /// that retention removed (#8910): collection health is green and says nothing about it.
    /// </summary>
    public const string CollectionHealthPointer = " Check get_collection_health to see when each collector last succeeded.";

    /// <summary>
    /// The window-empty message's closing pointer: <see cref="CollectionHealthPointer"/> for a window that may have lost its
    /// collection, nothing for one that retention removed.
    /// </summary>
    public static string HealthPointer(bool removedByRetention) => removedByRetention ? string.Empty : CollectionHealthPointer;

    /// <summary>
    /// The message for a window retention removed, answered before any fact was read (#9218). The same opening and the same
    /// retention sentence <see cref="Compose"/> gives such a window, without the count of point-in-time facts, because none was
    /// read: the window ends before the oldest row the store keeps, so no fact statement could find anything in it.
    /// </summary>
    public static string ComposeRemovedBeforeRead(AnalysisContext context, RetentionBoundary retention)
    {
        var window = string.Create(
            CultureInfo.InvariantCulture,
            $"({context.TimeRangeStart:yyyy-MM-dd HH:mm} to {context.TimeRangeEnd:yyyy-MM-dd HH:mm} UTC)");
        return $"Nothing was measured in the analysis window {window}. " + retention.Describe() + " No fact was read for it. This is NOT an all-clear.";
    }

    /// <summary>
    /// The get_analysis_facts message for a window retention removed, answered before any fact was read (#9218). It says what the
    /// message for an unobserved window says, minus the count of point-in-time facts that were never read.
    /// </summary>
    public static string RemovedFactsMessage(string displayName, RetentionBoundary retention) =>
        $"{retention.Describe()} The requested window for {displayName} is gone from the store, so no windowed fact (wait fractions, blocking or deadlock rates) exists to show, and no fact was read for it. audit_config reports configuration and current state.";

    /// <summary>
    /// Composes the message for a window with no observed time. <paramref name="factCount"/> is how many facts the
    /// pass still read (the point-in-time ones: configuration, trace flags, hardware), which changes the opening
    /// sentence and nothing else.
    /// </summary>
    public static string Compose(AnalysisContext context, double dataSpanHours, int factCount, RetentionBoundary? retention = null)
    {
        var window = string.Create(
            CultureInfo.InvariantCulture,
            $"({context.TimeRangeStart:yyyy-MM-dd HH:mm} to {context.TimeRangeEnd:yyyy-MM-dd HH:mm} UTC)");
        var history = string.Create(CultureInfo.InvariantCulture, $"{dataSpanHours:F1} hours of total collected history");

        /* #8910: a window that ends before the oldest row the store keeps was removed by retention. Collection did not
           break, and "even though this server has N hours of history" would read as "the data should be there". */
        if (retention is not null && retention.RemovedWindow(context.TimeRangeEnd))
        {
            var removedOpening = factCount > 0
                ? $"Nothing was measured in the analysis window {window}. " +
                  $"The {CountText.Plural(factCount, "fact")} that could still be read {(factCount == 1 ? "is" : "are")} point-in-time configuration and state, " +
                  "not measurements of this window. "
                : $"No facts were collected in the analysis window {window}. ";
            return removedOpening + retention.Describe() + " This is NOT an all-clear.";
        }

        var opening = factCount > 0
            ? $"The collector observed none of the analysis window {window}, even though this server has {history}. " +
              $"The {CountText.Plural(factCount, "fact")} that could still be read {(factCount == 1 ? "is" : "are")} point-in-time configuration and state, " +
              "not measurements of this window. "
            : $"No facts were collected in the analysis window {window}, even though this server has {history}. ";

        var coverage = context.Coverage;
        if (coverage is { SampleCount: > 0, FirstSampleTime: { } first })
        {
            // A collection landed, so the collector is up. A rate needs an interval between two collections, and
            // the window holds only this one (or ones too far apart to count), so there is nothing to divide by yet.
            var since = first > context.TimeRangeStart
                ? string.Create(CultureInfo.InvariantCulture, $"Collection was missing until about {first:yyyy-MM-dd HH:mm:ss} UTC and has resumed. ")
                : "Collection has resumed. ";
            return opening + since +
                   "The window holds too few collections to measure a rate over it yet, so nothing was measured. " +
                   "This is NOT an all-clear; run the analysis again after the next few collections.";
        }

        return opening +
               "No collection landed inside it, so nothing was measured. Collection may have stopped or broken " +
               "for this window, or may have resumed after it ended. This is NOT an all-clear.";
    }
}
