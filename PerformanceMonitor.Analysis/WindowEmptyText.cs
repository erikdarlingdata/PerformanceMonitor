using System;
using System.Globalization;

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
    /// Composes the message for a window with no observed time. <paramref name="factCount"/> is how many facts the
    /// pass still read (the point-in-time ones: configuration, trace flags, hardware), which changes the opening
    /// sentence and nothing else.
    /// </summary>
    public static string Compose(AnalysisContext context, double dataSpanHours, int factCount)
    {
        var window = string.Create(
            CultureInfo.InvariantCulture,
            $"({context.TimeRangeStart:yyyy-MM-dd HH:mm} to {context.TimeRangeEnd:yyyy-MM-dd HH:mm} UTC)");
        var history = string.Create(CultureInfo.InvariantCulture, $"{dataSpanHours:F1} hours of total collected history");

        var opening = factCount > 0
            ? $"The collector observed none of the analysis window {window}, even though this server has {history}. " +
              $"The {factCount} fact(s) that could still be read are point-in-time configuration and state, " +
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
