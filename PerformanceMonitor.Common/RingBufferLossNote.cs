/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;

namespace PerformanceMonitor.Common;

/// <summary>
/// #8146: the counts a ring-buffer collector leaves on its run when the buffer evicted events before the read or the
/// session dropped them, and the reader that turns them back into a sentence. The labels live here so the writer and
/// every reader move together (same reason as <see cref="RingBufferTruncationNote"/>).
/// </summary>
public static class RingBufferLossNote
{
    public const string EvictedLabel = "ring_buffer_evicted";
    public const string DroppedLabel = "xe_session_dropped";
    public const string UnknownLabel = "ring_buffer_loss_unknown";
    public const string AtLeastLabel = "ring_buffer_loss_at_least";
    public const string MemoryCappedLabel = "ring_buffer_memory_capped";
    public const string WindowSecondsLabel = "ring_buffer_loss_window_s";
    public const string LimitLabel = "ring_buffer_events_limit";
    public const string LimitBelowSettingLabel = "ring_buffer_limit_below_setting";

    public const string SettingHint = "the ring-buffer event limit setting (xeRingBufferMaxEvents in darling.json, xe_ring_buffer_max_events in Lite's settings.json; it applies when the session is next created)";

    public readonly record struct Loss(long Evicted, long Dropped, bool Unknown, bool AtLeast, bool MemoryCapped, bool Truncated, long WindowSeconds, long Limit, bool LimitBelowSetting);

    /// <summary>True when the note carries evicted, dropped or unknown above zero. Reads label=value pairs anywhere in the note.</summary>
    public static bool TryParse(string? note, out Loss loss)
    {
        loss = default;
        if (string.IsNullOrWhiteSpace(note))
        {
            return false;
        }

        long evicted = 0, dropped = 0, unknown = 0, atLeast = 0, memory = 0, window = 0, limit = 0, below = 0;
        foreach (var token in note.Split(new[] { ' ', ';' }, StringSplitOptions.RemoveEmptyEntries))
        {
            var eq = token.IndexOf('=', StringComparison.Ordinal);
            if (eq <= 0
                || !long.TryParse(token.AsSpan(eq + 1), NumberStyles.None, CultureInfo.InvariantCulture, out var value))
            {
                continue;
            }

            var label = token.AsSpan(0, eq);
            if (label.SequenceEqual(EvictedLabel))
            {
                evicted = value;
            }
            else if (label.SequenceEqual(DroppedLabel))
            {
                dropped = value;
            }
            else if (label.SequenceEqual(UnknownLabel))
            {
                unknown = value;
            }
            else if (label.SequenceEqual(AtLeastLabel))
            {
                atLeast = value;
            }
            else if (label.SequenceEqual(MemoryCappedLabel))
            {
                memory = value;
            }
            else if (label.SequenceEqual(WindowSecondsLabel))
            {
                window = value;
            }
            else if (label.SequenceEqual(LimitLabel))
            {
                limit = value;
            }
            else if (label.SequenceEqual(LimitBelowSettingLabel))
            {
                below = value;
            }
        }

        if (evicted == 0 && dropped == 0 && unknown == 0)
        {
            return false;
        }

        loss = new Loss(
            evicted, dropped, unknown > 0, atLeast > 0, memory > 0,
            RingBufferTruncationNote.TryParse(note, out _, out _), window, limit, below > 0);
        return true;
    }

    /// <summary>The plain sentence for a run that lost events, or null when the note says nothing was lost.</summary>
    public static string? Describe(string? note)
    {
        if (!TryParse(note, out var loss))
        {
            return null;
        }

        var inv = CultureInfo.InvariantCulture;
        var happened = new List<string>();
        if (loss.Evicted > 0)
        {
            happened.Add(string.Format(inv, "evicted {0}{1:N0} events before they were read", loss.AtLeast ? "at least " : "", loss.Evicted));
        }

        if (loss.Dropped > 0)
        {
            happened.Add(string.Format(inv, "dropped {0:N0} events before they reached its targets (its buffers were full)", loss.Dropped));
        }

        if (loss.Unknown)
        {
            happened.Add("restarted, so events evicted from its buffer or dropped in that span cannot be counted");
        }

        var text = string.Format(
            inv,
            "In the {0} before this run the session {1}.",
            RingBufferTruncationNote.FormatAge(loss.WindowSeconds),
            string.Join(", and ", happened));

        if (loss.Dropped > 0)
        {
            text += " The dropped count is session-wide and a larger event limit does not help it; collect more often.";
        }

        if (loss.Evicted > 0)
        {
            if (loss.MemoryCapped)
            {
                text += " The buffer was full at its 4 MB memory cap, so a larger event limit would not help. Collect more often.";
            }
            else if (loss.Truncated)
            {
                text += " This read was already truncated at about 4 MB of XML, so a larger event limit would not help. Collect more often.";
            }
            else
            {
                var holds = loss.Limit > 0 ? string.Format(inv, " The session holds at most {0:N0} events.", loss.Limit) : "";
                text += holds + " Raise " + SettingHint + ", or collect more often.";
            }
        }

        if (loss.LimitBelowSetting)
        {
            text += string.Format(
                inv,
                " This session's limit is {0:N0}, below the configured setting: it was created before the setting changed or by another install.",
                loss.Limit);
        }

        return text;
    }

    /// <summary>The loss notes of a window of runs, summed.</summary>
    public readonly record struct LossWindow(long Evicted, long Dropped, int UnknownRuns, DateTime FirstLossUtc, DateTime LastLossUtc, string NewestNote)
    {
        /// <summary>The runs whose note <see cref="TryParse"/> calls lossy, summed, or null when none is. The newest note is the newest run's, a tie on time going to the greater note.</summary>
        public static LossWindow? FromRuns(IEnumerable<(DateTime CollectionTime, string? Note)> runs)
        {
            ArgumentNullException.ThrowIfNull(runs);
            long evicted = 0, dropped = 0;
            var unknownRuns = 0;
            var count = 0;
            var first = DateTime.MaxValue;
            var last = DateTime.MinValue;
            string? newest = null;
            foreach (var (time, note) in runs)
            {
                if (!TryParse(note, out var loss))
                {
                    continue;
                }

                count++;
                evicted += loss.Evicted;
                dropped += loss.Dropped;
                if (loss.Unknown)
                {
                    unknownRuns++;
                }

                if (time < first)
                {
                    first = time;
                }

                if (time > last || (time == last && string.CompareOrdinal(note, newest) > 0))
                {
                    last = time;
                    newest = note;
                }
            }

            return count == 0 ? null : new LossWindow(evicted, dropped, unknownRuns, first, last, newest!);
        }

        /// <summary>
        /// The payload of <c>ring_buffer_loss</c>. A window whose lossy runs were all restarts has nothing countable, so both counts are
        /// null there, never 0: an unknowable value is not a zero (REVIEW-TRAPS:20/36). <paramref name="databaseFiltered"/> adds the note
        /// that a database filter does not narrow the loss.
        /// </summary>
        public IReadOnlyDictionary<string, object?> ToPayload(bool databaseFiltered = false)
        {
            /* A run is lossy only on an evicted, a dropped or an unknown count (TryParse), and a restart carries only the last, so zero in
               both sums means no run of the window could be counted. */
            var countable = Evicted > 0 || Dropped > 0;
            var payload = new Dictionary<string, object?>
            {
                ["evicted_events"] = countable ? Evicted : null,
                ["dropped_events"] = countable ? Dropped : null,
                ["unknown_spans"] = UnknownRuns,
                ["first_loss_utc"] = Iso(FirstLossUtc),
                ["last_loss_utc"] = Iso(LastLossUtc),
                ["note"] = Sentence(),
            };
            if (databaseFiltered)
            {
                payload["database_filter_note"] = RingBufferLossAnswer.DatabaseFilterNote;
            }

            return payload;
        }

        /* The same "o" round-trip text the neighbouring timestamp keys use, with the Z of an instant the store holds in UTC. */
        private static string Iso(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture);

        /// <summary>
        /// The <c>note</c> of the payload: the window's totals, then the newest lossy run's own sentence (<see cref="Describe"/>), which is
        /// where the advice and the setting to raise come from. The raw labels stay out of it. The totals come from the summed fields, so a
        /// window combined from two collectors reads the same way as one.
        /// </summary>
        private string Sentence()
        {
            var inv = CultureInfo.InvariantCulture;
            var parts = new List<string>();
            if (Evicted > 0)
            {
                parts.Add(string.Format(inv, "{0:N0} event(s) were evicted from the ring buffer before collection", Evicted));
            }

            if (Dropped > 0)
            {
                parts.Add(string.Format(inv, "{0:N0} event(s) were dropped by the session before they reached its targets", Dropped));
            }

            if (UnknownRuns > 0)
            {
                parts.Add(string.Format(inv, "{0:N0} span(s) after a session restart could not be counted", UnknownRuns));
            }

            /* The times are the collector runs that FOUND the loss; each loss happened in the window before its run. */
            var first = FirstLossUtc.ToString("yyyy-MM-dd HH:mm", inv);
            var last = LastLossUtc.ToString("yyyy-MM-dd HH:mm", inv);
            var found = first == last
                ? string.Format(inv, " found by the run at {0} UTC", first)
                : string.Format(inv, " found by the runs between {0} and {1} UTC", first, last);
            var text = string.Join("; ", parts) + found;
            return Describe(NewestNote) is { } latest ? text + ". Latest: " + latest : text;
        }
    }
}
