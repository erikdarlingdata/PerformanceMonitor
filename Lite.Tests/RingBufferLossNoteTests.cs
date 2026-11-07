/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using Xunit;

namespace Lite.Tests;

/// <summary>#8146: the ring-buffer loss note's parser, wording and window aggregate.</summary>
public sealed class RingBufferLossNoteTests
{
    private const string CountCap = "ring_buffer_evicted=1240 ring_buffer_loss_window_s=60 ring_buffer_events_limit=1000";

    [Fact]
    public void Describe_CountCap_NamesLimitAndSetting()
    {
        var text = RingBufferLossNote.Describe(CountCap);
        Assert.Contains("evicted 1,240 events", text);
        Assert.Contains("1,000 events", text);
        Assert.Contains(RingBufferLossNote.SettingHint, text);
        Assert.Contains("1 min before this run", text);
    }

    [Fact]
    public void Describe_MemoryCap_SaysLargerLimitWouldNotHelp()
    {
        var text = RingBufferLossNote.Describe(CountCap + " ring_buffer_memory_capped=1");
        Assert.Contains("a larger event limit would not help", text);
        Assert.DoesNotContain(RingBufferLossNote.SettingHint, text);
    }

    [Fact]
    public void Describe_TruncatedRead_SaysCollectMoreOften()
    {
        var text = RingBufferLossNote.Describe(CountCap + " ring_buffer_truncated=1");
        Assert.DoesNotContain(RingBufferLossNote.SettingHint, text);
        Assert.Contains("collect more often", text, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Describe_Dropped_IsSessionWide()
    {
        var text = RingBufferLossNote.Describe("xe_session_dropped=85 ring_buffer_loss_window_s=60");
        Assert.Contains("dropped 85 events before they reached its targets", text);
        Assert.DoesNotContain(RingBufferLossNote.SettingHint, text);
    }

    [Fact]
    public void Describe_Unknown_CoversBothCounts()
    {
        var text = RingBufferLossNote.Describe("ring_buffer_loss_unknown=1 ring_buffer_loss_window_s=60");
        Assert.Contains("restarted", text);
        Assert.Contains("cannot be counted", text);
    }

    [Fact]
    public void Describe_AtLeast_SaysAtLeast()
    {
        var text = RingBufferLossNote.Describe(CountCap + " ring_buffer_loss_at_least=1");
        Assert.Contains("at least 1,240 events", text);
    }

    [Fact]
    public void Describe_LimitBelowSetting_AddsNotAppliedSentence()
    {
        var text = RingBufferLossNote.Describe(CountCap + " ring_buffer_limit_below_setting=1");
        Assert.Contains("below the configured setting", text);
        Assert.DoesNotContain("below the configured", RingBufferLossNote.Describe(CountCap));
    }

    [Fact]
    public void Describe_EvictedAndDropped_NamesBoth()
    {
        var text = RingBufferLossNote.Describe(CountCap + " xe_session_dropped=85");
        Assert.True(text!.IndexOf("evicted", StringComparison.Ordinal) < text.IndexOf("dropped", StringComparison.Ordinal));
    }

    [Fact]
    public void TryParse_CleanNote_IsFalse()
    {
        Assert.False(RingBufferLossNote.TryParse("events_read=3", out _));
        Assert.Null(RingBufferLossNote.Describe("events_read=3"));
    }

    [Fact]
    public void TryParse_ReadsEveryLabel()
    {
        Assert.True(RingBufferLossNote.TryParse(CountCap + " ring_buffer_truncated=1 ring_buffer_limit_below_setting=1", out var loss));
        Assert.Equal(1240, loss.Evicted);
        Assert.Equal(60, loss.WindowSeconds);
        Assert.Equal(1000, loss.Limit);
        Assert.True(loss.Truncated);
        Assert.True(loss.LimitBelowSetting);
        Assert.False(loss.Unknown);
    }

    [Fact]
    public void FromRuns_SumsAndCountsUnknown()
    {
        var t = new DateTime(2026, 11, 7, 10, 0, 0, DateTimeKind.Utc);
        var window = RingBufferLossNote.LossWindow.FromRuns(new (DateTime, string?)[]
        {
            (t.AddMinutes(-5), "events_read=3"),
            (t.AddMinutes(-4), "ring_buffer_evicted=10 ring_buffer_loss_window_s=60"),
            (t.AddMinutes(-3), "ring_buffer_loss_unknown=1 ring_buffer_loss_window_s=60"),
            (t.AddMinutes(-2), "xe_session_dropped=4 ring_buffer_loss_window_s=60"),
            (t.AddMinutes(-1), null),
        });
        Assert.NotNull(window);
        Assert.Equal(10, window.Value.Evicted);
        Assert.Equal(4, window.Value.Dropped);
        Assert.Equal(1, window.Value.UnknownRuns);
        Assert.Equal(t.AddMinutes(-4), window.Value.FirstLossUtc);
        Assert.Equal(t.AddMinutes(-2), window.Value.LastLossUtc);
        Assert.Contains("xe_session_dropped=4", window.Value.NewestNote);
    }

    [Fact]
    public void FromRuns_NoLoss_IsNull()
    {
        Assert.Null(RingBufferLossNote.LossWindow.FromRuns(new (DateTime, string?)[] { (DateTime.UtcNow, "events_read=3") }));
    }

    [Fact]
    public void ToPayload_HasKeys()
    {
        var t = new DateTime(2026, 11, 7, 10, 0, 0, DateTimeKind.Utc);
        var payload = new RingBufferLossNote.LossWindow(10, 4, 1, t, t.AddMinutes(1), "n").ToPayload();
        foreach (var key in new[] { "evicted_events", "dropped_events", "unknown_spans", "first_loss_utc", "last_loss_utc", "note" })
        {
            Assert.True(payload.ContainsKey(key), key);
        }

        Assert.Equal(10L, payload["evicted_events"]);
    }

    [Fact]
    public void ToPayload_NoteIsSentenceWithAdviceNotLabels()
    {
        var t = new DateTime(2026, 11, 7, 10, 0, 0, DateTimeKind.Utc);
        var window = new RingBufferLossNote.LossWindow(
            12, 4, 1, t, t.AddMinutes(1),
            "ring_buffer_evicted=12 ring_buffer_loss_window_s=60 ring_buffer_events_limit=1000");

        var payload = window.ToPayload();
        var note = (string)payload["note"]!;

        Assert.Contains("12 event(s) were evicted from the ring buffer before collection", note, StringComparison.Ordinal);
        Assert.Contains("4 event(s) were dropped by the session before they reached its targets", note, StringComparison.Ordinal);
        Assert.Contains("1 span(s) after a session restart could not be counted", note, StringComparison.Ordinal);
        Assert.Contains("between 2026-11-07 10:00 and 2026-11-07 10:01 UTC", note, StringComparison.Ordinal);
        Assert.Contains(". Latest: ", note, StringComparison.Ordinal);
        Assert.Contains(RingBufferLossNote.SettingHint, note, StringComparison.Ordinal);
        Assert.DoesNotContain("ring_buffer_evicted=", note, StringComparison.Ordinal);
        Assert.EndsWith("Z", (string)payload["first_loss_utc"]!, StringComparison.Ordinal);
        Assert.EndsWith("Z", (string)payload["last_loss_utc"]!, StringComparison.Ordinal);
        Assert.Equal(t, DateTime.Parse((string)payload["first_loss_utc"]!, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.RoundtripKind));
    }
}
