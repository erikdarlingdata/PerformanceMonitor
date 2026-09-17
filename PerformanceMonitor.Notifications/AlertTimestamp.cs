/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// The alert body's clock-frame primitive: the one conversion from a monitored server's wall clock to UTC,
/// and the only two renderings a timestamp is allowed to reach an alert body in. Both renderings name the
/// frame the value is in.
/// <para>
/// <b>Why a marker on every stamp rather than one header line saying "all times UTC".</b> An alert body is
/// not delivered whole. <c>PerEventNotification.Split</c> emits one message per incident,
/// <see cref="IncidentDeliveryFilter"/> hands a channel a narrowed copy, each channel re-sections
/// <see cref="AlertContext.Details"/> its own way, and the flattened text is re-read afterwards by the MCP
/// alert reader, the triage page, the Viewer's detail pane and
/// <c>AlertMuteContext.PopulateFromDetailText</c>. A frame declared once at the top survives none of that;
/// a frame declared on the value travels with the value.
/// </para>
/// <para>
/// <b>Why UTC rather than the monitored server's clock.</b> An alert's <c>alert_time</c> is naive UTC, and
/// so is every instant the alert engine produces itself — including
/// <c>AlertIncidentRenderer</c>'s "Incident Since" and the analysis path's "Window", which already carry
/// UTC markers. A body mixing those with a monitored server's wall clock asks its reader to reconcile two
/// frames using an offset the body does not carry: on a fleet reporting -240, a two-minute-old failure
/// reads as four hours old, and "four hours ago" and "two minutes ago" support opposite decisions.
/// Labelling the server-local value instead would declare the frame without making the reader's
/// subtraction work, because the number they would need to subtract is the one thing missing. Converting
/// is what makes the arithmetic right for a reader who knows nothing about where the server is.
/// </para>
/// <para>
/// <b>Why the second rendering exists.</b> A server-local instant converts only with that server's own UTC
/// offset, and that offset is measured, not assumed — a server monitored for less than one
/// <c>server_properties</c> cycle, or a store carrying snapshots from before the column existed, has none.
/// The house rule is that an absence must never be mistakable for a value, so an unconvertible instant
/// renders with <see cref="UnknownOffsetMarker"/> rather than silently acquiring a
/// <see cref="UtcMarker"/> it has not earned. The store-side de-skews take
/// <c>COALESCE(utc_offset_minutes, 0)</c> and so render an unknown offset as UTC; that is defensible for a
/// grid the reader can cross-check against a neighbouring column, and is not defensible in a notification
/// that is the reader's only copy.
/// </para>
/// </summary>
public static class AlertTimestamp
{
    /// <summary>
    /// The instant format every alert-body timestamp shares, frame marker excluded. Second resolution:
    /// these are read by someone deciding whether something has just happened.
    /// </summary>
    public const string Format = "yyyy-MM-dd HH:mm:ss";

    /// <summary>Declares a rendered instant as naive UTC — the alert's own frame.</summary>
    public const string UtcMarker = "Z";

    /// <summary>
    /// Declares a rendered instant as the monitored server's own wall clock, unconverted because no UTC
    /// offset is available for that server. Says which frame AND why it is not the other one: "server
    /// clock" alone leaves a reader who cannot look the offset up exactly where they started.
    /// </summary>
    public const string UnknownOffsetMarker = " (server clock, UTC offset not collected)";

    /// <summary>
    /// Converts an instant on a monitored server's wall clock to naive UTC, given that server's collected
    /// offset (<c>DATEDIFF(MINUTE, GETUTCDATE(), GETDATE())</c> — negative west of UTC). Null when
    /// <paramref name="utcOffsetMinutes"/> is null, and null when the shift would leave
    /// <see cref="DateTime"/>'s range, which is how a sentinel default such as
    /// <see cref="DateTime.MinValue"/> arrives here from a row whose instant column was null.
    /// <para>Returning null in both cases rather than throwing or clamping keeps one outcome for
    /// "cannot be stated in UTC", which <see cref="ForServerInstant"/> then renders honestly.</para>
    /// <para>The bound is CHECKED rather than an overflow caught, and the check cannot itself overflow:
    /// an <see cref="int"/> offset is at most ~2.1e9 minutes, so the shift is at most ~1.3e18 ticks, and
    /// against a tick count of at most ~3.2e18 the sum stays well inside <see cref="long"/>. An
    /// <c>AddMinutes</c> in a <c>try</c> would read as if any offset might throw, when the only reachable
    /// case is a sentinel instant sitting at the very edge of the range.</para>
    /// </summary>
    public static DateTime? ToUtc(DateTime serverLocal, int? utcOffsetMinutes)
    {
        if (utcOffsetMinutes is not int offset)
        {
            return null;
        }

        long ticks = serverLocal.Ticks - ((long)offset * TimeSpan.TicksPerMinute);
        if (ticks < DateTime.MinValue.Ticks || ticks > DateTime.MaxValue.Ticks)
        {
            return null;
        }

        return new DateTime(ticks, serverLocal.Kind);
    }

    /// <summary>Renders a naive-UTC instant with the UTC marker.</summary>
    public static string Utc(DateTime utc) =>
        utc.ToString(Format, CultureInfo.InvariantCulture) + UtcMarker;

    /// <summary>
    /// Renders an instant in the monitored server's own clock, marked as unconverted. For the case where
    /// the offset needed to reach UTC is unavailable — never as a shortcut past a conversion that is
    /// available.
    /// </summary>
    public static string ServerClockUnconverted(DateTime serverLocal) =>
        serverLocal.ToString(Format, CultureInfo.InvariantCulture) + UnknownOffsetMarker;

    /// <summary>
    /// The rendering a server-sourced instant takes: UTC when the conversion succeeded, otherwise the
    /// marked server-clock rendering of the original value. One call, so a caller cannot reach the
    /// fallback while forgetting its marker.
    /// </summary>
    public static string ForServerInstant(DateTime serverLocal, int? utcOffsetMinutes)
    {
        var utc = ToUtc(serverLocal, utcOffsetMinutes);
        return utc.HasValue ? Utc(utc.Value) : ServerClockUnconverted(serverLocal);
    }
}
