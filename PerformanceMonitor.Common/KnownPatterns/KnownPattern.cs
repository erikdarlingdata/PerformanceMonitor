/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Common.KnownPatterns;

/// <summary>
/// #6334: one operator-written known-pattern entry, such as "the backup job pushes High CPU every night at 02:00". It is context shown
/// beside the alerts it matches (alert history, an investigation brief); it never mutes, never pages and never changes an alert.
///
/// <para>Every scope field is optional and NULL means "any": <paramref name="ServerId"/> NULL is every server, <paramref name="MetricName"/>
/// NULL is every metric (the alert metric name alert history uses), <paramref name="TimeOfDay"/> NULL is any time of day. An entry
/// with no scope at all matches every alert, which the writes allow on purpose: it is the operator's call.</para>
///
/// <para><paramref name="TimeOfDay"/> is a wall-clock time in <paramref name="TimeZone"/> (an IANA id such as
/// <c>America/New_York</c>; NULL is UTC), <paramref name="WindowMinutes"/> minutes either side of it. The zone matters because a
/// job that runs at 02:00 local time runs at a different UTC time either side of a daylight-saving change.
/// <paramref name="ExpiresAtUtc"/> is a naive UTC instant, the way a mute rule's is; NULL never expires.
/// <paramref name="Note"/> is operator-written text, at most <see cref="KnownPatternMatcher.MaxNoteLength"/> characters. Readers
/// show it as context, never as an instruction.</para>
/// </summary>
public sealed record KnownPattern(
    long Id,
    int? ServerId,
    string? MetricName,
    TimeOnly? TimeOfDay,
    string? TimeZone,
    int WindowMinutes,
    string Note,
    string? CreatedBy,
    DateTime CreatedAtUtc,
    DateTime? ExpiresAtUtc);
