/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// #8146: the one <c>ring_buffer</c> target clause of the deadlock, blocked-process and long-query sessions, so every create
/// site writes <c>max_events_limit</c> explicitly and the loss note can compare the session's limit with the setting. A
/// session keeps the limit it was created with; the engine's own default (1000) is what a create without the option gets.
/// </summary>
public static class RingBufferTarget
{
    public const int DefaultMaxEvents = 1000;
    public const int MinMaxEvents = 1000;
    public const int MaxMaxEvents = 10000;

    /// <summary>The target clause, without a trailing newline. A null limit leaves the option out (the engine's default).</summary>
    public static string Sql(int? maxEvents) =>
        "ADD TARGET package0.ring_buffer\n(\n    SET max_memory = 4096"
        + (maxEvents is { } limit ? ", max_events_limit = " + limit.ToString(CultureInfo.InvariantCulture) : string.Empty)
        + "\n)";

    /// <summary>The setting's value bounded to [<see cref="MinMaxEvents"/>, <see cref="MaxMaxEvents"/>]; unset is the default.</summary>
    public static int Clamp(int? configured) =>
        configured is { } value ? Math.Clamp(value, MinMaxEvents, MaxMaxEvents) : DefaultMaxEvents;
}
