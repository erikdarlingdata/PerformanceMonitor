/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Common;

/// <summary>One Collection Health row's newest error, as the connection-failure summary reads it.</summary>
/// <param name="CollectorName">The collector.</param>
/// <param name="LastErrorTime">When its newest failure ran (UTC), or null when it has none in the window.</param>
/// <param name="LastError">The newest failure's message.</param>
/// <param name="ErrorCount">How many ERROR runs the window holds for it.</param>
public readonly record struct CollectorErrorSample(string CollectorName, DateTime? LastErrorTime, string? LastError, long ErrorCount);

/// <summary>A connection failure that reached several collectors in one collection cycle.</summary>
/// <param name="StartUtc">The earliest of the collectors' newest-error instants.</param>
/// <param name="EndUtc">The latest of them.</param>
/// <param name="Collectors">The collectors whose newest error it is, in name order.</param>
/// <param name="CollectorsWithOtherErrors">How many of them also hold other ERROR runs in the window.</param>
public sealed record ConnectionFailureEvent(DateTime StartUtc, DateTime EndUtc, IReadOnlyList<string> Collectors, int CollectorsWithOtherErrors);

/// <summary>
/// Reads the Collection Health rows' newest errors for the one shape a reader cannot see from the rows alone: each collector
/// run opens its own connection, so a target that is unreachable for one cycle fails every collector due in it, and the grid
/// shows one ERROR on each of fifteen rows with nothing to say they are one event. This folds those rows into one sentence
/// for the tab's header. Display text only: no band reads it.
/// </summary>
public static class CollectionConnectionFailures
{
    /// <summary>
    /// Phrases the SQL Server and PostgreSQL drivers put in a message when the target itself could not be reached, as against
    /// a statement the target refused or failed. Matched without regard to case.
    /// </summary>
    private static readonly string[] s_connectionFailureMarks =
    {
        "pre-login handshake",
        "network-related or instance-specific error",
        "transport-level error",
        "Connection Timeout Expired",
        "server was not found or was not accessible",
        "No connection could be made",
        "Unable to connect",
        "Failed to connect",
        "forcibly closed by the remote host",
        "No such host is known",
        "Connection refused",
        "the connection is closed",
        "connection was terminated",
    };

    /// <summary>
    /// How far apart two collectors' newest-error instants may sit and still be one failure. One cycle runs its collectors in
    /// turn and each failed open can wait out the connect timeout, so the instants of one drop spread over minutes.
    /// </summary>
    public static readonly TimeSpan SameEventSpan = TimeSpan.FromMinutes(5);

    /// <summary>Whether <paramref name="errorMessage"/> says the target could not be reached, rather than what it answered.</summary>
    public static bool IsConnectionFailure(string? errorMessage)
    {
        if (string.IsNullOrWhiteSpace(errorMessage))
        {
            return false;
        }

        foreach (var mark in s_connectionFailureMarks)
        {
            if (errorMessage.Contains(mark, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// The connection failures that reached two or more collectors, newest first. A row counts when its newest error is a
    /// connection failure; rows whose instants sit within <see cref="SameEventSpan"/> of the previous one are one event. A
    /// failure that reached one collector alone is that row's own Last Error and is not an event.
    /// </summary>
    public static IReadOnlyList<ConnectionFailureEvent> Group(IEnumerable<CollectorErrorSample> rows)
    {
        var failures = rows
            .Where(row => row.LastErrorTime.HasValue && IsConnectionFailure(row.LastError))
            .OrderBy(row => row.LastErrorTime!.Value)
            .ThenBy(row => row.CollectorName, StringComparer.Ordinal)
            .ToList();

        var events = new List<ConnectionFailureEvent>();
        var index = 0;
        while (index < failures.Count)
        {
            var members = new List<CollectorErrorSample> { failures[index] };
            var end = failures[index].LastErrorTime!.Value;
            index++;
            while (index < failures.Count && failures[index].LastErrorTime!.Value - end <= SameEventSpan)
            {
                members.Add(failures[index]);
                end = failures[index].LastErrorTime!.Value;
                index++;
            }

            if (members.Count < 2)
            {
                continue;
            }

            events.Add(new ConnectionFailureEvent(
                members[0].LastErrorTime!.Value,
                end,
                members.Select(member => member.CollectorName).OrderBy(name => name, StringComparer.Ordinal).ToList(),
                members.Count(member => member.ErrorCount > 1)));
        }

        events.Reverse();
        return events;
    }

    /// <summary>
    /// The header sentence for <paramref name="rows"/>, or null when no connection failure reached two or more collectors.
    /// Names the newest event's time (<paramref name="formatTime"/> words a UTC instant in the tab's zone), how many
    /// collectors it reached, whether each of them missed that one run alone, and how many earlier events the window holds.
    /// </summary>
    public static string? SummaryLine(IEnumerable<CollectorErrorSample> rows, Func<DateTime, string> formatTime)
    {
        var events = Group(rows);
        if (events.Count == 0)
        {
            return null;
        }

        var newest = events[0];
        var when = newest.EndUtc - newest.StartUtc < TimeSpan.FromMinutes(1)
            ? $"at {formatTime(newest.StartUtc)}"
            : $"between {formatTime(newest.StartUtc)} and {formatTime(newest.EndUtc)}";
        var reach = $"reached {newest.Collectors.Count} collectors";
        var runs = newest.CollectorsWithOtherErrors == 0
            ? "each missed that one run and has no other error in the window"
            : $"{newest.CollectorsWithOtherErrors} of them have other errors in the window as well";
        var earlier = events.Count switch
        {
            1 => "Nothing to do unless it repeats.",
            2 => "One earlier connection failure is in the window too.",
            _ => $"{events.Count - 1} earlier connection failures are in the window too.",
        };

        return $"One connection failure {when} {reach}: {runs}. {earlier}";
    }
}
