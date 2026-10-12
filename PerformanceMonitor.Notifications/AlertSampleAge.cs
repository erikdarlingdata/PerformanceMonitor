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

namespace PerformanceMonitor.Notifications;

/// <summary>
/// The ONE place an alert's "newest sample" age is computed (#5642). The SQL Server engine
/// (<c>AlertEngine</c>, shared by Lite and Darling) and Darling's PostgreSQL families (<c>DarlingWorker</c>) both
/// stamp <see cref="AlertContext.NewestSample"/> through here, so the rule for which instants carry an age cannot
/// drift between the two (#3684: two copies pinned equal to each other fail only after both have drifted).
/// </summary>
public static class AlertSampleAge
{
    /// <summary>
    /// Fills <see cref="AlertContext.NewestSample"/> at FIRE time: the source's label, the newest sample's UTC
    /// instant and its age against the caller's clock, computed once and persisted with the alert's context. A
    /// null instant, a LOCAL-kind one (a pre-V134 stamp on the monitored server's clock) or the
    /// <see cref="DateTime.MinValue"/> sentinel gives no age and leaves the context as it was; a sample dated
    /// later than now ages as zero. The context is created only when there is an age to carry, so a site whose
    /// context was null stays null without one.
    /// </summary>
    public static AlertContext? Stamp(AlertContext? context, string source, DateTime? instantUtc, DateTime now)
    {
        if (!instantUtc.HasValue || instantUtc.Value.Kind == DateTimeKind.Local || instantUtc.Value == DateTime.MinValue)
        {
            return context;
        }

        var age = now - instantUtc.Value;
        context ??= new AlertContext();
        context.NewestSample = new AlertNewestSample(
            source, DateTime.SpecifyKind(instantUtc.Value, DateTimeKind.Utc), age < TimeSpan.Zero ? TimeSpan.Zero : age);
        return context;
    }

    /// <summary>The newest of a set of nullable sample instants, or null when none carries one. A default
    /// (<see cref="DateTime.MinValue"/>) instant is none.</summary>
    public static DateTime? Newest(IEnumerable<DateTime?> instants)
    {
        DateTime? newest = null;
        foreach (var instant in instants)
        {
            if (instant.HasValue && instant.Value != default && (!newest.HasValue || instant.Value > newest.Value))
            {
                newest = instant;
            }
        }

        return newest;
    }

    /// <summary>The newest of a set of non-nullable row instants (a default instant is none): the PostgreSQL
    /// reads' shape. Delegates to <see cref="Newest(IEnumerable{DateTime?})"/>.</summary>
    public static DateTime? Newest(IEnumerable<DateTime> instants) =>
        Newest(instants.Select(i => (DateTime?)i));
}
