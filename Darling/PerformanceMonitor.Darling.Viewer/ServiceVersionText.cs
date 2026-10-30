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
using System.Linq;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The version lines Manage Servers shows once above its grid (#5655): the version each service host recorded in the
/// store, and the viewer's own. Display only; a viewer older or newer than the service gets no warning here.
/// </summary>
public static class ServiceVersionText
{
    /// <summary>
    /// What a store with no row, or without the table at all, shows for the service version. A service writes its row
    /// from 3.11 on, so a missing row means a service older than that, or one that has not started since the upgrade.
    /// </summary>
    public const string Unknown = "unknown (older than 3.11)";

    /// <summary>What the service line shows when the read itself failed, which is not the same as no row.</summary>
    public const string Unavailable = "unavailable";

    /// <summary>The viewer's own version, as its line shows it.</summary>
    public static string ViewerLine(string viewerVersion) => "Viewer version: " + viewerVersion;

    /// <summary>
    /// The service's lines, one per service host in host order. One host reads "Service version: 3.11.0 (HOST, started
    /// 2026-10-01 08:00 UTC)": the host name stays, so an operator can tell which machine runs it, and the start time
    /// shows how old the row is (the service writes it once, at startup, so a retired host keeps its row and its old
    /// version). Several hosts read one "HOST: 3.11.0, started 2026-10-01 08:00 UTC" line each under a "Service
    /// versions:" heading. No host reads <see cref="Unknown"/>. The start time follows the viewer's display mode: local
    /// time when <paramref name="mode"/> is <see cref="TimeDisplayMode.LocalTime"/>, UTC otherwise. Server time is a
    /// monitored server's clock, and a service host is not a monitored server, so it reads UTC. Pure (no statics), so
    /// the text is unit-testable.
    /// </summary>
    public static string ServiceLines(IReadOnlyList<ServiceVersionRow> rows, TimeDisplayMode mode = TimeDisplayMode.UTC)
    {
        ArgumentNullException.ThrowIfNull(rows);

        if (rows.Count == 0)
        {
            return "Service version: " + Unknown;
        }

        var ordered = rows.OrderBy(r => r.ServiceHost, StringComparer.OrdinalIgnoreCase).ToList();
        if (ordered.Count == 1)
        {
            var r = ordered[0];
            return $"Service version: {r.ProductVersion} ({r.ServiceHost}, started {FormatStarted(r.ProcessStartedUtc, mode)})";
        }

        return "Service versions:" + Environment.NewLine
            + string.Join(Environment.NewLine, ordered.Select(r => $"{r.ServiceHost}: {r.ProductVersion}, started {FormatStarted(r.ProcessStartedUtc, mode)}"));
    }

    /// <summary>A host's process start time as "yyyy-MM-dd HH:mm" and its zone label, on the invariant culture.</summary>
    private static string FormatStarted(DateTime startedUtc, TimeDisplayMode mode)
    {
        return ViewerTimeHelper.FormatFleetWide(startedUtc, "yyyy-MM-dd HH:mm", mode);
    }
}
