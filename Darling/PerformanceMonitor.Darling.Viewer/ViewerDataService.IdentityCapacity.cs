/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// What the Identity Capacity tab reads for one server (#6875): the same answer <c>get_identity_capacity</c> gives. <see cref="Status"/>
/// is <c>data</c> (at least one column above 10 percent, or a database not yet scanned), <c>empty</c> (databases were scanned, none has one and none waits), or the read's own word
/// when nothing was scanned: <c>not_collected</c> (this engine has no such data), <c>precondition</c> (the collector's latest run was refused
/// or denied here) or <c>unavailable</c> (no scan in the window). <see cref="Message"/> is the sentence for the last three.
/// </summary>
public sealed record ViewerIdentityCapacityRead(string Status, string? Message, IdentityCapacityRead? Read)
{
    public const string Data = "data";
    public const string Empty = "empty";
    public const string NotCollected = "not_collected";
    public const string Precondition = "precondition";
    public const string Unavailable = "unavailable";

    /// <summary>At least one database was scanned, so there is a scan state to show.</summary>
    public bool HasScan => Read is { HasScan: true };
}

public sealed partial class ViewerDataService
{
    /// <summary>The window of scans the tab reads, as <c>get_identity_capacity</c> defaults to: seven days, so the rate has samples to compare.</summary>
    public const int IdentityCapacityHoursBack = 168;

    /// <summary>
    /// The server's identity columns above 10 percent used and each database's scan state over the <paramref name="hoursBack"/> hours
    /// ending at <paramref name="windowEndUtc"/>. The reader and the math are the ones the MCP tool calls, so both surfaces judge once.
    /// With no scan the answer follows the tool's order: the engine gap (<c>not_collected</c>), then the collector's latest named skip
    /// (<c>precondition</c>), then <c>unavailable</c>.
    /// </summary>
    public async Task<ViewerIdentityCapacityRead> ReadIdentityCapacityAsync(
        DarlingServer server, DateTime windowEndUtc, int hoursBack = IdentityCapacityHoursBack, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(server);
        var read = await DarlingIdentityCapacityReader.ReadAsync(
            _dataSource, server.ServerId, DatabaseFilter.All, windowEndUtc.AddHours(-hoursBack), windowEndUtc,
            ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);
        if (read.HasScan)
        {
            return new ViewerIdentityCapacityRead(read.Columns.Count > 0 || read.Unscanned.Count > 0 || read.Skipped.Count > 0 ? ViewerIdentityCapacityRead.Data : ViewerIdentityCapacityRead.Empty, null, read);
        }

        var engineGap = ViewerServerTab.EngineGapNote(server.DisplayName, server.EngineEdition, server.EngineKind, "identity_capacity");
        if (engineGap is not null)
        {
            return new ViewerIdentityCapacityRead(ViewerIdentityCapacityRead.NotCollected, engineGap, read);
        }

        /* The collector applies to every engine, so the only precondition is a latest run that was refused or denied (a named skip),
           the monitored server's own sentence, as get_identity_capacity says it. A diagnostic read that fails leaves the honest miss. */
        try
        {
            var (status, errorMessage, observedUtc) = await GetLatestCollectorOutcomeAsync(server.ServerId, "identity_capacity", cancellationToken);
            var note = ViewerServerTab.CollectorOutcomeNote(server.DisplayName, "identity_capacity", status, errorMessage, observedUtc);
            if (!string.IsNullOrEmpty(note))
            {
                return new ViewerIdentityCapacityRead(ViewerIdentityCapacityRead.Precondition, note, read);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* fall through to unavailable */
        }

        return new ViewerIdentityCapacityRead(
            ViewerIdentityCapacityRead.Unavailable,
            string.Create(CultureInfo.InvariantCulture,
                $"No identity capacity scan in the {hoursBack}-hour window ending {DateTime.SpecifyKind(windowEndUtc, DateTimeKind.Utc):o}. The identity_capacity collector has not run in it, or has not reached the database yet."),
            read);
    }
}
