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
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// What the Database Protection tab reads for one server (#6873): the same answer <c>get_database_protection</c> gives. <see cref="Status"/> is
/// <c>ok</c> or <c>overdue</c> when a capture exists, and otherwise the read's own word: <c>not_collected</c> (this engine does not collect it),
/// <c>precondition</c> (the collector is refused here) or <c>unavailable</c> (no capture in the window). <see cref="Message"/> is the sentence
/// for the last three. <see cref="Databases"/> are banded by <see cref="DatabaseProtectionBands"/>, the one judgement the tool and the web share.
/// </summary>
public sealed record ViewerDatabaseProtectionRead(
    string Status,
    string? Message,
    DateTime? CapturedAt,
    IReadOnlyList<(DatabaseProtectionFact Fact, DatabaseProtectionBand Band)> Databases,
    DatabaseProtectionSummary? Summary)
{
    public const string Ok = "ok";
    public const string Overdue = "overdue";
    public const string NotCollected = "not_collected";
    public const string Precondition = "precondition";
    public const string Unavailable = "unavailable";

    /// <summary>A capture exists, so there are databases and a summary to show.</summary>
    public bool HasCapture => Summary is not null;
}

public sealed partial class ViewerDataService
{
    /// <summary>The window of captures the tab reads for the newest one, as <c>get_database_protection</c> defaults to.</summary>
    public const int DatabaseProtectionHoursBack = 24;

    /// <summary>
    /// One server's last full backup, log backup and CHECKDB per database, banded, for the <paramref name="hoursBack"/> hours ending at
    /// <paramref name="windowEndUtc"/>. The reader and the banding are the ones the MCP tool calls. With no capture the answer follows the
    /// tool's order: the engine gap (<c>not_collected</c>), then the collector's latest named skip (<c>precondition</c>), then <c>unavailable</c>.
    /// </summary>
    public async Task<ViewerDatabaseProtectionRead> ReadDatabaseProtectionAsync(
        DarlingServer server, DateTime windowEndUtc, int hoursBack = DatabaseProtectionHoursBack, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(server);
        var read = await DarlingDatabaseProtectionReader.ReadAsync(
            _dataSource, server.ServerId, windowEndUtc.AddHours(-hoursBack), windowEndUtc,
            ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);
        if (read.HasCapture)
        {
            var capturedAt = DateTime.SpecifyKind(read.NewestCapture!.Value, DateTimeKind.Utc);
            var banded = read.Databases
                .Select(d => (Fact: d, Band: DatabaseProtectionBands.Evaluate(d, capturedAt, DatabaseProtectionThresholds.Default)))
                .ToList();
            var summary = DatabaseProtectionBands.Summarize(banded);
            return new ViewerDatabaseProtectionRead(
                summary.OverdueDatabases > 0 ? ViewerDatabaseProtectionRead.Overdue : ViewerDatabaseProtectionRead.Ok,
                null, capturedAt, banded, summary);
        }

        var engineGap = ViewerServerTab.EngineGapNote(server.DisplayName, server.EngineEdition, server.EngineKind, "database_protection", awsRdsKind: AwsRdsKind.None);
        if (engineGap is not null)
        {
            return new ViewerDatabaseProtectionRead(ViewerDatabaseProtectionRead.NotCollected, engineGap, null, [], null);
        }

        var (text, _) = await ViewerServerTab.ReadEngineGapStateAsync(
            (serverId, collector) => GetCollectorLastRunAsync(serverId, collector, cancellationToken), null,
            server.ServerId, server.DisplayName, server.EngineEdition, server.EngineKind, "database_protection", 0,
            (serverId, collector) => GetLatestCollectorOutcomeAsync(serverId, collector, cancellationToken), awsRdsKind: AwsRdsKind.None);
        if (!string.IsNullOrEmpty(text))
        {
            return new ViewerDatabaseProtectionRead(ViewerDatabaseProtectionRead.Precondition, text, null, [], null);
        }

        return new ViewerDatabaseProtectionRead(
            ViewerDatabaseProtectionRead.Unavailable,
            string.Create(CultureInfo.InvariantCulture,
                $"No database protection capture in the {hoursBack}-hour window ending {DateTime.SpecifyKind(windowEndUtc, DateTimeKind.Utc):o}. The database_protection collector has not run in it."),
            null, [], null);
    }
}
