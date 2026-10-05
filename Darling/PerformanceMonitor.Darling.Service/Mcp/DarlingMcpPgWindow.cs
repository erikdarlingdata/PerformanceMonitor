/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The window-floor notice of the PostgreSQL window reads (#4966), probed on the SAME source the web page's note probes.
/// A read <see cref="WebDataStartNote"/> lists is probed on <see cref="WebDataStartNote.TryGetReadSource"/> (the collector's
/// logged runs for the three sparse reads, else the table's own edge), so the tool and the page never name different starts for
/// one window. A read the web does not list (CPU utilization, the xmin horizon) is probed on its collector table.
/// </summary>
internal static class DarlingMcpPgWindow
{
    /// <summary>The reads the web does not list, and the collector table each one windows on (both on <c>collection_time</c>).</summary>
    internal static readonly System.Collections.Generic.IReadOnlyDictionary<string, string> UnlistedTableByRead =
        new System.Collections.Generic.Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["get_pg_cpu_utilization"] = "pg_cpu_utilization",
            ["get_pg_xmin_horizon"] = "pg_xmin_horizon",
        };

    /// <summary>The table <paramref name="read"/> is named after in a notice: the web's table when it lists the read, else its own.</summary>
    internal static string TableFor(string read) =>
        WebDataStartNote.TableByRead.TryGetValue(read, out var table) ? table : UnlistedTableByRead[read];

    /// <summary>The coverage probe source of <paramref name="read"/>: the web's own when it lists the read, else its collector table's.</summary>
    internal static DataWindowFloor.Source SourceFor(string read) =>
        WebDataStartNote.TryGetReadSource(read, out var source) ? source : DataWindowFloor.Source.ForCollectorTable(UnlistedTableByRead[read]);

    /// <summary>
    /// The notice for one read over [<paramref name="start"/>, <paramref name="end"/>]. A data answer over a window of 90
    /// minutes or less starts no probe; an empty one always does. A failed probe answers
    /// <see cref="McpWindowNotice.Unavailable"/>, which costs the notice and never the rows.
    /// </summary>
    internal static string Finish(string json, McpWindowNotice notice) =>
        notice.IsUnavailable ? DarlingMcpWindowNotice.WithoutKeys(json) : json;

    /// <summary>
    /// An empty answer written with <c>hints = notice.AsHints()</c>, without the <c>hints</c> key when the probe failed (a null
    /// there would otherwise be written as <c>"hints":null</c>).
    /// </summary>
    internal static string FinishEmpty(string json, McpWindowNotice notice)
    {
        if (!notice.IsUnavailable)
        {
            return json;
        }

        var node = System.Text.Json.Nodes.JsonNode.Parse(json)!.AsObject();
        node.Remove("hints");
        return node.ToJsonString(McpHelpers.JsonOptions);
    }

    /// <summary>The notice for one read over [<paramref name="start"/>, <paramref name="end"/>].</summary>
    internal static Task<McpWindowNotice> ReadAsync(
        NpgsqlDataSource postgres, string read, string serverName, DateTime start, DateTime end,
        bool emptyAnswer, ILogger? logger, CancellationToken cancellationToken) =>
        DarlingMcpWindowNotice.ReadAsync(
            () => DarlingMcpWindowNotice.Probe(postgres, SourceFor(read), serverName, start, end, cancellationToken),
            start, end, TableFor(read), emptyAnswer: emptyAnswer, logger: logger, cancellationToken: cancellationToken);
}
