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
using System.Text.Json;
using System.Text.Json.Nodes;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The words and the command arguments of the Collection Health tab's "Run all collectors now" and per-collector "Run now"
/// (#6382). They are the web dashboard's pair (#5941, <c>pages/collector-run.js</c>): the same <c>snapshot_now</c> command, with the
/// same optional <c>{"collectors":[...]}</c> argument, and the same result text, so the two surfaces say the same thing about the same run.
/// Also the seat gate of the tab's three write buttons (#8103): whether each is on, and what its tooltip says, from the seat, the server's
/// connection and a run in flight, so the tab and its tests take the state from one place.
/// </summary>
internal static class ViewerCollectorRun
{
    /// <summary>The <c>args_json</c> for a run of the named collectors, or null for the run-all form (every enabled collector).</summary>
    public static string? BuildArgs(IReadOnlyList<string>? collectors) =>
        collectors is { Count: > 0 }
            ? new JsonObject { ["collectors"] = new JsonArray(collectors.Select(c => (JsonNode?)JsonValue.Create(c)).ToArray()) }.ToJsonString()
            : null;

    /// <summary>What the status line says while the run goes (the web's wording).</summary>
    public static string RunningText(IReadOnlyList<string>? collectors) =>
        collectors is { Count: > 0 }
            ? "Running " + string.Join(", ", collectors) + " now..."
            : "Running every enabled collector now...";

    /// <summary>What the status line says when the Viewer stopped waiting (the web's wording; the Collection Log sub-tab is below the buttons).</summary>
    public const string StillRunningText = "The run is still going. Open the Collection Log below in a minute to see its result.";

    /// <summary>What the status line says to a read-only seat whose click got through (the web's 403 sentence), the backstop behind the disabled buttons.</summary>
    public const string ReadOnlyText = "This account has read-only access, so it cannot run collectors.";

    /// <summary>"Run all collectors now"'s tooltip while the seat can write and the server is reachable.</summary>
    public const string RunAllReadyTip = "Runs every enabled collector for this server once, now. Disabled collectors are skipped. Schedules do not change.";

    /// <summary>A row's "Run now" tooltip while the seat can write.</summary>
    public const string RunOneReadyTip = "Runs this collector once, now, even if it is disabled. Its schedule and enabled state do not change.";

    /// <summary>
    /// #8103: the tooltip of a run button that is off on a read-only seat, in the sentence shape Alert History's Dismiss buttons use: what the
    /// seat cannot do, then how to connect with a login that can.
    /// </summary>
    public const string ReadOnlyRunTip = "This viewer is connected read-only, so it cannot run collectors. " + ViewerReadOnlyException.HowToConnectAsAdmin;

    /// <summary>#8103: the tooltip of "Purge Now..." when it is off on a read-only seat.</summary>
    public const string ReadOnlyPurgeTip = "This viewer is connected read-only, so it cannot run the retention purge. " + ViewerReadOnlyException.HowToConnectAsAdmin;

    /// <summary>
    /// #8103: the state of "Run all collectors now", applied as the tab is built and whenever the server's connection state is redrawn, so a
    /// read-only seat sees the button off with the reason before it clicks. The seat is said first: no reconnect of the server makes a read-only
    /// seat able to write. A writable seat's button is off with <see cref="ViewerServerTab.OfflineRunTip"/> while the service has no connection
    /// to the server (a run would be skipped), and off with its ready tip while a run is in flight.
    /// </summary>
    /// <param name="readOnly"><see cref="ViewerDataService.IsReadOnly"/>.</param>
    /// <param name="serverDown">Whether the service's connect to this server has failed.</param>
    /// <param name="runActive">Whether a run is in flight.</param>
    public static (bool IsEnabled, string ToolTip) RunAllState(bool readOnly, bool serverDown, bool runActive) =>
        readOnly ? (false, ReadOnlyRunTip)
        : serverDown ? (false, ViewerServerTab.OfflineRunTip)
        : (!runActive, RunAllReadyTip);

    /// <summary>
    /// #8103: a row's "Run now": off with the reason on a read-only seat, else on with its tip. It does not follow the server's connection;
    /// a click while the server is offline says so on the status line.
    /// </summary>
    public static (bool IsEnabled, string ToolTip) RunOneState(bool readOnly) =>
        readOnly ? (false, ReadOnlyRunTip) : (true, RunOneReadyTip);

    /// <summary>#8103: "Purge Now...": off with the reason on a read-only seat, else on with no tooltip.</summary>
    public static (bool IsEnabled, string? ToolTip) PurgeState(bool readOnly) =>
        readOnly ? (false, ReadOnlyPurgeTip) : (true, null);

    /// <summary>
    /// The status line for a finished command. A failed run reads "Not run: " and the service's own error, else its result status,
    /// else "The run failed."; a finished one reads "Finished." and the counts and skipped collectors from the service's result.
    /// A null result is the Viewer having stopped waiting (<see cref="StillRunningText"/>).
    /// </summary>
    public static string Describe(CommandResult? result, out bool succeeded)
    {
        succeeded = false;
        if (result is null)
        {
            return StillRunningText;
        }

        if (result.Status != ViewerDataService.StatusSucceeded)
        {
            var error = ReadString(result.ResultJson, "error");
            return "Not run: " + (!string.IsNullOrEmpty(error) ? error : !string.IsNullOrEmpty(result.ResultStatus) ? result.ResultStatus : "The run failed.");
        }

        succeeded = true;
        var detail = DescribeResult(result.ResultJson);
        return detail.Length == 0 ? "Finished." : "Finished. " + detail;
    }

    /// <summary>The counts and skipped collectors of a result: "3 collectors run, 120 rows stored. x skipped: reason."</summary>
    public static string DescribeResult(string? resultJson)
    {
        if (string.IsNullOrWhiteSpace(resultJson))
        {
            return "";
        }

        var parts = new List<string>();
        try
        {
            using var doc = JsonDocument.Parse(resultJson);
            if (doc.RootElement.ValueKind != JsonValueKind.Object)
            {
                return "";
            }

            if (doc.RootElement.TryGetProperty("collectorsRun", out var run) && run.ValueKind == JsonValueKind.Number && run.TryGetInt32(out var count))
            {
                var rows = doc.RootElement.TryGetProperty("rows", out var r) && r.ValueKind == JsonValueKind.Number && r.TryGetInt64(out var n) ? n : 0;
                parts.Add(string.Create(CultureInfo.InvariantCulture, $"{count} collector{(count == 1 ? "" : "s")} run, {rows} rows stored."));
            }

            if (doc.RootElement.TryGetProperty("skipped", out var skipped) && skipped.ValueKind == JsonValueKind.Array)
            {
                foreach (var item in skipped.EnumerateArray())
                {
                    if (item.ValueKind == JsonValueKind.Object
                        && item.TryGetProperty("collector", out var c) && c.ValueKind == JsonValueKind.String
                        && item.TryGetProperty("reason", out var why) && why.ValueKind == JsonValueKind.String)
                    {
                        parts.Add(c.GetString() + " skipped: " + why.GetString() + ".");
                    }
                }
            }
        }
        catch (JsonException)
        {
            /* The service wrote it; if it is not JSON the line shows the status alone, as the web does. */
        }

        return string.Join(" ", parts);
    }

    private static string? ReadString(string? json, string property)
    {
        if (string.IsNullOrWhiteSpace(json))
        {
            return null;
        }

        try
        {
            using var doc = JsonDocument.Parse(json);
            return doc.RootElement.ValueKind == JsonValueKind.Object
                && doc.RootElement.TryGetProperty(property, out var v) && v.ValueKind == JsonValueKind.String
                    ? v.GetString()
                    : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }
}
