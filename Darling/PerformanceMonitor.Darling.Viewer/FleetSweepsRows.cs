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
using System.Text.Json.Nodes;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>One row of the Fleet Sweeps timeline (#6390): a sweep, as the web's timeline row words it.</summary>
public sealed record FleetSweepRunRow(
    string SweepId, string SweptAtLocal, string Flags, string ServersText, string BandsText, string WatchText, string WouldHavePagedText)
{
    /// <summary>The sweep instant in UTC, the "Swept at" column's sort key (<see cref="SweptAtLocal"/> is display text); MinValue when the node had none.</summary>
    public DateTime SweptAtUtc { get; init; }
}

public sealed record FleetSweepTransitionRow(string Server, string From, string To, string Reason);

public sealed record FleetSweepWouldHavePagedRow(string Server, string Family, string Evidence);

public sealed record FleetSweepVerdictRow(string Server, string Previous, string Band, string Reason);

public sealed record FleetSweepWatchRow(
    string Server, string Item, string State, string Position, string FirstSeen, string LastSeen, string Condition, string ConditionTooltip)
{
    /// <summary>The first-seen instant in UTC, the column's sort key (<see cref="FirstSeen"/> is display text); MinValue when the node had none.</summary>
    public DateTime FirstSeenUtc { get; init; }

    /// <summary>The last-seen instant in UTC, the column's sort key (<see cref="LastSeen"/> is display text); MinValue when the node had none.</summary>
    public DateTime LastSeenUtc { get; init; }
}

/// <summary>
/// Turns the JSON nodes <see cref="ViewerDataService"/> returns (the web's own wire shape, built by
/// <c>FleetSweepPresentation</c>) into the rows the Fleet Sweeps tab shows, with the wording of the web's
/// <c>sweeps.js</c>. A pure mapping, so a test can run it without a window.
/// </summary>
internal static class FleetSweepsRows
{
    /// <summary>The watch-item state labels (the web's <c>SWEEP_WATCH_STATE_LABELS</c>).</summary>
    internal static string StateLabel(string state) => state switch
    {
        "pending" => "Pending",
        "open" => "Open",
        "carried" => "Carried",
        "closed" => "Closed",
        _ => state,
    };

    /// <summary>The cadence line the web's sweeps page shows (its <c>renderCadence</c>): "sweeps every N min", or the disabled
    /// wording, from the alert settings' fleet_sweep group; blank when the store holds no settings row yet (as the page leaves it).</summary>
    internal static string CadenceText(AlertSettingsRow? settings) =>
        settings is null ? ""
        : settings.FleetSweepEnabled
            ? "sweeps every " + settings.FleetSweepIntervalMinutes.ToString("N0", CultureInfo.CurrentCulture) + " min"
            : "sweeps disabled (fleet_sweep.enabled: false)";

    private static JsonObject Obj(JsonNode? node) => node as JsonObject ?? new JsonObject();

    private static IEnumerable<JsonObject> Objects(JsonNode? node) =>
        node is JsonArray array ? array.OfType<JsonObject>() : Enumerable.Empty<JsonObject>();

    /// <summary>The length of a JSON array, 0 for anything else (the web's <c>count</c>).</summary>
    internal static int Count(JsonNode? node) => node is JsonArray array ? array.Count : 0;

    private static string Str(JsonNode? node, string key) =>
        node is JsonObject o && o[key] is JsonValue v && v.TryGetValue<string>(out var s) ? s : "";

    /* A node the builders made from an int holds an int, which TryGetValue<long> refuses; the number's own text parses either way. */
    private static long Int(JsonNode? node, string key) =>
        node is JsonObject o && o[key] is JsonValue v && v.GetValueKind() == System.Text.Json.JsonValueKind.Number
            && long.TryParse(v.ToJsonString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var n) ? n : 0;

    internal static bool Bool(JsonNode? node, string key, bool whenAbsent) =>
        node is JsonObject o && o[key] is JsonValue v && v.TryGetValue<bool>(out var b) ? b : whenAbsent;

    private static string Num(long n) => n.ToString("N0", CultureInfo.CurrentCulture);

    /// <summary>An ISO instant as a UTC DateTime, for a column's sort key; MinValue when it is not one.</summary>
    internal static DateTime Utc(string iso) =>
        DateTime.TryParse(iso, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var t)
            ? DateTime.SpecifyKind(t, DateTimeKind.Utc)
            : DateTime.MinValue;

    /// <summary>An ISO instant (as the builders write them) as fleet-wide text, labelled with its zone; empty when it is not one.
    /// A sweep belongs to the whole fleet and no server, so it never takes the active server tab's clock: Local mode reads this
    /// machine's zone and every other mode reads UTC (<see cref="ViewerTimeHelper.FormatFleetWide(DateTime, string)"/>).</summary>
    internal static string Local(string iso) => Local(iso, ViewerTimeHelper.CurrentDisplayMode);

    /// <summary><see cref="Local(string)"/> for an explicit display mode.</summary>
    internal static string Local(string iso, TimeDisplayMode mode) =>
        DateTime.TryParse(iso, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var t)
            ? ViewerTimeHelper.FormatFleetWide(DateTime.SpecifyKind(t, DateTimeKind.Utc), "yyyy-MM-dd HH:mm:ss", mode)
            : "";

    private static string BandsText(JsonObject report)
    {
        var bands = Obj(Obj(report["fleet"])["bands"]);
        return string.Join("  ", bands.OrderBy(b => b.Key, StringComparer.Ordinal).Select(b => $"{b.Value} {b.Key}"));
    }

    /// <summary>The timeline rows, in the order the node carries them (newest first).</summary>
    internal static List<FleetSweepRunRow> Runs(JsonObject timeline)
    {
        var rows = new List<FleetSweepRunRow>();
        foreach (var s in Objects(timeline["sweeps"]))
        {
            var report = Obj(s["report"]);
            var watch = Obj(report["watch"]);
            var flags = new List<string>();
            if (!Bool(s, "instruments_alive", true)) flags.Add("INSTRUMENTS DEAD");
            if (!Bool(s, "alerts_enabled", true)) flags.Add("MUTED");
            var opened = Count(watch["opened"]);
            var closed = Count(watch["closed"]);
            var watchText = string.Join(" ", new[] { opened > 0 ? $"+{opened} watch" : "", closed > 0 ? $"−{closed} watch" : "" }.Where(t => t.Length > 0));
            var whp = Count(report["would_have_paged"]);
            rows.Add(new FleetSweepRunRow(
                Str(s, "sweep_id"),
                Local(Str(s, "swept_at")),
                string.Join(", ", flags),
                $"{Num(Int(s, "servers_reported"))}/{Num(Int(s, "servers_expected"))} reported",
                BandsText(report),
                watchText,
                whp > 0 ? $"{whp} would have paged" : "")
            {
                SweptAtUtc = Utc(Str(s, "swept_at")),
            });
        }

        return rows;
    }

    /// <summary>The per-server verdict rows (previous band beside band, the reason for each).</summary>
    internal static List<FleetSweepVerdictRow> Verdicts(JsonObject detail) =>
        Objects(detail["verdicts"])
            .Select(v => new FleetSweepVerdictRow(
                Str(v, "server"),
                Str(v, "previous_band") is { Length: > 0 } p ? p : "—",
                Str(v, "band"),
                Str(v, "reason") is { Length: > 0 } r ? r : "—"))
            .ToList();

    /// <summary>The would-have-paged ledger rows; null when the sweep carries no ledger (an alerts-on sweep: no check was made).</summary>
    internal static List<FleetSweepWouldHavePagedRow>? WouldHavePaged(JsonObject detail) =>
        detail["would_have_paged"] is JsonArray
            ? Objects(detail["would_have_paged"])
                .Select(w => new FleetSweepWouldHavePagedRow(
                    Str(w, "server") is { Length: > 0 } n ? n : "server " + Int(w, "server_id"),
                    Str(w, "family"),
                    w["evidence"]?.ToJsonString() ?? ""))
                .ToList()
            : null;

    /// <summary>The band transitions of the embedded report's <c>changes</c> block.</summary>
    internal static List<FleetSweepTransitionRow> Transitions(JsonObject detail) =>
        Objects(Obj(Obj(detail["report"])["changes"])["band_transitions"])
            .Select(t => new FleetSweepTransitionRow(
                Str(t, "server"), Str(t, "from"), Str(t, "to"), Str(t, "reason") is { Length: > 0 } r ? r : "—"))
            .ToList();

    /// <summary>The note under the changes grid: the web's three cases (first sweep, nothing changed) plus the new and departed server lines.</summary>
    internal static string ChangesNote(JsonObject detail)
    {
        var report = Obj(detail["report"]);
        var changes = Obj(report["changes"]);
        var transitions = Count(changes["band_transitions"]);
        var newServers = NameList(changes["new_servers"]);
        var departed = NameList(changes["departed_servers"]);
        if (Bool(report, "no_previous_sweep", false))
        {
            return "First sweep — there is no previous sweep to diff against, so this document states absolutes.";
        }

        if (transitions == 0 && newServers.Length == 0 && departed.Length == 0)
        {
            return "No band changes, arrivals or departures since the previous sweep.";
        }

        var lines = new List<string>();
        if (newServers.Length > 0) lines.Add("New servers: " + newServers);
        if (departed.Length > 0) lines.Add("Departed servers: " + departed);
        return string.Join("\n", lines);
    }

    private static string NameList(JsonNode? node) =>
        node is JsonArray array ? string.Join(", ", array.Select(n => n?.ToString() ?? "")) : "";

    /// <summary>The header summary line: the span, the reported count, the band counts, watch opened/closed and the ledger count.</summary>
    internal static string DetailSummary(JsonObject detail)
    {
        var report = Obj(detail["report"]);
        var parts = new List<string>
        {
            $"span {Local(Str(detail, "span_start"))} → {Local(Str(detail, "span_end"))}",
            $"{Num(Int(detail, "servers_reported"))} / {Num(Int(detail, "servers_expected"))} servers reported",
        };
        var bands = BandsText(report);
        if (bands.Length > 0) parts.Add(bands);
        var watch = Obj(report["watch"]);
        parts.Add($"watch opened / closed {Num(Count(watch["opened"]))} / {Num(Count(watch["closed"]))}");
        if (detail["would_have_paged"] is JsonArray ledger) parts.Add($"would have paged {Num(ledger.Count)}");
        if (detail["previous_sweep_id"] is null) parts.Add("no previous sweep");
        return string.Join("   |   ", parts);
    }

    /// <summary>The liveness block as text: the verdict, the counters and every note, as the web's liveness table words them.</summary>
    internal static string Liveness(JsonObject detail)
    {
        var lv = Obj(detail["instrument_liveness"]);
        var lines = new List<string>
        {
            "Verdict: " + (Bool(detail, "instruments_alive", true) ? "alive" : "DEAD"),
            "Service started: " + Local(Str(lv, "service_started_at")),
        };
        if (Bool(lv, "baseline_reset", false)) lines.Add("Pass-counter baseline: reset by restart — advance not judgeable this sweep");
        var previous = lv["alert_passes_previous"] is JsonValue ? $" (previous {Num(Int(lv, "alert_passes_previous"))})" : " (no baseline)";
        var advancing = lv["alert_passes_advancing"] is JsonValue av && av.TryGetValue<bool>(out var ab) ? (ab ? " — advancing" : " — frozen") : "";
        lines.Add("Alert passes: " + Num(Int(lv, "alert_passes_total")) + previous + advancing);
        lines.Add("Alert read failures: " + Num(Int(lv, "alert_read_failures_total")));
        var faults = Objects(lv["server_read_faults"]).ToList();
        if (faults.Count > 0)
        {
            lines.Add(faults.Count + " server read fault(s): " + string.Join("; ", faults.Select(f => $"{Str(f, "server")} ({Str(f, "error")})")));
        }

        foreach (var note in lv["notes"] is JsonArray notes ? notes : new JsonArray())
        {
            lines.Add("• " + note);
        }

        return string.Join("\n", lines);
    }

    /// <summary>The watch-item rows; the position reads against the bars the node carries, never a fixed number.</summary>
    internal static List<FleetSweepWatchRow> WatchItems(JsonObject node)
    {
        var entryBar = Int(node, "entry_bar_sweeps");
        var exitBar = Int(node, "exit_bar_sweeps");
        return Objects(node["items"])
            .Select(w =>
            {
                var state = Str(w, "state");
                var hits = Int(w, "consecutive_hits");
                var misses = Int(w, "consecutive_misses");
                var position = state == "pending" ? $"{hits} of {entryBar} hits to open"
                    : state == "closed" ? "closed"
                    : misses > 0 ? $"{misses} of {exitBar} quiet to close"
                    : $"{hits} consecutive hit{(hits == 1 ? "" : "s")}";
                /* The fleet-scope sentinel has no server name; "server 0" would be a fabrication. */
                var server = Bool(w, "fleet_scope", false) ? "Fleet"
                    : Str(w, "server") is { Length: > 0 } n ? n : "server " + Int(w, "server_id");
                var condition = Str(w, "condition");
                var evidence = w["evidence"]?.ToJsonString(new System.Text.Json.JsonSerializerOptions { WriteIndented = true });
                return new FleetSweepWatchRow(
                    server, Str(w, "item"), StateLabel(state), position,
                    Local(Str(w, "first_seen_at")), Local(Str(w, "last_seen_at")),
                    condition, evidence is null ? condition : condition + "\n" + evidence)
                {
                    FirstSeenUtc = Utc(Str(w, "first_seen_at")),
                    LastSeenUtc = Utc(Str(w, "last_seen_at")),
                };
            })
            .ToList();
    }
}
