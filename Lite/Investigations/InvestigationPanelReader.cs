/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Investigations;

/// <summary>One line of the investigations panel's list (#5904): what <c>list_investigation_requests</c> answers, with the
/// times as local text. The claim time itself is in the journal, so the list carries the agent and the lease end and the
/// detail carries the claim time, as the Darling page does.</summary>
internal sealed record InvestigationPanelRequest(InvestigationRequestRow Row)
{
    public Guid RequestId => Row.RequestId;
    public string State => Row.State;
    public string ServerName => Row.ServerName;
    public string FirstMetric => Row.FirstMetric;
    public string Family => Row.Family;
    public int RouteId => Row.RouteId;
    public string Agent => Row.ClaimedBy ?? "";
    public int ClaimCount => Row.ClaimCount;
    public int AlertCount => Row.AlertCount;
    /* The UTC times are what the grid's time columns sort by (SortMemberPath), never their local text. */
    public DateTime OpenedAtUtc => Row.OpenedAtUtc;
    public DateTime? LeaseUntilUtc => Row.LeaseUntilUtc;
    public DateTime? ReportedAtUtc => Row.ReportedAtUtc;
    public string Opened => InvestigationPanelReader.LocalText(Row.OpenedAtUtc);
    public string LeaseUntil => InvestigationPanelReader.LocalText(Row.LeaseUntilUtc);
    public string Reported => InvestigationPanelReader.LocalText(Row.ReportedAtUtc);
}

/// <summary>One report under a request, as the data-detail masker left it. The fields are text for a read-only box: a
/// masked field is its JSON.</summary>
internal sealed record InvestigationPanelReport(
    string AgentId, string Submitted, string Verdict, bool RootCauseFound, string Summary, string RootCause, string Evidence);

/// <summary>One journal row under a request (claims, releases, lapses, limits, refusals), newest first.</summary>
internal sealed record InvestigationPanelJournalRow(string Time, string Event, string Actor, string Reason, string Detail, DateTime TimeUtc);

/// <summary>The detail pane of one request: its reports, journal and stored brief, all at full detail. <see cref="Claimed"/> is the newest <c>claimed</c> journal time, or empty when never claimed.</summary>
internal sealed record InvestigationPanelDetail(
    InvestigationRequestRow Request,
    string Claimed,
    IReadOnlyList<InvestigationPanelReport> Reports,
    IReadOnlyList<InvestigationPanelJournalRow> Journal,
    string? Brief,
    bool JournalTruncated);

/// <summary>
/// The read side of Lite's investigations panel (#5904), read-only over <see cref="IInvestigationRequestStore"/>: the list
/// in the MCP list order (<c>opened_at DESC, request_id</c>, which the store's list read already gives) and, per request,
/// the same <c>ReportsAnswer</c> the <c>get_investigation_reports</c> tool builds, at <see cref="McpDataDetail.Full"/>.
/// The panel is the operator's own window, like the Darling web page and the Darling Viewer, so the report, the stored
/// brief and the journal are shown in full; <c>mcp_data_detail</c> masks agents over MCP only and is never consulted here.
/// Each call is its own store connection under the store's read rule. Nothing here claims, releases or reports.
/// </summary>
internal sealed class InvestigationPanelReader
{
    /// <summary>The most requests one list shows (the MCP list tool's cap).</summary>
    internal const int ListLimit = InvestigationToolCore.MaxLimit;

    private readonly IInvestigationRequestStore _store;
    private readonly Func<DateTime> _utcNow;

    internal InvestigationPanelReader(IInvestigationRequestStore store, Func<DateTime>? utcNow = null)
    {
        _store = store;
        _utcNow = utcNow ?? (() => DateTime.UtcNow);
    }

    /// <summary>The requests, newest opened first, optionally by state and family; <c>Truncated</c> says more than
    /// <see cref="ListLimit"/> matched.</summary>
    internal async Task<(IReadOnlyList<InvestigationPanelRequest> Requests, bool Truncated)> ListAsync(
        string? state, string? family, CancellationToken cancellationToken = default)
    {
        var rows = await _store.ListAsync(
            string.IsNullOrEmpty(state) ? null : state, null, string.IsNullOrEmpty(family) ? null : family,
            ListLimit + 1, cancellationToken);
        var truncated = rows.Count > ListLimit;
        return (rows.Take(ListLimit).Select(r => new InvestigationPanelRequest(r)).ToList(), truncated);
    }

    /// <summary>One request's detail, or null when it is gone (purged between the list and the pick).</summary>
    internal async Task<InvestigationPanelDetail?> ReadDetailAsync(Guid requestId, CancellationToken cancellationToken = default)
    {
        var read = await _store.ReadReportsAsync(
            requestId, null, _utcNow(), 1, InvestigationToolCore.JournalLimit, cancellationToken);
        return read.Requests.Count == 0 ? null : Detail(read, McpDataDetail.Full);
    }

    /// <summary>The detail of the first request of <paramref name="read"/> at <paramref name="mode"/>. The masked text comes
    /// from <see cref="InvestigationToolCore.ReportsAnswer"/>; the times and event names come from the read itself, row for row.</summary>
    internal static InvestigationPanelDetail Detail(InvestigationReportsRead read, McpDataDetail mode)
    {
        var answer = JsonNode.Parse(InvestigationToolCore.ReportsAnswer(read, null, mode))!.AsObject();
        var reportNodes = answer["reports"]!.AsArray();
        var journalNodes = answer["journal"]!.AsArray();

        var reports = new List<InvestigationPanelReport>();
        for (var i = 0; i < read.Reports.Count; i++)
        {
            var r = read.Reports[i];
            var node = reportNodes[i]!.AsObject();
            reports.Add(new InvestigationPanelReport(
                r.AgentId, LocalText(r.SubmittedAtUtc), Text(node["verdict"]), r.RootCauseFound,
                Withheld(node, Text(node["summary"])), Text(node["root_cause"]), Text(node["evidence"])));
        }

        var journal = new List<InvestigationPanelJournalRow>();
        for (var i = 0; i < read.Journal.Count; i++)
        {
            var j = read.Journal[i];
            var node = journalNodes[i]!.AsObject();
            journal.Add(new InvestigationPanelJournalRow(
                LocalText(j.EventTimeUtc), j.Event, Text(node["actor"]), Withheld(node, Text(node["reason"])), Text(node["detail"]), j.EventTimeUtc));
        }

        var claimed = read.Journal.Where(j => j.Event == "claimed").Select(j => (DateTime?)j.EventTimeUtc).Max();
        var brief = answer["brief"];
        return new InvestigationPanelDetail(
            read.Requests[0], LocalText(claimed), reports, journal, brief is null ? null : Text(brief), read.JournalTruncated);
    }

    /// <summary>A node as text: a string as it is, nothing as empty, anything else (a masked field is an object) as indented JSON.</summary>
    internal static string Text(JsonNode? node) => node switch
    {
        null => "",
        JsonValue v when v.TryGetValue(out string? s) => s ?? "",
        _ => node.ToJsonString(new JsonSerializerOptions { WriteIndented = true }),
    };

    /// <summary>A node the masker could not read comes back as <c>{"withheld": reason}</c>; the panel says so in the free-text field.</summary>
    private static string Withheld(JsonObject node, string text) =>
        node["withheld"] is { } w ? "(withheld) " + Text(w) : text;

    /// <summary>A UTC time as local text, empty when null.</summary>
    internal static string LocalText(DateTime? utc) =>
        utc is null ? "" : DateTime.SpecifyKind(utc.Value, DateTimeKind.Utc).ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
}
