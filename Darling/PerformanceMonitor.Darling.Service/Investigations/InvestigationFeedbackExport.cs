/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */


using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Investigations;

/// <summary>What one export wrote (#6236): the cases in the file, the cases a live practice replays that were left out, whether the
/// filter held more than <see cref="InvestigationRequestStore.FeedbackExportLimit"/> cases, and whether the statement check refused
/// the export (the stream then holds a partial file the caller must not keep).</summary>
public sealed record InvestigationExportOutcome(int Cases, int HeldBack, bool Truncated, bool Refused);

/// <summary>
/// The export of the investigation cases that have feedback (#6236, part of #5632): the test set an operator replays their own
/// agent on. One JSON object per case, one case per line: <c>request_id</c>, <c>alert</c> (the request: server, family, first metric,
/// when and how often it fired), <c>brief</c> (the alert and the evidence the agent was given), <c>report</c> (what it wrote) and
/// <c>feedback</c> (the verdict, corrected verdict, missed step and comment people recorded).
///
/// <para><b>One pipeline for the command line and the web download.</b> Both call <see cref="WriteAsync"/>, so the same rows at the
/// same <c>mcp.dataDetail</c> give the same bytes. A case goes through <see cref="InvestigationToolCore.ReportsAnswer"/> at that
/// level (free text, evidence, a stored brief weaker than the level, and a person's login are masked as
/// <c>get_investigation_reports</c> masks them), then through the credential-statement check, then through the whole-answer masking
/// pass the MCP host applies. A line the check cannot read refuses the whole export, so a partial file never reads as a smaller test
/// set. Cases are read a page at a time and written one at a time.</para>
///
/// <para>A case a live practice request replays is left out, as the investigations arm's report read leaves out its answer, and it is
/// left out of every other case's <c>earlier_reports</c> and <c>prior_links</c> (<see cref="InvestigationReplayFilter"/>).</para>
/// </summary>
public static class InvestigationFeedbackExport
{
    /// <summary>The verdicts a person may record (the V191 CHECK).</summary>
    public static readonly IReadOnlyList<string> Verdicts = InvestigationFeedbackRules.Verdicts;

    /// <summary>The sentence a refused export answers with: the statement check's own, as every other surface words it.</summary>
    public static string RefusalSentence => SensitiveStatements.JsonRefusal;

    /// <summary>Checks a verdict filter; null when it is blank or one of <see cref="Verdicts"/>, else the refusal in plain words.</summary>
    public static string? VerdictRefusal(string? verdict) =>
        string.IsNullOrWhiteSpace(verdict) || Verdicts.Contains(verdict.Trim(), StringComparer.Ordinal)
            ? null
            : $"A verdict filter is one of {string.Join(", ", Verdicts)}; '{verdict}' is not.";

    /// <summary>The refusal for a filter neither path can use: a verdict that is not one, or a window that ends before it starts. Null when usable.</summary>
    public static string? FilterRefusal(DateTime? sinceUtc, DateTime? untilUtc, string? verdict)
    {
        if (sinceUtc is { } from && untilUtc is { } to && from >= to)
            return "since must be before until.";
        return VerdictRefusal(verdict);
    }

    /// <summary>
    /// Finds the one registered server <paramref name="name"/> names, over every registry row (a server that was disabled still has
    /// cases). The name is the storage name or the display name, ignoring case; a partial name matches nothing, because a file meant
    /// for one server must never hold another's cases. Two registrations that carry the name are refused with their storage names.
    /// A blank name is no server filter.
    /// </summary>
    public static async Task<(int? ServerId, string? Label, string? Error)> ResolveServerAsync(
        NpgsqlDataSource dataSource, string? name, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        if (string.IsNullOrWhiteSpace(name))
            return (null, null, null);
        var wanted = name.Trim();
        var all = new List<(int Id, string Name, string? Display)>();
        await using (var connection = await dataSource.OpenConnectionAsync(cancellationToken))
        await using (var command = new NpgsqlCommand("SELECT server_id, server_name, display_name FROM collect.servers ORDER BY server_name", connection))
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
                all.Add((reader.GetInt32(0), reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetString(2)));
        }

        var hits = all.Where(s =>
            string.Equals(s.Name, wanted, StringComparison.OrdinalIgnoreCase)
            || string.Equals(s.Display?.Trim(), wanted, StringComparison.OrdinalIgnoreCase)).ToList();
        if (hits.Count == 1)
            return (hits[0].Id, string.IsNullOrWhiteSpace(hits[0].Display) ? hits[0].Name : $"{hits[0].Name} ({hits[0].Display})", null);
        if (hits.Count > 1)
            return (null, null, $"'{wanted}' names {hits.Count} registered servers: {string.Join(", ", hits.Select(h => h.Name))}. Use one storage name exactly.");
        return (null, null, $"No registered server is named '{wanted}'. Use a server's storage name or display name exactly; a part of a name is not enough.");
    }

    /// <summary>
    /// Writes the cases of the filter to <paramref name="output"/> as UTF-8 JSON lines (no byte order mark, a line feed after each),
    /// newest request first, one case at a time. Returns what it wrote; when <see cref="InvestigationExportOutcome.Refused"/> is set
    /// the caller discards what the stream holds.
    /// </summary>
    public static async Task<InvestigationExportOutcome> WriteAsync(
        InvestigationRequestStore store, DateTime? sinceUtc, DateTime? untilUtc, int? serverId, string? verdict,
        McpDataDetail mode, PseudonymKey? key, Stream output, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(output);
        var replayed = await store.LivePracticeReplaysAsync(cancellationToken);
        var written = 0;
        var heldBack = 0;
        var refused = false;
        var writer = new StreamWriter(output, new UTF8Encoding(false), 16 * 1024, leaveOpen: true) { NewLine = "\n" };
        await using (writer.ConfigureAwait(false))
        {
            var truncated = await store.ReadFeedbackCasesAsync(
                sinceUtc, untilUtc, serverId, string.IsNullOrWhiteSpace(verdict) ? null : verdict.Trim(),
                InvestigationRequestStore.FeedbackExportLimit,
                async export =>
                {
                    if (refused)
                        return;
                    if (replayed.Contains(export.Request.RequestId))
                    {
                        heldBack++;
                        return;
                    }

                    /* A brief stored before the practice started still lists the replayed case's report: the one shared filter takes it out. */
                    var served = replayed.Count == 0 ? export : export with { Brief = InvestigationReplayFilter.WithoutReplayed(export.Brief, replayed) };
                    if (Line(served, mode, key) is not { } line)
                    {
                        refused = true;
                        return;
                    }

                    await writer.WriteLineAsync(line.AsMemory(), cancellationToken).ConfigureAwait(false);
                    written++;
                },
                cancellationToken);
            await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
            return new InvestigationExportOutcome(written, heldBack, truncated, refused);
        }
    }

    /// <summary>One case as one JSON line, masked at <paramref name="mode"/> and checked for credential statements; null when a check
    /// cannot read the line (the export is refused, never written with the line raw).</summary>
    public static string? Line(InvestigationFeedbackCase export, McpDataDetail mode, PseudonymKey? key)
    {
        ArgumentNullException.ThrowIfNull(export);
        var answer = JsonNode.Parse(InvestigationToolCore.ReportsAnswer(
            new InvestigationReportsRead(
                [export.Request], [export.Report], [], export.Brief, Feedback: export.Feedback, EvidenceByBasis: true),
            null, mode, key))!;
        var report = (JsonObject)answer["reports"]!.AsArray()[0]!.DeepClone();
        var feedback = report["feedback"]?.DeepClone() ?? new JsonArray();
        report.Remove("feedback");
        report.Remove("labels");
        var line = new JsonObject
        {
            ["request_id"] = export.Request.RequestId.ToString(),
            ["alert"] = answer["requests"]!.AsArray()[0]!.DeepClone(),
            ["brief"] = answer["brief"]?.DeepClone(),
            ["report"] = report,
            ["feedback"] = feedback,
        };

        var swept = DarlingWebStatementSweep.Apply(line.ToJsonString(McpHelpers.JsonOptions));
        if (swept.Refused)
            return null;
        var masked = McpDataDetailMasker.ForAnswer(mode, key).Answer(swept.Text);
        return string.Equals(masked, McpDataDetailMasker.Refusal, StringComparison.Ordinal) ? null : masked;
    }
}
