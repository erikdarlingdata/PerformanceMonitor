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
using System.Net;
using System.Text;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Service.Reports;

/// <summary>
/// Renders a <see cref="DailyHealthReportData"/> two ways (#7041): short plain text for Slack, Teams and the generic webhook, and
/// HTML for email. Both leave out empty sections and give a healthy server with nothing to report one line.
///
/// <para>The text stays inside <see cref="TextLimit"/>, which is under the smallest of those channels' limits (Slack's section text
/// object is 3000 characters), and says "and N more servers" when it had to stop. The HTML uses inline CSS only, with no scripts and
/// no external images, and every collected string (server, database and job names, alert text) goes through
/// <see cref="WebUtility.HtmlEncode(string?)"/>.</para>
/// </summary>
internal static class DailyHealthReportRenderer
{
    /// <summary>The longest text this renders: Slack's 3000-character text object, less room for the sender's own framing.</summary>
    public const int TextLimit = 2800;

    /// <summary>How many items one text section lists before it says "and N more".</summary>
    private const int TextItemCap = 5;

    /// <summary>How many characters of a job's failure message the report keeps.</summary>
    private const int MessageCap = 160;

    public static string Subject(DailyHealthReportData data) =>
        string.Create(CultureInfo.InvariantCulture,
            $"{data.Title}: {data.Servers.Count} servers, {data.WarningCount} warning, {data.CriticalCount} critical, {data.LocalDate:yyyy-MM-dd}");

    private static string KindLabel(string kind) => kind switch
    {
        "full_backup" => "No recent full backup",
        "log_backup" => "No recent log backup",
        "checkdb" => "No recent CHECKDB",
        _ => kind,
    };

    private static string Clip(string? text, int max)
    {
        if (string.IsNullOrWhiteSpace(text)) return "";
        var flat = text.Replace('\r', ' ').Replace('\n', ' ').Trim();
        return flat.Length <= max ? flat : flat[..McpHelpers.TextElementCutLength(flat, max - 1)] + "…";
    }

    private static string Utc(DateTime value) => value.ToString("MM-dd HH:mm", CultureInfo.InvariantCulture) + " UTC";

    private static string Gb(double value) => value.ToString("0.#", CultureInfo.InvariantCulture);

    /* ───────────────────────────── text ───────────────────────────── */

    public static string Text(DailyHealthReportData data)
    {
        var header = new StringBuilder();
        header.Append(Subject(data)).Append('\n');
        header.Append("24 hours to ").Append(Utc(data.PeriodEndUtc)).Append(". ")
              .Append(data.HealthyCount).Append(" healthy, ").Append(data.WarningCount).Append(" warning, ")
              .Append(data.CriticalCount).Append(" critical");
        if (data.OfflineCount > 0) header.Append(", ").Append(data.OfflineCount).Append(" offline");
        header.Append(". ").Append(data.AlertsFired).Append(data.AlertsFired == 1 ? " alert fired." : " alerts fired.");

        if (data.Servers.Count == 0)
        {
            header.Append("\nNo servers are registered.");
            return header.ToString();
        }

        var blocks = data.Servers.Select(TextBlock).ToList();
        var result = new StringBuilder(header.ToString());
        var shown = 0;
        foreach (var block in blocks)
        {
            var remaining = blocks.Count - shown - 1;
            /* Room is kept for the closing "and N more" line whenever servers would be left out after this block. */
            var reserve = remaining > 0 ? 40 : 0;
            if (result.Length + 1 + block.Length + reserve > TextLimit)
            {
                if (shown == 0)
                {
                    /* Not even the worst server fits whole: cut it rather than send a header with nothing under it. */
                    var room = Math.Max(0, TextLimit - result.Length - 1 - 40);
                    result.Append('\n').Append(block.Length <= room ? block : block[..McpHelpers.TextElementCutLength(block, Math.Max(0, room - 1))] + "…");
                    shown = 1;
                }
                break;
            }

            result.Append('\n').Append(block);
            shown++;
        }

        if (shown < blocks.Count)
        {
            var left = blocks.Count - shown;
            result.Append("\nand ").Append(left).Append(left == 1 ? " more server" : " more servers");
        }

        return result.ToString();
    }

    private static string TextBlock(DailyHealthServerReport server)
    {
        if (server.IsQuiet)
        {
            return server.Name + ": " + server.Band.ToLowerInvariant() + ", nothing to report";
        }

        var sb = new StringBuilder();
        sb.Append(server.Name).Append(" - ").Append(server.Band);
        if (server.Reasons.Count > 0)
        {
            sb.Append(": ").Append(Clip(string.Join("; ", server.Reasons), 200));
        }

        if (server.Alerts.Count > 0)
        {
            sb.Append("\n  Alerts: ").Append(Capped(server.Alerts.Select(a => a.Metric + " x" + a.Count.ToString(CultureInfo.InvariantCulture))));
        }

        if (server.FailedJobs.Count > 0)
        {
            sb.Append("\n  Failed jobs: ").Append(Capped(server.FailedJobs.Select(j =>
                j.JobName + " (" + j.Failures.ToString(CultureInfo.InvariantCulture) + ", last " + Utc(j.LastFailureUtc) + ")")));
        }

        foreach (var group in server.Protection.GroupBy(p => p.Kind))
        {
            sb.Append("\n  ").Append(KindLabel(group.Key)).Append(": ").Append(Capped(group.Select(p => p.Database)));
        }

        if (server.LowDisk.Count > 0)
        {
            sb.Append("\n  Low disk: ").Append(Capped(server.LowDisk.Select(d =>
                d.Volume + " " + Gb(d.FreeGb) + " GB free (" + d.FreePercent.ToString("0", CultureInfo.InvariantCulture) + "%)")));
        }

        if (server.CollectionProblems.Count > 0)
        {
            sb.Append("\n  Collection: ").Append(Capped(server.CollectionProblems));
        }

        return sb.ToString();
    }

    private static string Capped(IEnumerable<string> items)
    {
        var list = items.ToList();
        var text = string.Join(", ", list.Take(TextItemCap));
        return list.Count > TextItemCap ? text + " and " + (list.Count - TextItemCap).ToString(CultureInfo.InvariantCulture) + " more" : text;
    }

    /* ───────────────────────────── html ───────────────────────────── */

    private static string E(string? text) => WebUtility.HtmlEncode(text ?? "");

    private static string BandColour(string band) => band switch
    {
        "Critical" => "#c0392b",
        "Offline" => "#7f8c8d",
        "Warning" => "#d68910",
        _ => "#1e8449",
    };

    private const string CellStyle = "padding:4px 8px;border:1px solid #dddddd;vertical-align:top;font-size:13px;";

    public static string Html(DailyHealthReportData data)
    {
        var sb = new StringBuilder();
        sb.Append("<html><body style=\"font-family:Segoe UI,Arial,sans-serif;color:#222222;\">");
        sb.Append("<h2 style=\"margin:0 0 4px 0;\">").Append(E(data.Title)).Append("</h2>");
        sb.Append("<p style=\"margin:0 0 12px 0;color:#555555;\">")
          .Append(E(data.LocalDate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)))
          .Append(", the 24 hours to ").Append(E(Utc(data.PeriodEndUtc))).Append("</p>");
        sb.Append("<p style=\"margin:0 0 12px 0;\"><b>").Append(data.Servers.Count).Append("</b> servers: ")
          .Append(data.HealthyCount).Append(" healthy, ").Append(data.WarningCount).Append(" warning, ")
          .Append(data.CriticalCount).Append(" critical");
        if (data.OfflineCount > 0) sb.Append(", ").Append(data.OfflineCount).Append(" offline");
        sb.Append(". <b>").Append(data.AlertsFired).Append("</b> ").Append(data.AlertsFired == 1 ? "alert" : "alerts").Append(" fired.</p>");

        if (data.Servers.Count == 0)
        {
            sb.Append("<p>No servers are registered.</p></body></html>");
            return sb.ToString();
        }

        foreach (var server in data.Servers)
        {
            if (server.IsQuiet)
            {
                sb.Append("<p style=\"margin:4px 0;font-size:13px;\"><b>").Append(E(server.Name)).Append("</b> ")
                  .Append("<span style=\"color:").Append(BandColour(server.Band)).Append(";\">").Append(E(server.Band))
                  .Append("</span>, nothing to report.</p>");
                continue;
            }

            sb.Append("<table style=\"border-collapse:collapse;margin:12px 0;width:100%;max-width:760px;\">");
            sb.Append("<tr><th colspan=\"2\" style=\"text-align:left;padding:6px 8px;background:").Append(BandColour(server.Band))
              .Append(";color:#ffffff;font-size:14px;\">").Append(E(server.Name)).Append(" - ").Append(E(server.Band)).Append("</th></tr>");

            if (server.Reasons.Count > 0)
            {
                Row(sb, "Why", string.Join("; ", server.Reasons.Select(E)));
            }

            if (server.Alerts.Count > 0)
            {
                Row(sb, "Alerts", string.Join("<br/>", server.Alerts.Select(a => E(a.Metric) + " x" + a.Count.ToString(CultureInfo.InvariantCulture))));
            }

            if (server.FailedJobs.Count > 0)
            {
                Row(sb, "Failed jobs", string.Join("<br/>", server.FailedJobs.Select(j =>
                    E(j.JobName) + " - " + j.Failures.ToString(CultureInfo.InvariantCulture) + (j.Failures == 1 ? " failure" : " failures")
                    + ", last " + E(Utc(j.LastFailureUtc))
                    + (string.IsNullOrWhiteSpace(j.Message) ? "" : ": " + E(Clip(j.Message, MessageCap))))));
            }

            foreach (var group in server.Protection.GroupBy(p => p.Kind))
            {
                Row(sb, KindLabel(group.Key), string.Join("<br/>", group.Select(p => E(p.Database) + (string.IsNullOrWhiteSpace(p.Text) ? "" : " - " + E(p.Text)))));
            }

            if (server.LowDisk.Count > 0)
            {
                Row(sb, "Low disk", string.Join("<br/>", server.LowDisk.Select(d =>
                    E(d.Volume) + " - " + Gb(d.FreeGb) + " GB free of " + Gb(d.TotalGb) + " GB (" + d.FreePercent.ToString("0", CultureInfo.InvariantCulture) + "%)")));
            }

            if (server.CollectionProblems.Count > 0)
            {
                Row(sb, "Collection", string.Join("<br/>", server.CollectionProblems.Select(E)));
            }

            sb.Append("</table>");
        }

        sb.Append("</body></html>");
        return sb.ToString();
    }

    /// <summary>One label/value row. <paramref name="valueHtml"/> must already be encoded by the caller.</summary>
    private static void Row(StringBuilder sb, string label, string valueHtml) =>
        sb.Append("<tr><td style=\"").Append(CellStyle).Append("width:140px;font-weight:bold;\">").Append(E(label)).Append("</td><td style=\"")
          .Append(CellStyle).Append("\">").Append(valueHtml).Append("</td></tr>");
}
