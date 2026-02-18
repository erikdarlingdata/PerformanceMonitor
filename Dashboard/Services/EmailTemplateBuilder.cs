/*
 * Performance Monitor Dashboard
 * Copyright (c) 2026 Darling Data, LLC
 * Licensed under the MIT License - see LICENSE file for details
 */

using System;
using System.Net;
using System.Text;

namespace PerformanceMonitorDashboard.Services
{
    /// <summary>
    /// Builds HTML and plain-text email bodies for alert notifications.
    /// Static helper — no state, no dependencies.
    /// </summary>
    internal static class EmailTemplateBuilder
    {
        private const string EditionName = "Performance Monitor Dashboard";
        private const string FontStack = "-apple-system, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif";

        /// <summary>
        /// Builds both HTML and plain-text bodies for an alert email.
        /// </summary>
        public static (string HtmlBody, string PlainTextBody) BuildAlertEmail(
            string metricName,
            string serverName,
            string currentValue,
            string thresholdValue,
            AlertContext? context = null)
        {
            var utcNow = DateTime.UtcNow;
            var localNow = DateTime.Now;
            var (accentColor, badgeText) = GetSeverity(metricName);

            var html = BuildHtmlBody(metricName, serverName, currentValue,
                thresholdValue, utcNow, localNow, accentColor, badgeText, context: context);

            var plain = BuildPlainTextBody(metricName, serverName, currentValue,
                thresholdValue, utcNow, localNow, context);

            return (html, plain);
        }

        /// <summary>
        /// Builds both HTML and plain-text bodies for a test email.
        /// </summary>
        public static (string HtmlBody, string PlainTextBody) BuildTestEmail()
        {
            var localNow = DateTime.Now;
            const string accentColor = "#2eaef1";
            const string badgeText = "TEST";

            var html = BuildHtmlBody("Test Email", "", "",
                "", DateTime.UtcNow, localNow, accentColor, badgeText, isTest: true);

            var plain = $"{EditionName}\r\n" +
                        $"-------------------------------\r\n" +
                        $"This is a test email.\r\n" +
                        $"Sent at: {localNow:yyyy-MM-dd HH:mm:ss}\r\n";

            return (html, plain);
        }

        private static (string AccentColor, string BadgeText) GetSeverity(string metricName)
        {
            return metricName switch
            {
                "Blocking Detected" => ("#D97706", "ALERT"),
                "Deadlocks Detected" => ("#DC2626", "ALERT"),
                "High CPU" => ("#F59E0B", "WARNING"),
                "Poison Wait" => ("#DC2626", "CRITICAL"),
                "Long-Running Query" => ("#D97706", "WARNING"),
                "TempDB Space" => ("#D97706", "WARNING"),
                "Long-Running Job" => ("#D97706", "WARNING"),
                _ => ("#2eaef1", "INFO")
            };
        }

        private static string BuildHtmlBody(
            string metricName,
            string serverName,
            string currentValue,
            string thresholdValue,
            DateTime utcNow,
            DateTime localNow,
            string accentColor,
            string badgeText,
            bool isTest = false,
            AlertContext? context = null)
        {
            var sb = new StringBuilder(2048);

            /* HTML document start */
            sb.Append("<!DOCTYPE html>");
            sb.Append("<html lang=\"en\">");
            sb.Append("<head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\"></head>");
            sb.Append("<body style=\"margin:0;padding:0;background-color:#1a1a1a;\">");

            /* Outer wrapper table for centering */
            sb.Append("<table role=\"presentation\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\" width=\"100%\" style=\"background-color:#1a1a1a;\">");
            sb.Append("<tr><td align=\"center\" style=\"padding:24px 12px;\">");

            /* Inner content table - 600px */
            sb.Append("<table role=\"presentation\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\" width=\"600\" style=\"background-color:#252525;border-radius:6px;overflow:hidden;\">");

            /* Accent bar */
            sb.Append($"<tr><td style=\"height:4px;background-color:{accentColor};font-size:0;line-height:0;\">&nbsp;</td></tr>");

            /* Header */
            sb.Append("<tr><td style=\"padding:20px 24px 12px 24px;\">");
            sb.Append($"<table role=\"presentation\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\" width=\"100%\">");
            sb.Append("<tr><td>");
            sb.Append($"<span style=\"font-family:{FontStack};font-size:18px;font-weight:600;color:#FFFFFF;\">SQL Server Performance Monitor</span><br>");
            sb.Append($"<span style=\"font-family:{FontStack};font-size:12px;color:#808080;\">{WebUtility.HtmlEncode(EditionName)}</span>");
            sb.Append("</td></tr></table>");
            sb.Append("</td></tr>");

            /* Separator */
            sb.Append("<tr><td style=\"padding:0 24px;\"><table role=\"presentation\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\" width=\"100%\"><tr><td style=\"height:1px;background-color:#404040;font-size:0;line-height:0;\">&nbsp;</td></tr></table></td></tr>");

            /* Badge + metric name */
            sb.Append("<tr><td style=\"padding:16px 24px 8px 24px;\">");
            sb.Append($"<span style=\"display:inline-block;font-family:{FontStack};font-size:11px;font-weight:700;color:#FFFFFF;background-color:{accentColor};padding:3px 10px;border-radius:3px;letter-spacing:0.5px;\">{WebUtility.HtmlEncode(badgeText)}</span>");
            sb.Append($"<span style=\"font-family:{FontStack};font-size:16px;font-weight:600;color:#FFFFFF;padding-left:10px;\">{WebUtility.HtmlEncode(metricName)}</span>");
            sb.Append("</td></tr>");

            /* Data rows */
            sb.Append("<tr><td style=\"padding:8px 24px 20px 24px;\">");
            sb.Append($"<table role=\"presentation\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\" width=\"100%\" style=\"background-color:#333333;border-radius:4px;\">");

            if (isTest)
            {
                AppendDataRow(sb, "Status", "SMTP configuration is working correctly", false);
                AppendDataRow(sb, "Sent at", localNow.ToString("yyyy-MM-dd HH:mm:ss"), true);
            }
            else
            {
                AppendDataRow(sb, "Server", serverName, false);
                AppendDataRow(sb, "Current Value", currentValue, false);
                AppendDataRow(sb, "Threshold", thresholdValue, false);
                AppendDataRow(sb, "Time (UTC)", utcNow.ToString("yyyy-MM-dd HH:mm:ss"), false);
                AppendDataRow(sb, "Time (Local)", localNow.ToString("yyyy-MM-dd HH:mm:ss"), true);
            }

            sb.Append("</table>");
            sb.Append("</td></tr>");

            /* Detail section (blocking chains, deadlock participants) */
            if (context?.Details?.Count > 0)
            {
                AppendDetailSection(sb, context);
            }

            /* Attachment note */
            if (!string.IsNullOrEmpty(context?.AttachmentFileName))
            {
                sb.Append("<tr><td style=\"padding:4px 24px 12px 24px;\">");
                sb.Append($"<span style=\"font-family:{FontStack};font-size:12px;color:#B0B0B0;\">&#128206; Attached: {WebUtility.HtmlEncode(context.AttachmentFileName)}</span>");
                sb.Append("</td></tr>");
            }

            /* Footer */
            sb.Append("<tr><td style=\"padding:0 24px;\"><table role=\"presentation\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\" width=\"100%\"><tr><td style=\"height:1px;background-color:#404040;font-size:0;line-height:0;\">&nbsp;</td></tr></table></td></tr>");
            sb.Append("<tr><td style=\"padding:12px 24px 16px 24px;\">");
            sb.Append($"<span style=\"font-family:{FontStack};font-size:11px;color:#808080;\">");
            sb.Append($"Sent by {WebUtility.HtmlEncode(EditionName)}");
            if (!isTest)
            {
                sb.Append(" &middot; 15-minute cooldown between repeat alerts");
            }
            sb.Append("</span>");
            sb.Append("</td></tr>");

            /* Close inner table */
            sb.Append("</table>");

            /* Close outer wrapper */
            sb.Append("</td></tr></table>");
            sb.Append("</body></html>");

            return sb.ToString();
        }

        private static void AppendDataRow(StringBuilder sb, string label, string value, bool isLast)
        {
            var borderStyle = isLast ? "" : "border-bottom:1px solid #404040;";
            sb.Append("<tr>");
            sb.Append($"<td style=\"padding:10px 16px;width:120px;{borderStyle}font-family:{FontStack};font-size:13px;color:#B0B0B0;vertical-align:top;\">{WebUtility.HtmlEncode(label)}</td>");
            sb.Append($"<td style=\"padding:10px 16px;{borderStyle}font-family:{FontStack};font-size:13px;color:#FFFFFF;\">{WebUtility.HtmlEncode(value)}</td>");
            sb.Append("</tr>");
        }

        private static void AppendDetailSection(StringBuilder sb, AlertContext context)
        {
            /* Separator + heading */
            sb.Append("<tr><td style=\"padding:0 24px;\"><table role=\"presentation\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\" width=\"100%\"><tr><td style=\"height:1px;background-color:#404040;font-size:0;line-height:0;\">&nbsp;</td></tr></table></td></tr>");
            sb.Append("<tr><td style=\"padding:12px 24px 4px 24px;\">");
            sb.Append($"<span style=\"font-family:{FontStack};font-size:13px;font-weight:600;color:#808080;letter-spacing:0.5px;\">RECENT EVENTS</span>");
            sb.Append("</td></tr>");

            foreach (var item in context.Details)
            {
                /* Detail item heading */
                sb.Append("<tr><td style=\"padding:8px 24px 4px 24px;\">");
                sb.Append($"<span style=\"font-family:{FontStack};font-size:14px;font-weight:600;color:#E0E0E0;\">{WebUtility.HtmlEncode(item.Heading)}</span>");
                sb.Append("</td></tr>");

                /* Detail item fields */
                sb.Append("<tr><td style=\"padding:2px 24px 8px 24px;\">");
                sb.Append($"<table role=\"presentation\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\" width=\"100%\" style=\"background-color:#333333;border-radius:4px;\">");

                for (int i = 0; i < item.Fields.Count; i++)
                {
                    var (label, value) = item.Fields[i];
                    bool isLast = i == item.Fields.Count - 1;
                    bool isQuery = label.Contains("Query") || label.Contains("SQL");

                    if (isQuery && !string.IsNullOrEmpty(value))
                    {
                        AppendQueryRow(sb, label, value, isLast);
                    }
                    else
                    {
                        AppendDataRow(sb, label, value, isLast);
                    }
                }

                sb.Append("</table>");
                sb.Append("</td></tr>");
            }
        }

        private static void AppendQueryRow(StringBuilder sb, string label, string value, bool isLast)
        {
            var borderStyle = isLast ? "" : "border-bottom:1px solid #404040;";
            sb.Append("<tr>");
            sb.Append($"<td style=\"padding:10px 16px;width:120px;{borderStyle}font-family:{FontStack};font-size:13px;color:#B0B0B0;vertical-align:top;\">{WebUtility.HtmlEncode(label)}</td>");
            sb.Append($"<td style=\"padding:10px 16px;{borderStyle}\">");
            sb.Append($"<pre style=\"margin:0;font-family:'Courier New',Consolas,monospace;font-size:12px;color:#E0E0E0;white-space:pre-wrap;word-break:break-all;\">{WebUtility.HtmlEncode(value)}</pre>");
            sb.Append("</td></tr>");
        }

        private static string BuildPlainTextBody(
            string metricName,
            string serverName,
            string currentValue,
            string thresholdValue,
            DateTime utcNow,
            DateTime localNow,
            AlertContext? context = null)
        {
            var sb = new StringBuilder();
            sb.Append($"{EditionName} Alert\r\n");
            sb.Append($"-------------------------------\r\n");
            sb.Append($"Server: {serverName}\r\n");
            sb.Append($"Metric: {metricName}\r\n");
            sb.Append($"Current Value: {currentValue}\r\n");
            sb.Append($"Threshold: {thresholdValue}\r\n");
            sb.Append($"Time (UTC): {utcNow:yyyy-MM-dd HH:mm:ss}\r\n");
            sb.Append($"Time (Local): {localNow:yyyy-MM-dd HH:mm:ss}\r\n");

            if (context?.Details?.Count > 0)
            {
                sb.Append($"\r\n--- Recent Events ---\r\n");
                foreach (var item in context.Details)
                {
                    sb.Append($"\r\n  {item.Heading}\r\n");
                    foreach (var (label, value) in item.Fields)
                    {
                        sb.Append($"  {label}: {value}\r\n");
                    }
                }
            }

            if (!string.IsNullOrEmpty(context?.AttachmentFileName))
            {
                sb.Append($"\r\nAttached: {context.AttachmentFileName}\r\n");
            }

            return sb.ToString();
        }
    }
}
