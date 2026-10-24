/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The custom-alert rule teardown (#3305): force-resolve a rule's open incidents, then delete the rule; and the name
/// sanitizing its recovery rows use. In Storage so the Viewer's delete runs the web's own logic (#6390 S2b-B); the
/// Service's <c>CustomAlertEvaluator</c> keeps forwarders for its many callers. Every deadline is a parameter.
/// </summary>
public static class CustomAlertTeardown
{
    /// <summary>The reason a firing (rule, server) subject is force-resolved when its rule has passed its expiry (#6235).</summary>
    public const string ReasonExpired = "the rule expired";

    /// <summary>The reason a firing (rule, server) subject is force-resolved when the rule is deleted (#3305).</summary>
    public const string ReasonDeleted = "the rule was deleted";

    /// <summary>
    /// Makes a user-authored rule name/description safe to render in a notification title and to write into
    /// the <c>detail_text</c> that the viewer's mute-from-history pre-fill
    /// (<see cref="AlertMuteContext.PopulateFromDetailText"/>) later parses. That parser splits
    /// <c>detail_text</c> on <c>'\n'</c> and reads any line beginning with a <c>"Database: "</c> /
    /// <c>"Query: "</c> / <c>"Wait Type: "</c> / <c>"Job Name: "</c> label into a mute pattern, so a crafted
    /// name carrying <c>"\nDatabase: master"</c> could forge a mute-context field and spoof the pre-fill.
    /// Collapsing every line break and control character (CR/LF/TAB/NEL and the Unicode line/paragraph
    /// separators) to a single space guarantees the value stays one line that cannot begin a forged label
    /// line, and the length cap bounds the other half of the problem. Runs of whitespace collapse and the
    /// result is trimmed; returns "" for null/blank input so a render site falls back to the metric name.
    /// </summary>
    public static string SanitizeDisplayText(string? value, int maxLength)
    {
        if (string.IsNullOrWhiteSpace(value) || maxLength <= 0)
        {
            return string.Empty;
        }

        var builder = new StringBuilder(Math.Min(value.Length, maxLength));
        var pendingSpace = false;
        foreach (var rune in value.EnumerateRunes())
        {
            /* Characters that reorder or hide text are dropped outright: a right-to-left override or an invisible
               character in a title would show a subject or card line other than the one stored (#5654). The test
               runs on code points, so a character outside the Basic Multilingual Plane (the tag characters) is seen. */
            if (IsInvisibleOrReordering(rune))
            {
                continue;
            }

            var code = rune.Value;
            var isBreakOrControl =
                code < ' ' || code == 0x7F || code == 0x85 || code == 0x2028 || code == 0x2029;

            if (isBreakOrControl || code == ' ')
            {
                // Defer whitespace: emitted only before the next real char (drops leading + trailing runs).
                pendingSpace = builder.Length > 0;
                continue;
            }

            if (pendingSpace)
            {
                if (builder.Length >= maxLength)
                {
                    break;
                }

                builder.Append(' ');
                pendingSpace = false;
            }

            /* A surrogate pair goes in whole or not at all, so the cap never leaves half of one. */
            if (builder.Length + rune.Utf16SequenceLength > maxLength)
            {
                break;
            }

            builder.Append(rune.ToString());
        }

        return builder.ToString();
    }

    /// <summary>Every code point of the Unicode Format category (the bidirectional controls U+202A-202E and U+2066-2069,
    /// the Arabic letter mark U+061C, the zero-width and mark characters U+200B-200F, U+2060-2064, U+FEFF, the tag
    /// characters U+E0001 and U+E0020-E007F, and the rest), except the zero-width non-joiner U+200C and joiner U+200D,
    /// which the spelling of Persian, Indic and emoji sequences needs; and the C1 controls (U+0080-009F except U+0085,
    /// which the sanitizer already treats as a line break).</summary>
    public static bool IsInvisibleOrReordering(Rune rune)
    {
        var code = rune.Value;
        if (code >= 0x80 && code <= 0x9F)
        {
            return code != 0x85;
        }

        if (code == 0x200C || code == 0x200D)
        {
            return false;
        }

        return Rune.GetUnicodeCategory(rune) == UnicodeCategory.Format;
    }

    /// <summary>
    /// A rule's operator-typed name, judged by the statement filter (#5320, L4 of #5360). The name rides a resolution
    /// row's title (the history row's metric_name) and, on the teardown row, its message; both rows go out to the
    /// history store, so the name is judged there like the display name of a fired alert. The local log line and the
    /// server name stay as they are (the log never leaves the machine; the server name is a routing key). The
    /// " Resolved" suffix is added after the judgment, so a withheld name still reads as a resolution.
    /// </summary>
    public static string JudgedRuleName(string safeName) => SensitiveStatements.Text(safeName) ?? safeName;

    /// <summary>
    /// Writes ONE recovery/resolution row for a teardown (#3305) — the SAME resolve idiom
    /// <see cref="DeliverResolveAsync"/> uses (<c>BuildResolutionRecord</c> + <c>AlertFiringLog.Resolved</c>),
    /// but keyed off an already-known (rule, server) rather than a live evaluation, and stating WHY (deleted /
    /// disabled / out of scope) rather than a threshold recovery. Static so both the delete path and the sweep
    /// reconcile can call it. The rule name is newline-stripped + length-capped before it reaches
    /// <c>detail_text</c>, so a crafted name cannot re-open the mute-from-history spoof on the recovery row.
    /// Failure-isolated: an audit-row write must never break a delete or a sweep.
    /// </summary>
    public static async Task WriteTeardownResolutionAsync(
        PgAlertHistoryStore historyStore, ILogger? logger,
        long ruleId, string ruleName, int serverId, string serverName, string reason)
    {
        var safeName = SanitizeDisplayText(ruleName, CustomAlertRuleStore.MaxNameLength);
        var title = JudgedRuleName(safeName) + " Resolved";
        var message = string.Create(CultureInfo.InvariantCulture, $"{serverName}: {JudgedRuleName(safeName)} resolved because {reason}");

        logger?.LogInformation("{Line}", AlertFiringLog.Resolved(serverName, safeName + " Resolved", string.Create(CultureInfo.InvariantCulture, $"{serverName}: {safeName} resolved because {reason}")));

        try
        {
            // #3334: route the recovery-row write through the SECURITY DEFINER config.record_custom_alert_resolution
            // (owner-privileged, EXECUTE-granted to viewer/mcp) so it succeeds on the least-privilege delete pools
            // (mcp delete tool / viewer web DELETE), which may not INSERT config_alert_log directly -- the gap that
            // silently dropped this row (#3305). The row maps the same way the DeliverResolveAsync/BuildResolutionRecord
            // resolve does (title -> metric_name, message -> detail_text, zeroed values, unmuted), but is pinned to a
            // NO-CHANNEL row (alert_sent false, notification_type 'none') -- UNLIKE the natural-clear resolve's 'tray':
            // a teardown surfaces no operator notification, and the definer function must never let a grantee write a
            // row claiming a delivery. That divergence is intentional; nothing pairs open/resolved on those columns.
            await historyStore.RecordCustomAlertResolutionAsync(serverId, serverName, title, message);
        }
        catch (Exception ex)
        {
            logger?.LogWarning("Could not record custom alert teardown resolution for rule {Rule} on {Server}: {Message}",
                ruleId, serverName, ex.Message);
        }
    }

    /// <summary>
    /// Force-resolves any OPEN incident of a rule and then deletes the rule (#3305). Reads the rule (for its
    /// name) and its firing subjects and writes a recovery row for each BEFORE calling
    /// <see cref="CustomAlertRuleStore.DeleteAsync"/> — because that delete's FK cascade drops the
    /// <c>custom_alert_state</c> rows that say which (rule, server) pairs were firing, so the resolve must read
    /// them first. Static (the MCP delete tool holds only the owner pool, not the singleton evaluator); the
    /// evaluator's in-memory no-data map is pruned lazily on its next cache refresh. The resolve is best-effort
    /// (its own failure isolation) so a resolve blip never blocks the delete the operator asked for.
    /// <paramref name="commandTimeoutSeconds"/> is the deadline of the rule and state reads and the delete;
    /// <paramref name="historyCommandTimeoutSeconds"/> that of the recovery-row writes.
    /// </summary>
    public static Task<CustomAlertRuleResult> ResolveAndDeleteRuleAsync(
        NpgsqlDataSource postgres, long ruleId, ILogger? logger,
        int commandTimeoutSeconds, int historyCommandTimeoutSeconds, CancellationToken cancellationToken) =>
        ResolveAndDeleteRuleAsync(
            postgres, new CustomAlertRuleStore(postgres, commandTimeoutSeconds), ruleId, logger,
            commandTimeoutSeconds, historyCommandTimeoutSeconds, cancellationToken);

    /// <summary>
    /// The same resolve-then-delete over <paramref name="ruleStore"/> (#5915): a Level 2 agent delete passes a store bound to
    /// the gate's transaction, so the delete commits with the journal row. The resolution rows are written on their own
    /// connections before the delete, as they always were, so they stand if that transaction is later abandoned.
    /// </summary>
    public static async Task<CustomAlertRuleResult> ResolveAndDeleteRuleAsync(
        NpgsqlDataSource postgres, CustomAlertRuleStore ruleStore, long ruleId, ILogger? logger,
        int commandTimeoutSeconds, int historyCommandTimeoutSeconds, CancellationToken cancellationToken)
    {
        var current = await ruleStore.GetAsync(ruleId, cancellationToken);
        if (current is CustomAlertRuleResult.Ok ok && ok.Rule is not null)
        {
            var stateStore = new CustomAlertStateStore(postgres, commandTimeoutSeconds);
            var firing = await stateStore.ListFiringWithNamesAsync(ruleId, cancellationToken);
            if (firing.Count > 0)
            {
                var historyStore = new PgAlertHistoryStore(postgres, historyCommandTimeoutSeconds, logger);
                foreach (var (serverId, serverName) in firing)
                {
                    await WriteTeardownResolutionAsync(historyStore, logger, ruleId, ok.Rule.Name, serverId, serverName, ReasonDeleted);
                }
            }
        }

        return await ruleStore.DeleteAsync(ruleId, cancellationToken);
    }
}
