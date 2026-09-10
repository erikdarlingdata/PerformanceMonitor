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

namespace PerformanceMonitor.Notifications;

/// <summary>
/// Builds the per-alert triage-page URL every webhook channel links to (#2710, option 2) — the Datadog
/// "notebook link" parity half of the alert shape (the structured-tag half shipped separately).
///
/// <para><b>The URL is COMPUTED, never provisioned.</b> No row is written anywhere when an alert links here:
/// the link carries (server, metric, firing instant, dedup key) and the Darling web host's
/// <c>GET /api/triage</c> endpoint assembles the page ON READ from data the store already holds — so a link
/// is stable for as long as retention keeps the underlying rows, and there is nothing to GC when it ages out
/// (the page then honestly shows empty windows rather than 404ing). That is why the key is
/// dedup-key + timestamp rather than an alert-history row id: the history row is written AFTER delivery
/// (<c>DarlingAlertDeliverer.SendAndRecordAsync</c>), so no id exists yet at payload-build time.</para>
///
/// <para><b>The base URL is deployment plumbing, not an alert knob.</b> It comes from
/// <see cref="IAlertSettings.TriageBaseUrl"/> (Darling: <c>web.publicBaseUrl</c> in darling.json, beside the
/// web host's own binding config; Lite serves no web page and always supplies empty). Empty/invalid ⇒ null ⇒
/// every channel omits the link and delivers exactly the pre-#2710 payload — a link can never be the reason
/// an alert fails to deliver.</para>
/// </summary>
public static class TriageLink
{
    /// <summary>The SPA hash route the link lands on; the query after it is parsed client-side and echoed to
    /// <c>GET /api/triage</c>. A hash route so the static SPA serves it with no server-side route addition.</summary>
    private const string TriageRoute = "/#/triage";

    /// <summary>
    /// PURE: the triage-page URL for one alert firing, or null when no valid base URL is configured — the
    /// callers all treat null as "omit the link", so a blank or garbage <c>web.publicBaseUrl</c> degrades to
    /// today's linkless payload rather than shipping a dead href. Only absolute http/https bases are accepted;
    /// a trailing slash on the base is tolerated (trimmed). <paramref name="firedUtc"/> stamps the firing
    /// instant (second precision, trailing Z) so the page can anchor its windows AT the incident rather than
    /// at click time; <paramref name="dedupKey"/> is the #1140 correlation fingerprint (the same key PagerDuty
    /// dedups on), passed through so the page can highlight the matching incident — optional, because an
    /// incident-less alert (CPU, low disk) has only the metric+server fallback identity.
    /// </summary>
    public static string? Build(string? baseUrl, string serverName, string metricName, DateTime firedUtc, string? dedupKey = null)
    {
        var trimmed = baseUrl?.Trim().TrimEnd('/');
        if (string.IsNullOrEmpty(trimmed))
        {
            return null;
        }

        if (!Uri.TryCreate(trimmed, UriKind.Absolute, out var uri)
            || (!string.Equals(uri.Scheme, "http", StringComparison.OrdinalIgnoreCase)
                && !string.Equals(uri.Scheme, "https", StringComparison.OrdinalIgnoreCase)))
        {
            return null;
        }

        var builder = new StringBuilder(trimmed.Length + 96);
        builder.Append(trimmed).Append(TriageRoute)
            .Append("?server=").Append(Uri.EscapeDataString(serverName ?? ""))
            .Append("&metric=").Append(Uri.EscapeDataString(metricName ?? ""))
            .Append("&at=").Append(Uri.EscapeDataString(
                firedUtc.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture)));

        if (!string.IsNullOrEmpty(dedupKey))
        {
            builder.Append("&dedup=").Append(Uri.EscapeDataString(dedupKey));
        }

        return builder.ToString();
    }
}
