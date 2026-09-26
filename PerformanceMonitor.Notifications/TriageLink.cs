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

    /// <summary>The SPA hash route the Fleet Sweep Rollup links to instead of a triage page (#4223) — the
    /// sweep-with-memory timeline (#3466), which takes no query: the rollup names no one server or metric, so
    /// there is nothing for <c>server=</c>/<c>metric=</c>/<c>at=</c> to carry.</summary>
    private const string SweepsRoute = "/#/sweeps";

    /// <summary>The exact <c>MetricName</c> the Fleet Sweep Rollup self-alert fires under (see
    /// <see cref="AlertFamily.MetricFamilies"/>). Held here, not read from Darling's own
    /// <c>DarlingSelfAlertEvaluator.FleetSweepRollupMetric</c>, because this project cannot reference Darling's
    /// — the two constants name the same string on purpose, and <c>AlertFamilyCensusTests</c>' census keeps
    /// them from drifting apart silently.</summary>
    internal const string FleetSweepRollupMetric = "Fleet Sweep Rollup";

    /// <summary>
    /// PURE: the link for one alert firing, or null when there is nothing to link to. The callers all treat
    /// null as "omit the link", so a blank/garbage <c>web.publicBaseUrl</c> degrades to today's linkless
    /// payload rather than shipping a dead href — that degrade now also covers the two report digests
    /// (#4223): a scheduled document naming no single incident has no triage page to point at, so
    /// <see cref="AlertFamily.Reports"/> metrics other than the rollup return null here on purpose.
    ///
    /// <para><b>#4223: the Fleet Sweep Rollup is the one report with somewhere to send a reader</b> — the
    /// sweep timeline it summarizes, <c>#/sweeps</c>, which takes no query (the page has no single
    /// server/metric/instant to anchor on, unlike a page-worthy alert). Every other metric, report or not,
    /// keeps the triage-page URL below.</para>
    ///
    /// <para>Only absolute http/https bases are accepted; a trailing slash on the base is tolerated (trimmed).
    /// <paramref name="firedUtc"/> stamps the firing instant (second precision, trailing Z) so the triage page
    /// can anchor its windows AT the incident rather than at click time; <paramref name="dedupKey"/> is the
    /// #1140 correlation fingerprint (the same key PagerDuty dedups on), passed through so the page can
    /// highlight the matching incident — optional, because an incident-less alert (CPU, low disk) has only the
    /// metric+server fallback identity.
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

        if (AlertFamily.Of(metricName) == AlertFamily.Reports)
        {
            return string.Equals(metricName, FleetSweepRollupMetric, StringComparison.Ordinal)
                ? trimmed + SweepsRoute
                : null;
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

    /// <summary>
    /// PURE: the host component of <paramref name="baseUrl"/> when it is the same absolute-http/https shape
    /// <see cref="Build"/> accepts, or null otherwise (unset, or not that shape). No port, no scheme, no
    /// credentials — just the Host-header-comparable name or IP literal.
    ///
    /// <para>#4220: the web host passes this to its Host-header allowlist as one extra admitted value (see
    /// <c>DarlingHostBinding.IsAllowedHost</c>) — the host the operator configured is not attacker-reachable,
    /// so admitting it closes the gap where <c>darling.sample.json</c> suggests a DNS name for
    /// <c>publicBaseUrl</c> but the guard, unchanged, would have refused every request that named it.</para>
    /// </summary>
    public static string? TryGetHost(string? baseUrl)
    {
        var trimmed = baseUrl?.Trim();
        if (string.IsNullOrEmpty(trimmed)
            || !Uri.TryCreate(trimmed, UriKind.Absolute, out var uri)
            || (!string.Equals(uri.Scheme, "http", StringComparison.OrdinalIgnoreCase)
                && !string.Equals(uri.Scheme, "https", StringComparison.OrdinalIgnoreCase)))
        {
            return null;
        }

        return uri.Host;
    }

    /// <summary>
    /// PURE: the button/link label for a URL <see cref="Build"/> returned — "Open sweep timeline" for the
    /// Fleet Sweep Rollup's <c>#/sweeps</c> link (#4223: it is not a triage page, so the label must not claim
    /// it is one), else "Open triage page" for every other non-null link. Callers pass the SAME url they
    /// received from <see cref="Build"/> so the label always matches what it names.
    /// </summary>
    public static string LinkLabel(string? triageUrl)
    {
        return triageUrl is not null && triageUrl.Contains(SweepsRoute, StringComparison.Ordinal)
            ? "Open sweep timeline"
            : "Open triage page";
    }

    /// <summary>
    /// PURE: a one-line startup warning when <paramref name="baseUrl"/> is shaped like it carries a credential
    /// — a query string, a fragment, or userinfo (e.g. the dashboard's own sign-in link,
    /// <c>http://host:port/?token=...</c>, pasted in by mistake). Null when there is nothing to warn about.
    ///
    /// <para>Ruled (#4220, 2026-09-25): sending the link is the operator's decision — <see cref="Build"/> does
    /// not refuse a base shaped this way, and every channel still gets the link. This is the whole mitigation:
    /// name the risk once at startup instead of silently repeating it in every alert payload from then on. The
    /// message never echoes the configured value — that would put the very credential it warns about into the
    /// log.</para>
    /// </summary>
    public static string? DescribeCredentialShapedBaseWarning(string? baseUrl)
    {
        var trimmed = baseUrl?.Trim().TrimEnd('/');
        if (string.IsNullOrEmpty(trimmed)
            || !Uri.TryCreate(trimmed, UriKind.Absolute, out var uri)
            || (!string.Equals(uri.Scheme, "http", StringComparison.OrdinalIgnoreCase)
                && !string.Equals(uri.Scheme, "https", StringComparison.OrdinalIgnoreCase)))
        {
            /* Not a shape Build ever turns into a link (same absolute-http/https gate as Build) — nothing
               will ever carry this value, so there is nothing to warn about. */
            return null;
        }

        if (string.IsNullOrEmpty(uri.Query) && string.IsNullOrEmpty(uri.Fragment) && string.IsNullOrEmpty(uri.UserInfo))
        {
            return null;
        }

        return "web.publicBaseUrl includes a query string, fragment, or embedded sign-in details — that text is "
            + "sent verbatim in every alert's triage link (Slack, Teams, PagerDuty, generic, email). If it is a "
            + "token or credential, treat every channel that receives alerts as having it. This is not refused; "
            + "edit web.publicBaseUrl if that was not intended.";
    }
}
