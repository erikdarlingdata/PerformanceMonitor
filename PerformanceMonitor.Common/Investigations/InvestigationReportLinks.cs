/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace PerformanceMonitor.Common;

/// <summary>One link a report carries (#5914): a Jira ticket, a pull request, a bug report, a commit or a release the agent opened or found.
/// <paramref name="Key"/> is the ticket or PR key the agent gave (<c>OPS-123</c>, <c>#42</c>), or null.</summary>
public sealed record InvestigationReportLink(string Kind, string Url, string? Key);

/// <summary>
/// The link rules of an investigation report (#5914, #5632 Phase B slice L). A report may carry up to
/// <see cref="MaxLinks"/> links, each <c>{kind, url, key}</c>. A URL must be an absolute https URL with no user info:
/// http, file and UNC links are refused, because a UNC or file link opened on Windows sends the user's NTLM credentials to
/// the named host, and user info in a URL is a credential. The same <see cref="UrlProblem"/> runs at submit and again
/// wherever a stored link is rendered (the viewer and the web dashboard), so a link planted in the store directly still
/// renders as text. Links are stored as <see cref="JournalEvent"/> journal rows, one per request and URL; no column holds
/// them. Darling never calls Jira or git: it stores what the agent says.
/// </summary>
public static class InvestigationReportLinks
{
    /// <summary>The most links one report may carry.</summary>
    public const int MaxLinks = 10;

    /// <summary>The longest URL, in characters.</summary>
    public const int MaxUrlChars = 1000;

    /// <summary>The longest key, in characters.</summary>
    public const int MaxKeyChars = 200;

    /// <summary>The journal event a stored link is recorded under, once per request and URL.</summary>
    public const string JournalEvent = "link_recorded";

    /// <summary>The kinds a link may be.</summary>
    public static IReadOnlyList<string> Kinds { get; } = new[] { KindJira, KindPullRequest, KindBugReport, KindCommit, KindRelease };

    /// <summary>A Jira ticket.</summary>
    public const string KindJira = "jira";

    /// <summary>A pull request.</summary>
    public const string KindPullRequest = "pull_request";

    /// <summary>A bug report.</summary>
    public const string KindBugReport = "bug_report";

    /// <summary>A commit that fixes it (#6004, D17).</summary>
    public const string KindCommit = "commit";

    /// <summary>The release that carries the fix (#6004, D17).</summary>
    public const string KindRelease = "release";

    /// <summary>The kinds as a refusal names them (<c>jira, pull_request, bug_report, commit or release</c>), built from the kind
    /// constants so the two error messages cannot drift from <see cref="Kinds"/> (#6004; #3684: alias, do not copy). A const,
    /// so a tool description can use it.</summary>
    public const string KindsText = KindJira + ", " + KindPullRequest + ", " + KindBugReport + ", " + KindCommit + " or " + KindRelease;

    /// <summary>The kinds as a tool description writes the argument: <c>jira|pull_request|bug_report|commit|release</c>.</summary>
    public const string KindsAlternation = KindJira + "|" + KindPullRequest + "|" + KindBugReport + "|" + KindCommit + "|" + KindRelease;

    /// <summary>The URL rule, as a refusal names it.</summary>
    public const string UrlRule =
        "A link url must be an absolute https URL with no user name or password; http, file and UNC (\\\\server\\share or //server/share) links are refused.";

    /// <summary>
    /// Why <paramref name="url"/> may not be stored or rendered as a link, or null when it may: an absolute
    /// <c>https://host/...</c> URL, at most <see cref="MaxUrlChars"/> characters, with no whitespace or control character,
    /// no backslash, and no user info.
    /// </summary>
    public static string? UrlProblem(string? url)
    {
        if (string.IsNullOrWhiteSpace(url))
            return "missing.";
        if (url.Length > MaxUrlChars)
            return $"{url.Length} characters, over {MaxUrlChars}.";
        if (url.Any(c => char.IsWhiteSpace(c) || char.IsControl(c)))
            return "contains whitespace or a control character.";
        if (url.StartsWith("//", StringComparison.Ordinal) || url.Contains('\\', StringComparison.Ordinal))
            return "a UNC path (\\\\server\\share or //server/share) is refused.";
        if (url.StartsWith("file:", StringComparison.OrdinalIgnoreCase))
            return "a file URL is refused.";
        if (url.StartsWith("http:", StringComparison.OrdinalIgnoreCase))
            return "http is refused; use https.";
        if (!url.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
            return "not an https URL.";

        var authorityEnd = url.IndexOfAny(['/', '?', '#'], "https://".Length);
        var authority = authorityEnd < 0 ? url["https://".Length..] : url["https://".Length..authorityEnd];
        if (authority.Contains('@', StringComparison.Ordinal))
            return "user info (a name or password before @) is refused.";
        if (!Uri.TryCreate(url, UriKind.Absolute, out var uri) || string.IsNullOrEmpty(uri.Host))
            return "not an absolute https URL with a host.";
        if (!string.Equals(uri.Scheme, Uri.UriSchemeHttps, StringComparison.Ordinal) || uri.IsUnc || uri.IsFile)
            return "not an https URL.";
        if (!string.IsNullOrEmpty(uri.UserInfo))
            return "user info (a name or password before @) is refused.";
        return null;
    }

    /// <summary>True when <paramref name="url"/> may be rendered as a link (<see cref="UrlProblem"/> finds nothing).</summary>
    public static bool IsSafeUrl(string? url) => UrlProblem(url) is null;

    /// <summary>
    /// Parses and checks the <c>links</c> argument: null, blank, <c>[]</c> or <c>null</c> is no links. Returns every
    /// problem as one sentence (without the field name), or null with <paramref name="links"/> holding each link once per
    /// URL, the first kept.
    /// </summary>
    public static string? Validate(string? linksJson, out IReadOnlyList<InvestigationReportLink> links)
    {
        links = Array.Empty<InvestigationReportLink>();
        if (string.IsNullOrWhiteSpace(linksJson) || linksJson.Trim() is "[]" or "null")
            return null;

        JsonNode? root;
        try
        {
            root = JsonNode.Parse(linksJson);
        }
        catch (JsonException ex)
        {
            return "not valid JSON: " + ex.Message;
        }

        if (root is not JsonArray items)
            return "must be a JSON array of {kind, url, key}.";
        if (items.Count > MaxLinks)
            return $"{items.Count} links, over {MaxLinks}.";

        var problems = new List<string>();
        var parsed = new List<InvestigationReportLink>();
        for (var i = 0; i < items.Count; i++)
        {
            if (items[i] is not JsonObject item)
            {
                problems.Add($"item {i} is not an object.");
                continue;
            }

            var kind = Text(item["kind"]);
            var url = Text(item["url"]);
            var key = Text(item["key"])?.Trim();
            var bad = false;
            if (kind is null || !Kinds.Contains(kind, StringComparer.Ordinal))
            {
                problems.Add($"item {i} kind '{kind}' is not {KindsText}.");
                bad = true;
            }

            if (UrlProblem(url) is { } urlProblem)
            {
                problems.Add($"item {i} url: {urlProblem} {UrlRule}");
                bad = true;
            }

            if (key is { Length: > MaxKeyChars })
            {
                problems.Add($"item {i} key: {key.Length} characters, over {MaxKeyChars}.");
                bad = true;
            }

            if (!bad)
                parsed.Add(new InvestigationReportLink(kind!, url!, string.IsNullOrEmpty(key) ? null : key));
        }

        if (problems.Count > 0)
            return string.Join(" ", problems);

        links = Distinct(parsed);
        return null;
    }

    /// <summary>A store's own check of a submission's links, the tool layer's rule again (at most <see cref="MaxLinks"/>, a
    /// known kind, a URL <see cref="UrlProblem"/> passes): a caller that skipped the tool's validation gets an
    /// <see cref="ArgumentException"/> before anything is written.</summary>
    public static void ThrowIfInvalid(IReadOnlyList<InvestigationReportLink>? links, string paramName)
    {
        if (links is not { Count: > 0 })
            return;
        if (links.Count > MaxLinks)
            throw new ArgumentException($"links: {links.Count} links, over {MaxLinks}.", paramName);
        foreach (var link in links)
        {
            if (!Kinds.Contains(link.Kind, StringComparer.Ordinal))
                throw new ArgumentException($"links: kind '{link.Kind}' is not {KindsText}.", paramName);
            if (UrlProblem(link.Url) is { } problem)
                throw new ArgumentException("links: url " + problem + " " + UrlRule, paramName);
            if (link.Key is { Length: > MaxKeyChars })
                throw new ArgumentException($"links: key {link.Key.Length} characters, over {MaxKeyChars}.", paramName);
        }
    }

    /// <summary>Each link once per URL (ordinal), the first kept: what the journal's once-per-request-and-URL rule stores.</summary>
    public static IReadOnlyList<InvestigationReportLink> Distinct(IEnumerable<InvestigationReportLink> links) =>
        links.GroupBy(l => l.Url, StringComparer.Ordinal).Select(g => g.First()).ToList();

    /// <summary>The journal <c>detail</c> of a stored link: <c>{"kind","url","key"}</c>, <c>key</c> only when given.</summary>
    public static string DetailJson(InvestigationReportLink link)
    {
        ArgumentNullException.ThrowIfNull(link);
        var node = new JsonObject { ["kind"] = link.Kind, ["url"] = link.Url };
        if (link.Key is not null)
            node["key"] = link.Key;
        return node.ToJsonString();
    }

    /// <summary>The link a <see cref="JournalEvent"/> row's detail holds, or null when it holds none. Never checks the
    /// URL: a reader re-checks it with <see cref="UrlProblem"/> before rendering it as a link.</summary>
    public static InvestigationReportLink? FromDetail(string? detailJson)
    {
        if (string.IsNullOrWhiteSpace(detailJson))
            return null;
        try
        {
            return JsonNode.Parse(detailJson) is JsonObject o && Text(o["url"]) is { } url
                ? new InvestigationReportLink(Text(o["kind"]) ?? "", url, Text(o["key"]))
                : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    /// <summary>The journal once-key of a link on a request: one row per request and URL. The URL is hashed, so a long URL
    /// never meets an index entry limit.</summary>
    public static string OnceKey(Guid requestId, string url)
    {
        ArgumentNullException.ThrowIfNull(url);
        var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(url)));
        return "link:" + requestId.ToString("D") + ":" + hash;
    }

    private static string? Text(JsonNode? node) =>
        node is JsonValue value && value.TryGetValue(out string? text) ? text : null;
}
