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
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;

namespace PerformanceMonitor.Darling.Service;

/// <summary>What a name is, which decides the prefix of its alias (<c>server-1</c>, <c>db-3</c>).</summary>
internal enum AliasKind
{
    Server,
    Host,
    Database,
    Domain,
    Login,
    Role,
    Ip,
}

/// <summary>One place a known name or secret survived aliasing: which section and field, never the text itself.</summary>
internal sealed record BundleLeak(string Section, string Field, string Class);

/// <summary>
/// Replaces every known name in a diagnostics bundle with an ordinal alias, and then proves none survived.
///
/// <para><b>Ordinals, never hashes.</b> <c>server-1</c>, <c>db-3</c>, <c>host-2</c>, <c>ip-1</c> are derived from
/// nothing, so there is nothing to enumerate or reverse. Aliases are stable for a given order of registration, so two
/// bundles from one store use the same alias for the same server. The alias map leaves this class only through
/// <see cref="AliasMapLines"/>, for the opt-in map file; the bundle never carries it.</para>
///
/// <para><b>The scan</b> rewrites string VALUES, never keys, with one compiled alternation, longest token first, with
/// word-character boundaries so <c>sales</c> does not match inside <c>wholesales</c>. It runs in one pass, so an alias
/// is never re-scanned. <see cref="Finish"/> is the proof: it scans the serialized bytes and the JSON-unescaped text
/// for every known token of <see cref="VerifyMinimumLength"/> characters or more as a plain substring, replaces a
/// glued hit with no boundary, and reports anything that still survives.</para>
/// </summary>
internal sealed class BundleAliaser
{
    /// <summary>Tokens shorter than this are boundary-replaced but not substring-verified: they would hit everywhere.</summary>
    internal const int VerifyMinimumLength = 4;

    /// <summary>Shorter than this and a name is not a token at all (a single character would garble every sentence).</summary>
    internal const int TokenMinimumLength = 2;

    /// <summary>
    /// Names that identify nothing and appear in product text: the system databases, the product's own roles, and
    /// the operating-system accounts every install shares. Pinned by a test.
    /// </summary>
    internal static readonly string[] KeptNames =
    {
        "master", "model", "msdb", "tempdb", "distribution", "rdsadmin", "postgres", "template0", "template1",
        "darling", "root", "localhost",
    };

    /// <summary>The product's own store roles. A <c>role_name</c> in this set is kept; any other role name is a login and is aliased.</summary>
    internal static readonly string[] ProductRoles = { "owner", "admin", "viewer", "mcp" };

    /// <summary>The JSON keys whose string value is a name the harvest pass adds to the name set, with the kind it is.</summary>
    internal static readonly IReadOnlyDictionary<string, AliasKind> HarvestKeys = new Dictionary<string, AliasKind>(StringComparer.OrdinalIgnoreCase)
    {
        ["server_name"] = AliasKind.Server,
        ["display_name"] = AliasKind.Server,
        ["instance_name"] = AliasKind.Server,
        ["host"] = AliasKind.Host,
        ["host_name"] = AliasKind.Host,
        ["listener"] = AliasKind.Host,
        ["ag_name"] = AliasKind.Host,
        ["client_hostname"] = AliasKind.Host,
        ["database"] = AliasKind.Database,
        ["database_name"] = AliasKind.Database,
        ["db_name"] = AliasKind.Database,
        ["dbname"] = AliasKind.Database,
        ["login_name"] = AliasKind.Login,
        ["user_name"] = AliasKind.Login,
        ["client_addr"] = AliasKind.Ip,
        ["role_name"] = AliasKind.Role,
    };

    private static readonly HashSet<string> s_kept = new(KeptNames, StringComparer.OrdinalIgnoreCase);

    private const string Octet = @"(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)";
    private const string Ipv4 = Octet + @"(?:\." + Octet + "){3}";
    private const string Ipv6 = @"(?:[0-9a-f]{1,4}:){7}[0-9a-f]{1,4}|(?:[0-9a-f]{1,4}:){1,7}:(?:[0-9a-f]{1,4}(?::[0-9a-f]{1,4}){0,5})?|::(?:[0-9a-f]{1,4}:){0,6}[0-9a-f]{1,4}";

    private readonly Dictionary<string, string> _aliasByToken = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<AliasKind, int> _counts = new();
    private readonly Dictionary<int, string> _aliasByServerId = new();
    private readonly List<string> _secrets = new();
    private readonly HashSet<string> _secretSet = new(StringComparer.Ordinal);
    private readonly List<string> _tokenOrder = new();
    private Regex? _scan;

    /// <summary>How many aliases of each kind exist, for the manifest (counts only).</summary>
    internal IReadOnlyDictionary<AliasKind, int> Counts => _counts;

    /// <summary>The number of known name tokens.</summary>
    internal int TokenCount => _aliasByToken.Count;

    /// <summary>True when <paramref name="name"/> is one of the names that are never aliased.</summary>
    internal static bool IsKept(string name) => s_kept.Contains(name.Trim());

    /// <summary>Adds a secret value the verifier must never find. Empty text is ignored.</summary>
    internal void AddSecret(string? secret)
    {
        /* A secret equal to one of the kept product words (a lab password of "darling", say) cannot be removed from
           product text without destroying it, so it is not tracked. Documented limitation. */
        if (!string.IsNullOrEmpty(secret) && !IsKept(secret) && _secretSet.Add(secret))
        {
            _secrets.Add(secret);
        }
    }

    /// <summary>Registers a server's id so a <c>server_id</c> value becomes its alias, and returns that alias.</summary>
    internal string AddServer(int serverId, string? name)
    {
        string alias;
        if (!string.IsNullOrWhiteSpace(name))
        {
            AddName(AliasKind.Server, name);
            alias = AliasOf(name.Trim().Trim('[', ']', '"', '\''), AliasKind.Server);
        }
        else
        {
            alias = NextAlias(AliasKind.Server);
        }

        _aliasByServerId[serverId] = alias;
        return alias;
    }

    /// <summary>The alias a server id stands for; an id the registry never listed gets a fresh server alias.</summary>
    internal string AliasForServerId(long serverId)
    {
        if (serverId is >= int.MinValue and <= int.MaxValue && _aliasByServerId.TryGetValue((int)serverId, out var known))
        {
            return known;
        }

        var alias = NextAlias(AliasKind.Server);
        if (serverId is >= int.MinValue and <= int.MaxValue)
        {
            _aliasByServerId[(int)serverId] = alias;
        }

        return alias;
    }

    /// <summary>
    /// Adds a name and its pieces to the name set. <c>host\instance</c> adds the host and the instance;
    /// <c>host,port</c> and <c>host:port</c> add the host; an FQDN adds the full name, its first label and its domain
    /// suffix (as <see cref="AliasKind.Domain"/>); a bracketed or quoted form adds the bare name; an IP literal is an
    /// <see cref="AliasKind.Ip"/>. The whole spelling and the host share one alias.
    /// </summary>
    internal void AddName(AliasKind kind, string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return;
        }

        var name = raw.Trim().Trim('[', ']', '"', '\'', ' ');
        if (name.Length < TokenMinimumLength)
        {
            return;
        }

        var baseName = name;
        string? instance = null;
        if (!IPAddress.TryParse(name, out _))
        {
            var port = Regex.Match(name, @"^(?<h>[^,:]+(?:\\[^,:]+)?)[,:]\d{1,5}$", RegexOptions.CultureInvariant, TimeSpan.FromSeconds(1));
            if (port.Success)
            {
                baseName = port.Groups["h"].Value;
            }

            var slash = baseName.IndexOf('\\', StringComparison.Ordinal);
            if (slash > 0)
            {
                instance = baseName[(slash + 1)..];
                baseName = baseName[..slash];
            }
        }

        baseName = baseName.Trim().Trim('[', ']', '"', '\'');
        var baseAlias = RegisterBase(kind, baseName);
        if (baseAlias is not null && !string.Equals(name, baseName, StringComparison.OrdinalIgnoreCase))
        {
            Map(name, baseAlias);
        }

        if (!string.IsNullOrWhiteSpace(instance))
        {
            RegisterBase(kind == AliasKind.Server ? AliasKind.Host : kind, instance.Trim().Trim('[', ']', '"', '\''));
        }
    }

    /// <summary>Gives <paramref name="raw"/> the alias <paramref name="anchor"/> already has (a display name shares its server's alias).</summary>
    internal void AddNameLike(string? raw, string? anchor)
    {
        if (string.IsNullOrWhiteSpace(raw) || string.IsNullOrWhiteSpace(anchor))
        {
            return;
        }

        if (_aliasByToken.TryGetValue(anchor.Trim().Trim('[', ']', '"', '\''), out var alias))
        {
            Map(raw.Trim(), alias);
        }
        else
        {
            AddName(AliasKind.Server, raw);
        }
    }

    /// <summary>Adds each comma-separated name in <paramref name="list"/> (a connection string's <c>Host</c> may hold several).</summary>
    internal void AddNameList(AliasKind kind, string? list)
    {
        if (string.IsNullOrWhiteSpace(list))
        {
            return;
        }

        foreach (var part in list.Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            AddName(kind, part);
        }
    }

    /// <summary>
    /// The harvest pass: walks <paramref name="node"/> and adds the string value of every key in
    /// <see cref="HarvestKeys"/>, at any depth, to the name set.
    /// </summary>
    internal void Harvest(JsonNode? node)
    {
        switch (node)
        {
            case JsonObject obj:
                foreach (var (key, value) in obj)
                {
                    if (HarvestKeys.TryGetValue(key, out var kind) && value is JsonValue v && v.TryGetValue<string>(out var text)
                        && !string.Equals(text, "(fleet)", StringComparison.Ordinal)
                        && !(kind == AliasKind.Role && ProductRoles.Contains(text, StringComparer.OrdinalIgnoreCase)))
                    {
                        AddName(kind, text);
                    }

                    Harvest(value);
                }

                break;
            case JsonArray array:
                foreach (var item in array)
                {
                    Harvest(item);
                }

                break;
        }
    }

    /// <summary>
    /// Applies the secret guard and then the alias pass to one text: <c>Password=...</c> and <c>://user:pass@</c> are
    /// redacted, every known name becomes its alias, and every IP literal becomes <c>ip-N</c>.
    /// </summary>
    internal string Alias(string text)
    {
        if (text.Length == 0)
        {
            return text;
        }

        var redacted = text;
        foreach (var secret in _secrets)
        {
            redacted = redacted.Replace(secret, SecretTextGuard.RedactedMarker, StringComparison.Ordinal);
        }

        redacted = SecretTextGuard.RedactText(redacted);
        try
        {
            return Scanner().Replace(redacted, Evaluate);
        }
        catch (RegexMatchTimeoutException)
        {
            return SecretTextGuard.RedactedMarker;
        }
    }

    /// <summary>
    /// Rewrites a tree: every string value goes through <see cref="Alias"/>; every key naming a secret has its value
    /// replaced by the redaction marker at any depth; a <c>server_id</c> number becomes a <c>server_alias</c> string.
    /// Keys are never aliased, so the structure survives.
    /// </summary>
    internal JsonNode? AliasTree(JsonNode? node)
    {
        switch (node)
        {
            case null:
                return null;
            case JsonObject obj:
            {
                var copy = new JsonObject();
                foreach (var (key, value) in obj)
                {
                    if (value is JsonValue idValue && string.Equals(key, "server_id", StringComparison.OrdinalIgnoreCase)
                        && idValue.TryGetValue<long>(out var id))
                    {
                        copy["server_alias"] = AliasForServerId(id);
                    }
                    else if (IsVersionKey(key) && value is JsonValue ver && ver.TryGetValue<string>(out var versionText) && s_versionShape.IsMatch(versionText))
                    {
                        copy[key] = versionText;
                    }
                    else if (SecretTextGuard.IsSecretKey(key) && value is not null && !IsPlainScalar(value))
                    {
                        copy[key] = SecretTextGuard.RedactedMarker;
                    }
                    else
                    {
                        copy[key] = AliasTree(value);
                    }
                }

                return copy;
            }

            case JsonArray array:
            {
                var copy = new JsonArray();
                foreach (var item in array)
                {
                    copy.Add(AliasTree(item));
                }

                return copy;
            }

            case JsonValue value:
                return value.TryGetValue<string>(out var text) ? JsonValue.Create(Alias(text)) : value.DeepClone();
            default:
                return node.DeepClone();
        }
    }

    /// <summary>
    /// The proof. Serializes <paramref name="root"/> (indented), scans the bytes and the JSON-unescaped text for every
    /// known token and secret as a plain case-insensitive substring, replaces a name that was glued to word
    /// characters (which the boundary rule let through) with no boundary, and scans again. Returns the leaks that
    /// survive, and the final serialized text when there are none.
    /// </summary>
    internal (IReadOnlyList<BundleLeak> Leaks, string? Text) Finish(JsonNode root, JsonSerializerOptions options)
    {
        for (var pass = 0; pass < 6; pass++)
        {
            var text = root.ToJsonString(options);
            var leaks = Scan(root, text);
            if (leaks.Count == 0)
            {
                return (leaks, text);
            }

            if (leaks.Any(l => l.Class == "secret"))
            {
                return (leaks, null);
            }

            var tokens = LeakedTokens(root);
            if (tokens.Count == 0)
            {
                return (leaks, null);
            }

            ReplaceGlued(root, tokens);
        }

        var finalText = root.ToJsonString(options);
        var remaining = Scan(root, finalText);
        return remaining.Count == 0 ? (remaining, finalText) : (remaining, null);
    }

    /// <summary>The alias map (alias, then the name it stands for), one pair per line, for the opt-in map file.</summary>
    internal IReadOnlyList<string> AliasMapLines()
    {
        var lines = new List<string>();
        foreach (var token in _tokenOrder)
        {
            if (_aliasByToken.TryGetValue(token, out var alias))
            {
                lines.Add(alias + "\t" + token);
            }
        }

        foreach (var pair in _aliasByServerId.OrderBy(p => p.Key))
        {
            lines.Add(pair.Value + "\tserver_id " + pair.Key.ToString(CultureInfo.InvariantCulture));
        }

        return lines;
    }

    private static readonly Regex s_versionShape = new(@"^[0-9][0-9A-Za-z.+\-]*$", RegexOptions.CultureInvariant, TimeSpan.FromSeconds(1));

    /* A version string such as 1.5.0.0 has the shape of an IPv4 address; it names nothing, so it is kept verbatim. */
    private static bool IsVersionKey(string key) =>
        key.EndsWith("_version", StringComparison.OrdinalIgnoreCase) || string.Equals(key, "version", StringComparison.OrdinalIgnoreCase);

    private static bool IsPlainScalar(JsonNode value) =>
        value is JsonValue v && !v.TryGetValue<string>(out _);

    private string? RegisterBase(AliasKind kind, string baseName)
    {
        if (baseName.Length < TokenMinimumLength || IsKept(baseName))
        {
            return null;
        }

        if (IPAddress.TryParse(baseName, out _))
        {
            return Map(baseName, AliasOf(baseName, AliasKind.Ip));
        }

        var dot = baseName.IndexOf('.', StringComparison.Ordinal);
        if (dot > 0 && dot < baseName.Length - 1 && !baseName.Split('.').All(label => label.All(char.IsDigit)))
        {
            var firstLabel = baseName[..dot];
            var suffix = baseName[(dot + 1)..];
            var alias = IsKept(firstLabel) ? null : AliasOf(firstLabel, kind);
            if (alias is not null)
            {
                Map(baseName, alias);
            }
            else
            {
                alias = AliasOf(baseName, kind);
            }

            if (suffix.Length >= TokenMinimumLength)
            {
                AliasOf(suffix, AliasKind.Domain);
            }

            return alias;
        }

        return AliasOf(baseName, kind);
    }

    private string AliasOf(string token, AliasKind kind)
    {
        if (_aliasByToken.TryGetValue(token, out var known))
        {
            return known;
        }

        var alias = NextAlias(kind);
        return Map(token, alias);
    }

    private string Map(string token, string alias)
    {
        if (token.Length >= TokenMinimumLength && !IsKept(token) && !_aliasByToken.ContainsKey(token))
        {
            _aliasByToken[token] = alias;
            _tokenOrder.Add(token);
            _scan = null;
        }

        return alias;
    }

    private string NextAlias(AliasKind kind)
    {
        _counts.TryGetValue(kind, out var n);
        _counts[kind] = ++n;
        return Prefix(kind) + "-" + n.ToString(CultureInfo.InvariantCulture);
    }

    private static string Prefix(AliasKind kind) => kind switch
    {
        AliasKind.Server => "server",
        AliasKind.Host => "host",
        AliasKind.Database => "db",
        AliasKind.Domain => "domain",
        AliasKind.Login => "login",
        AliasKind.Role => "role",
        _ => "ip",
    };

    private string Evaluate(Match match)
    {
        var ip = match.Groups["ip"];
        if (ip.Success)
        {
            return _aliasByToken.TryGetValue(ip.Value, out var known) ? known : AliasOf(ip.Value, AliasKind.Ip);
        }

        return _aliasByToken.TryGetValue(match.Groups["tok"].Value, out var alias) ? alias : match.Value;
    }

    private Regex Scanner()
    {
        if (_scan is not null)
        {
            return _scan;
        }

        var tokens = _aliasByToken.Keys.OrderByDescending(t => t.Length).ThenBy(t => t, StringComparer.Ordinal).Select(Regex.Escape).ToList();
        var alternation = tokens.Count == 0 ? "(?!)" : string.Join('|', tokens);
        var pattern = "(?<![A-Za-z0-9_])(?:(?<tok>" + alternation + ")|(?<ip>" + Ipv4 + "|" + Ipv6 + "))(?![A-Za-z0-9_])";
        _scan = new Regex(
            pattern,
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | (tokens.Count > 20 ? RegexOptions.Compiled : RegexOptions.None),
            TimeSpan.FromSeconds(10));
        return _scan;
    }

    private bool Verifiable(string token) => token.Length >= VerifyMinimumLength;

    /// <summary>All keys and string values of a tree, for the unescaped scan and the key-contained exemption.</summary>
    private static void Collect(JsonNode? node, string path, List<(string Path, string Text)> values, HashSet<string> keys)
    {
        switch (node)
        {
            case JsonObject obj:
                foreach (var (key, value) in obj)
                {
                    keys.Add(key);
                    Collect(value, path.Length == 0 ? key : path + "." + key, values, keys);
                }

                break;
            case JsonArray array:
                for (var i = 0; i < array.Count; i++)
                {
                    Collect(array[i], path + "[" + i.ToString(CultureInfo.InvariantCulture) + "]", values, keys);
                }

                break;
            case JsonValue v when v.TryGetValue<string>(out var text):
                values.Add((path, text));
                break;
        }
    }

    private List<BundleLeak> Scan(JsonNode root, string serialized)
    {
        var values = new List<(string Path, string Text)>();
        var keys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        Collect(root, string.Empty, values, keys);
        var unescaped = string.Join('\n', values.Select(v => v.Text));
        var leaks = new List<BundleLeak>();

        foreach (var secret in _secrets)
        {
            if (serialized.Contains(secret, StringComparison.Ordinal) || unescaped.Contains(secret, StringComparison.Ordinal))
            {
                leaks.Add(Locate(values, secret, "secret"));
            }
        }

        foreach (var token in _aliasByToken.Keys)
        {
            if (!Verifiable(token) || TokenIsAliasText(token))
            {
                continue;
            }

            /* A token that is also one of the bundle's own keys is checked in the values only: keys are the product's
               vocabulary and are never rewritten. Every other token is checked in the serialized bytes as well. */
            var inKeys = keys.Any(k => k.Contains(token, StringComparison.OrdinalIgnoreCase));
            if (unescaped.Contains(token, StringComparison.OrdinalIgnoreCase)
                || (!inKeys && serialized.Contains(token, StringComparison.OrdinalIgnoreCase)))
            {
                leaks.Add(Locate(values, token, "name"));
            }
        }

        return leaks;
    }

    private static bool TokenIsAliasText(string token)
    {
        return Regex.IsMatch(token, @"^(server|host|db|domain|login|role|ip)-\d+$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant, TimeSpan.FromSeconds(1))
            || new[] { "server", "host", "db", "domain", "login", "role", "ip" }.Any(p => (p + "-").Contains(token, StringComparison.OrdinalIgnoreCase));
    }

    private static BundleLeak Locate(List<(string Path, string Text)> values, string needle, string cls)
    {
        foreach (var (path, text) in values)
        {
            if (text.Contains(needle, StringComparison.OrdinalIgnoreCase))
            {
                return new BundleLeak(SectionOf(path), FieldOf(path), cls);
            }
        }

        return new BundleLeak("(structure)", "(key or escaped form)", cls);
    }

    private static string SectionOf(string path)
    {
        var parts = path.Split('.');
        return parts.Length >= 2 && parts[0] == "sections" ? parts[1] : parts[0];
    }

    private static string FieldOf(string path)
    {
        var parts = path.Split('.');
        return parts[^1];
    }

    private HashSet<string> LeakedTokens(JsonNode root)
    {
        var values = new List<(string Path, string Text)>();
        var keys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        Collect(root, string.Empty, values, keys);
        var unescaped = string.Join('\n', values.Select(v => v.Text));
        var hits = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var token in _aliasByToken.Keys)
        {
            if (Verifiable(token) && !TokenIsAliasText(token) && unescaped.Contains(token, StringComparison.OrdinalIgnoreCase))
            {
                hits.Add(token);
            }
        }

        return hits;
    }

    private void ReplaceGlued(JsonNode? node, HashSet<string> tokens)
    {
        switch (node)
        {
            case JsonObject obj:
                foreach (var key in obj.Select(p => p.Key).ToList())
                {
                    if (obj[key] is JsonValue v && v.TryGetValue<string>(out var text))
                    {
                        obj[key] = ReplaceNoBoundary(text, tokens);
                    }
                    else
                    {
                        ReplaceGlued(obj[key], tokens);
                    }
                }

                break;
            case JsonArray array:
                for (var i = 0; i < array.Count; i++)
                {
                    if (array[i] is JsonValue v && v.TryGetValue<string>(out var text))
                    {
                        array[i] = ReplaceNoBoundary(text, tokens);
                    }
                    else
                    {
                        ReplaceGlued(array[i], tokens);
                    }
                }

                break;
        }
    }

    private string ReplaceNoBoundary(string text, HashSet<string> tokens)
    {
        var result = text;
        foreach (var token in tokens.OrderByDescending(t => t.Length))
        {
            if (result.Contains(token, StringComparison.OrdinalIgnoreCase))
            {
                result = Regex.Replace(
                    result, Regex.Escape(token), _aliasByToken[token], RegexOptions.IgnoreCase | RegexOptions.CultureInvariant, TimeSpan.FromSeconds(5));
            }
        }

        return result;
    }
}
