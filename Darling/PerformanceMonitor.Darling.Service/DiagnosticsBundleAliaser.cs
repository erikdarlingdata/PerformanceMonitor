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
        ["role"] = AliasKind.Role,
    };

    private static readonly HashSet<string> s_kept = new(KeptNames, StringComparer.OrdinalIgnoreCase);

    private const string Octet = @"(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)";
    private const string Ipv4 = Octet + @"(?:\." + Octet + "){3}";
    /* An IPv4 address written inside an IPv6 one (::ffff:203.0.113.77, 64:ff9b::203.0.113.77, 0:0:0:0:0:ffff:203.0.113.77).
       It is tried before the plain IPv6 branches, which would otherwise stop at the first octet. */
    private const string Ipv6WithIpv4 = @"(?:[0-9a-f]{0,4}:){2,7}" + Ipv4;
    private const string Ipv6 = @"(?:[0-9a-f]{1,4}:){7}[0-9a-f]{1,4}|(?:[0-9a-f]{1,4}:){1,7}:(?:[0-9a-f]{1,4}(?::[0-9a-f]{1,4}){0,5})?|::(?:[0-9a-f]{1,4}:){0,6}[0-9a-f]{1,4}";

    private readonly Dictionary<string, string> _aliasByToken = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<AliasKind, int> _counts = new();
    private readonly Dictionary<AliasKind, int> _ordinals = new();
    private readonly HashSet<string> _domainTokens = new(StringComparer.OrdinalIgnoreCase);
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
        if (!string.IsNullOrWhiteSpace(secret) && !IsKept(secret) && _secretSet.Add(secret))
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

        var name = StripProtocol(raw.Trim().Trim('[', ']', '"', '\'', ' '));
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

    /// <summary>Registers a DNS domain whole (an e-mail domain, a URL's registrable suffix) with no first-label split.</summary>
    internal void AddDomain(string? raw)
    {
        var name = raw?.Trim().Trim('[', ']', '"', '\'', ' ', '.', '>', '<');
        if (!string.IsNullOrEmpty(name) && name.Length >= TokenMinimumLength)
        {
            AliasOf(name, AliasKind.Domain);
        }
    }

    private static readonly Regex s_protocolPrefix = new(@"^(?:tcp|np|lpc|admin):", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant, TimeSpan.FromSeconds(1));

    /// <summary>
    /// SQL Server connection spellings carry a protocol prefix (<c>tcp:host,1433</c>, <c>np:\\host\pipe\sql\query</c>,
    /// <c>lpc:host</c>, <c>admin:host</c>). The prefix is dropped, and a named-pipe path is reduced to its host.
    /// </summary>
    private static string StripProtocol(string name)
    {
        var stripped = s_protocolPrefix.Replace(name, string.Empty, 1).Trim();

        /* "admin:5432" is a host called admin with a port, not a protocol prefix. */
        if (stripped.Length == name.Length || stripped.All(char.IsDigit))
        {
            return name;
        }

        if (stripped.StartsWith(@"\\", StringComparison.Ordinal))
        {
            var rest = stripped.TrimStart('\\');
            var end = rest.IndexOf('\\', StringComparison.Ordinal);
            return end < 0 ? rest : rest[..end];
        }

        return stripped;
    }

    /* A quoted WORD\word is a Windows account (domain and login): 'CORP\svc_darling' in a SQL Server 18456 message.
       The domain half is NetBIOS-shaped (a letter first, at most 15 characters, no dot), so a quoted relative path
       ('..\logs') or a dotted file name is not an account. */
    private static readonly Regex s_quotedAccount = new(
        @"(?<q>['""])(?<d>[A-Za-z][A-Za-z0-9_\-]{1,14})\\(?<l>[A-Za-z0-9_.$\-]{2,64})\k<q>",
        RegexOptions.CultureInvariant, TimeSpan.FromSeconds(2));

    /// <summary>Words that read as product or path vocabulary, not as a domain or login: a quoted <c>'server\instance'</c> is help text.</summary>
    private static readonly HashSet<string> s_vocabulary = new(StringComparer.OrdinalIgnoreCase)
    {
        "server", "host", "db", "domain", "login", "role", "ip", "tag", "instance", "database", "pg", "log", "logs",
        "data", "store", "service", "user", "path", "bin", "etc", "var", "tmp", "opt", "usr", "lib",
    };

    private static readonly Regex s_fileExtension = new(
        @"\.(?:log|json|txt|conf|cfg|ini|xml|exe|dll|md|csv|sql|tmp)$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant, TimeSpan.FromSeconds(1));

    private readonly HashSet<string> _bundleKeys = new(StringComparer.Ordinal);

    /// <summary>
    /// Records every JSON key of <paramref name="node"/>. A quoted half that is a kept word, or that has the shape of a
    /// product key (lowercase letters, digits and underscores) and IS one of these keys, is vocabulary and not an account.
    /// </summary>
    internal void NoteKeys(JsonNode? node)
    {
        switch (node)
        {
            case JsonObject obj:
                foreach (var (key, value) in obj)
                {
                    _bundleKeys.Add(key);

                    /* A status is a fixed word of the product (ok, not_present, store_unreachable), never data. */
                    if (key == "status" && value is JsonValue statusValue && statusValue.TryGetValue<string>(out var status))
                    {
                        NoteProductValues(new[] { status });
                    }

                    NoteKeys(value);
                }

                break;
            case JsonArray array:
                foreach (var item in array)
                {
                    NoteKeys(item);
                }

                break;
        }
    }

    private readonly HashSet<string> _productValues = new(StringComparer.Ordinal);

    /// <summary>
    /// Registers text the product itself writes as a value (the section names in the manifest listing). A value that is
    /// exactly one of these is the product's vocabulary, so a real name that is a substring of it (a database called
    /// <c>Store</c> against <c>store_log</c>) is not a leak there.
    /// </summary>
    internal void NoteProductValues(IEnumerable<string> values)
    {
        foreach (var value in values)
        {
            if (s_productKeyShape.IsMatch(value))
            {
                _productValues.Add(value);
            }
        }
    }

    private bool IsAccountHalf(string half, bool isLogin)
    {
        if (IsKept(half) || s_vocabulary.Contains(half) || (s_productKeyShape.IsMatch(half) && _bundleKeys.Contains(half)))
        {
            return false;
        }

        return !(isLogin && s_fileExtension.IsMatch(half));
    }

    /// <summary>Adds both halves of every quoted <c>WORD\word</c> in <paramref name="text"/>: the domain, and the login.</summary>
    private void RegisterQuotedAccounts(string text)
    {
        if (text.IndexOf('\\', StringComparison.Ordinal) < 0)
        {
            return;
        }

        try
        {
            foreach (Match m in s_quotedAccount.Matches(text))
            {
                /* The pair is an account only when both halves can be one; a path or help text registers nothing. */
                if (IsAccountHalf(m.Groups["d"].Value, isLogin: false) && IsAccountHalf(m.Groups["l"].Value, isLogin: true))
                {
                    AddName(AliasKind.Domain, m.Groups["d"].Value);
                    AddName(AliasKind.Login, m.Groups["l"].Value);
                }
            }
        }
        catch (RegexMatchTimeoutException)
        {
            /* A pathological text adds no names here; the verifier still scans for every name already known. */
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
            case JsonValue plain when plain.TryGetValue<string>(out var plainText):
                RegisterQuotedAccounts(plainText);
                break;
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
            redacted = ReplaceSecret(redacted, secret);
        }

        redacted = SecretTextGuard.RedactText(redacted);
        RegisterQuotedAccounts(redacted);
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
    /// A secret of <see cref="VerifyMinimumLength"/> characters or more is replaced wherever it occurs; a shorter one is
    /// replaced only as a whole word, so a two-character secret does not shred ordinary words.
    /// </summary>
    private static string ReplaceSecret(string text, string secret)
    {
        if (secret.Length >= VerifyMinimumLength)
        {
            return text.Replace(secret, SecretTextGuard.RedactedMarker, StringComparison.Ordinal);
        }

        try
        {
            return Regex.Replace(
                text, "(?<![A-Za-z0-9_])" + Regex.Escape(secret) + "(?![A-Za-z0-9_])", SecretTextGuard.RedactedMarker,
                RegexOptions.CultureInvariant, TimeSpan.FromSeconds(2));
        }
        catch (RegexMatchTimeoutException)
        {
            return SecretTextGuard.RedactedMarker;
        }
    }

    /// <summary>
    /// The key a source key is written under. A key whose whole text equals a known name (the same case-insensitive
    /// lookup the value pass uses) is renamed to that name's alias, so a name used as a key is never left for the verify
    /// to exempt as product vocabulary. The product's own keys (<c>status</c>, the harvest keys, the section names and
    /// the statuses) are never names. A second key that lands on an alias already written gets a numbered suffix.
    /// </summary>
    private string AliasKey(string key, JsonObject written)
    {
        if (!_aliasByToken.TryGetValue(key, out var alias) || s_productKeys.Contains(key) || HarvestKeys.ContainsKey(key) || _productValues.Contains(key))
        {
            return key;
        }

        var renamed = alias;
        for (var n = 2; written.ContainsKey(renamed); n++)
        {
            renamed = alias + "-" + n.ToString(CultureInfo.InvariantCulture);
        }

        return renamed;
    }

    private static readonly HashSet<string> s_productKeys = new(StringComparer.Ordinal) { "status", "server_id", "server_alias" };

    /// <summary>
    /// Rewrites a tree: every string value goes through <see cref="Alias"/>; every key naming a secret has its value
    /// replaced by the redaction marker at any depth; a <c>server_id</c> number becomes a <c>server_alias</c> string.
    /// A key is rewritten only when its WHOLE text is a known name (see <see cref="AliasKey"/>), so the structure
    /// survives and a monitored name used as a key never reaches the file.
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
                foreach (var (sourceKey, value) in obj)
                {
                    var key = AliasKey(sourceKey, copy);
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

        var alias = NextAlias(kind, token);
        return Map(token, alias);
    }

    private string Map(string token, string alias)
    {
        if (token.Length >= TokenMinimumLength && !IsKept(token) && !_aliasByToken.ContainsKey(token))
        {
            _aliasByToken[token] = alias;
            _tokenOrder.Add(token);
            if (alias.StartsWith("domain-", StringComparison.Ordinal))
            {
                _domainTokens.Add(token);
            }

            _scan = null;
        }

        return alias;
    }

    /// <summary>
    /// The next ordinal alias of a kind. An ordinal whose text equals a real name (a server called <c>server-1</c>, or
    /// <paramref name="avoid"/>, the token about to take the alias) is skipped, so an alias never reads as a real name.
    /// </summary>
    private string NextAlias(AliasKind kind, string? avoid = null)
    {
        _ordinals.TryGetValue(kind, out var n);
        string alias;
        do
        {
            alias = Prefix(kind) + "-" + (++n).ToString(CultureInfo.InvariantCulture);
        }
        while (_aliasByToken.ContainsKey(alias) || string.Equals(alias, avoid, StringComparison.OrdinalIgnoreCase));

        _ordinals[kind] = n;
        _counts.TryGetValue(kind, out var issued);
        _counts[kind] = issued + 1;
        return alias;
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
            if (_aliasByToken.TryGetValue(ip.Value, out var known))
            {
                return known;
            }

            /* An IPv4 address written inside an IPv6 one shares the alias of the plain IPv4 address. */
            var lastColon = ip.Value.LastIndexOf(':');
            if (lastColon >= 0 && ip.Value.IndexOf('.', StringComparison.Ordinal) > lastColon)
            {
                return Map(ip.Value, AliasOf(ip.Value[(lastColon + 1)..], AliasKind.Ip));
            }

            return AliasOf(ip.Value, AliasKind.Ip);
        }

        var dotted = match.Groups["dn"];
        if (dotted.Success)
        {
            AddName(AliasKind.Host, dotted.Value);
            return _aliasByToken.TryGetValue(dotted.Value, out var hostAlias) ? hostAlias : AliasOf(dotted.Value, AliasKind.Host);
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
        /* A dotted name under a known domain (sql02.example.test when example.test is a domain) is a host nobody registered. */
        var domains = _domainTokens.OrderByDescending(t => t.Length).ThenBy(t => t, StringComparer.Ordinal).Select(Regex.Escape).ToList();
        var dotted = domains.Count == 0 ? "(?!)" : @"[A-Za-z0-9_\-]+(?:\.[A-Za-z0-9_\-]+)*\.(?:" + string.Join('|', domains) + ")";
        var pattern = "(?<![A-Za-z0-9_])(?:(?<tok>" + alternation + ")|(?<dn>" + dotted + ")|(?<ip>" + Ipv6WithIpv4 + "|" + Ipv4 + "|" + Ipv6 + "))(?![A-Za-z0-9_])";
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
        var unescaped = string.Join('\n', values.Where(v => !_productValues.Contains(v.Text)).Select(v => v.Text));
        var leaks = new List<BundleLeak>();

        foreach (var secret in _secrets.Where(Verifiable))
        {
            if (serialized.Contains(secret, StringComparison.Ordinal) || unescaped.Contains(secret, StringComparison.Ordinal))
            {
                leaks.Add(Locate(values, secret, "secret"));
            }
        }

        var minted = new HashSet<string>(_aliasByToken.Values, StringComparer.OrdinalIgnoreCase);

        /* Alias text the generator minted (domain-1, host-3) is output, not information, so a real name such as "main"
           or "host" must not match inside it. The product's own keys are masked the same way (see MaskProductKeys). */
        var maskedValues = MaskAliasText(unescaped, minted);
        var maskedBytes = MaskAliasText(MaskProductKeys(serialized), minted);
        foreach (var token in _aliasByToken.Keys)
        {
            if (!Verifiable(token) || TokenIsAliasText(token, minted))
            {
                continue;
            }

            if (maskedValues.Contains(token, StringComparison.OrdinalIgnoreCase) || maskedBytes.Contains(token, StringComparison.OrdinalIgnoreCase))
            {
                leaks.Add(Locate(values, token, "name"));
            }
        }

        return leaks;
    }

    private static readonly Regex s_productKeyShape = new("^[a-z0-9_]+$", RegexOptions.CultureInvariant, TimeSpan.FromSeconds(1));

    /* A product-shaped string that is a key (a colon follows it) or a registered product value. */
    private static readonly Regex s_serializedProductKey = new(@"""[a-z0-9_]+""", RegexOptions.CultureInvariant, TimeSpan.FromSeconds(5));

    private static readonly Regex s_aliasShape = new(
        @"(?<![A-Za-z0-9_])(?:server|host|db|domain|login|role|ip|tag)-\d+(?![A-Za-z0-9_])",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant, TimeSpan.FromSeconds(5));

    /// <summary>
    /// Blanks every JSON key whose WHOLE text has the product's key shape (<c>^[a-z0-9_]+$</c>): keys are never rewritten,
    /// so a name that is a substring of <c>store_log</c> is the product's own vocabulary. A key with mixed case, a dot, a
    /// space or a hyphen is data-shaped and stays in the text, so a name inside it is still a leak.
    /// </summary>
    private string MaskProductKeys(string serialized) =>
        s_serializedProductKey.Replace(serialized, m =>
        {
            var isKey = serialized.AsSpan(m.Index + m.Length).TrimStart().StartsWith(":", StringComparison.Ordinal);
            return isKey || _productValues.Contains(m.Value[1..^1]) ? new string('\u0001', m.Length) : m.Value;
        });

    /// <summary>Blanks the alias text the generator minted, so a real name cannot match inside it.</summary>
    private static string MaskAliasText(string text, HashSet<string> minted) =>
        s_aliasShape.Replace(text, m => minted.Contains(m.Value) ? new string('\u0001', m.Length) : m.Value);

    /// <summary>
    /// True for text the alias generator itself mints (<c>server-3</c>, <c>ip-1</c>): a token that is exactly one of the
    /// aliases handed out. A real name that merely looks like an alias, and was never handed out, is still verified.
    /// </summary>
    private static bool TokenIsAliasText(string token, HashSet<string> minted)
    {
        return minted.Contains(token)
            && Regex.IsMatch(token, @"^(server|host|db|domain|login|role|ip|tag)-\d+$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant, TimeSpan.FromSeconds(1));
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
        var unescaped = string.Join('\n', values.Where(v => !_productValues.Contains(v.Text)).Select(v => v.Text));
        var hits = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var minted = new HashSet<string>(_aliasByToken.Values, StringComparer.OrdinalIgnoreCase);
        var masked = MaskAliasText(unescaped, minted);
        foreach (var token in _aliasByToken.Keys)
        {
            if (Verifiable(token) && !TokenIsAliasText(token, minted) && masked.Contains(token, StringComparison.OrdinalIgnoreCase))
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
                        obj[key] = _productValues.Contains(text) ? text : ReplaceNoBoundary(text, tokens);
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
                        array[i] = _productValues.Contains(text) ? text : ReplaceNoBoundary(text, tokens);
                    }
                    else
                    {
                        ReplaceGlued(array[i], tokens);
                    }
                }

                break;
        }
    }

    /// <summary>Replaces each token wherever it occurs, except inside alias text the generator minted (which is output, never information).</summary>
    private string ReplaceNoBoundary(string text, HashSet<string> tokens)
    {
        var minted = new HashSet<string>(_aliasByToken.Values, StringComparer.OrdinalIgnoreCase);
        var result = new StringBuilder(text.Length);
        var last = 0;
        foreach (Match alias in s_aliasShape.Matches(text))
        {
            if (!minted.Contains(alias.Value))
            {
                continue;
            }

            result.Append(ReplaceTokens(text[last..alias.Index], tokens));
            result.Append(alias.Value);
            last = alias.Index + alias.Length;
        }

        result.Append(ReplaceTokens(text[last..], tokens));
        return result.ToString();
    }

    private string ReplaceTokens(string text, HashSet<string> tokens)
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
