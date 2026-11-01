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
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Xml;
using System.Xml.Linq;

namespace PerformanceMonitor.Common;

/// <summary>
/// Applies <c>mcp.dataDetail</c> (#5632) to what the MCP tools hand out: the field table is
/// <see cref="McpDataDetailKeys"/>. It runs on a tool's answer (<see cref="McpDataDetailFilter"/>), on the
/// investigation brief, and on the earlier reports a brief carries (<see cref="MaskJson(string, McpDataDetail, bool)"/>
/// with <c>freeText</c>); never inside a reader, so every reader keeps returning what it read.
/// <list type="bullet">
/// <item>JSON is walked by <c>SensitiveStatements.JsonCore</c>, keyed: a value is masked by its key's class, or by its
/// context label's class (<c>{"label": "Query", "value": ...}</c>).</item>
/// <item>Any value that is XML is masked by shape whatever its key: a showplan is withheld under
/// <c>metadataOnly</c> and has its values replaced under <c>maskLiterals</c>; a deadlock or blocked-process report
/// keeps its shape, with its input buffers and frames masked as statements and its logins and hosts masked as
/// identities.</item>
/// <item>A whole answer that is not JSON is error text.</item>
/// <item>Under <see cref="McpDataDetail.MetadataOnly"/> a login or host becomes <c>login#</c> or <c>host#</c> plus 8 hex
/// characters of a keyed hash of its name (<see cref="PseudonymKey"/>), the same in every answer from one store (#6066). With no
/// key it is numbered per answer (<c>login#1</c>), which hides every name too.</item>
/// <item>It fails closed: any exception returns <see cref="Refusal"/>, never the input.</item>
/// </list>
/// </summary>
public sealed class McpDataDetailMasker
{
    /// <summary>What a replaced literal or value becomes.</summary>
    public const string ReplacedValue = "?";

    /// <summary>What a withheld field becomes.</summary>
    public const string WithheldText = "(withheld by mcp.dataDetail)";

    /// <summary>What a whole answer becomes when the masker could not read it.</summary>
    public const string Refusal = "Output withheld: the mcp.dataDetail masker could not read this result.";

    private readonly McpDataDetail _mode;
    private readonly Dictionary<string, string> _logins = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string> _hosts = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string> _loginsTaken = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string> _hostsTaken = new(StringComparer.Ordinal);
    private readonly PseudonymKey? _key;

    private McpDataDetailMasker(McpDataDetail mode, PseudonymKey? key)
    {
        _mode = mode;
        _key = key;
    }

    /// <summary>
    /// One answer's masker at <paramref name="mode"/>, with no pseudonym key: under <c>metadataOnly</c> a login or host is
    /// numbered per answer (<c>login#1</c>), the numbering used when a store's key is not available (#6066). For
    /// <c>full</c> and <c>maskLiterals</c>, which keep logins and hosts, this is the whole API; every <c>metadataOnly</c>
    /// call site passes the store's key through <see cref="ForAnswer(McpDataDetail, PseudonymKey?)"/>.
    /// </summary>
    public static McpDataDetailMasker ForAnswer(McpDataDetail mode) => ForAnswer(mode, null);

    /// <summary>
    /// One answer's masker at <paramref name="mode"/> (#5632, #6066): every field, JSON document and free text it masks shares
    /// one set of pseudonyms. Under <c>metadataOnly</c> with a <paramref name="key"/>, a login is
    /// <c>login#</c> plus 8 hex from the key's keyed hash of its name, the same in every answer from one store; two different
    /// names that hash to one pseudonym in this answer get a <c>-2</c>, <c>-3</c> suffix, so they never share one. With no
    /// key (not read yet, or the read failed) the numbering is per answer, <c>login#1</c>, which hides every name too.
    /// Make one per answer (a tool's result, a brief); never share one across answers.
    /// </summary>
    public static McpDataDetailMasker ForAnswer(McpDataDetail mode, PseudonymKey? key)
    {
        if (mode == McpDataDetail.MetadataOnly && key is null) PseudonymKeyNotice.NoKey();
        return new(mode, key);
    }

    /// <summary>
    /// Masks <paramref name="output"/> (a tool's answer, a brief, or an earlier report) at <paramref name="mode"/>.
    /// Under <see cref="McpDataDetail.Full"/>, and whenever nothing changes, the SAME instance comes back.
    /// <paramref name="freeText"/> is for text a person or an agent wrote (an earlier report composed into a later
    /// brief): every string the table does not list has its literals replaced too, under both masked levels.
    /// </summary>
    public static string MaskJson(string output, McpDataDetail mode, bool freeText = false, PseudonymKey? key = null) =>
        ForAnswer(mode, key).Json(output, freeText);

    /// <summary>Masks one field's text (<paramref name="name"/> is its key or context label) at
    /// <paramref name="mode"/>. Logins and hosts are numbered within this one call only: an answer that masks several
    /// fields uses one <see cref="ForAnswer"/> instance instead.</summary>
    public static string? MaskField(string? name, string? value, McpDataDetail mode, PseudonymKey? key = null) => ForAnswer(mode, key).Field(name, value);

    /// <summary>Error text (an exception's message) at <paramref name="mode"/>.</summary>
    public static string MaskErrorText(string message, McpDataDetail mode, PseudonymKey? key = null) => ForAnswer(mode, key).ErrorText(message);

    /// <summary>This answer's JSON <paramref name="output"/>, as <see cref="MaskJson(string, McpDataDetail, bool)"/>
    /// masks it, numbering logins and hosts with every other field of this answer.</summary>
    public string Json(string output, bool freeText = false)
    {
        if (_mode == McpDataDetail.Full || string.IsNullOrEmpty(output)) return output;
        return SensitiveStatements.JsonCore(output, (key, value) => Value(key, value, freeText), Xml, key => key, Refusal);
    }

    /// <summary>
    /// A tool's whole JSON answer, as the host filter masks it (#5632 Phase A, L8 ruling). The status envelope's
    /// <c>message</c> (the top-level <c>message</c> of an object that also carries a top-level string <c>status</c>) is
    /// Darling's own sentence the operator must read: a <c>precondition</c> names the fix, an <c>unavailable</c> says
    /// what is missing. It is masked as free text under both levels (its literals and digits replaced) and never
    /// withheld; under <c>metadataOnly</c> the logins and client hosts it quotes are also numbered
    /// (<see cref="NumberEnvelopeNames"/>). Every other field, a <c>message</c> key inside a result row included, is masked by
    /// <see cref="Json(string, bool)"/> as before. The SAME instance when nothing changed.
    /// </summary>
    public string Answer(string output)
    {
        if (_mode == McpDataDetail.Full || string.IsNullOrEmpty(output)) return output;
        if (!TryEnvelope(output, out JsonObject? root, out string? message)) return Json(output);

        // Keep the key in its place while the rest is masked: a JSON null passes the walker untouched.
        root!["message"] = null;
        string rest = root.ToJsonString(McpHelpers.JsonOptions);
        string maskedRest = Json(rest);
        if (string.Equals(maskedRest, Refusal, StringComparison.Ordinal)) return Refusal;

        string maskedMessage = IsOwnPlaceholder(message!) ? message! : MaskLiterals(NumberEnvelopeNames(message!));
        if (ReferenceEquals(maskedRest, rest) && string.Equals(maskedMessage, message, StringComparison.Ordinal)) return output;

        JsonObject masked = JsonNode.Parse(maskedRest)!.AsObject();
        masked["message"] = maskedMessage;
        return masked.ToJsonString(McpHelpers.JsonOptions);
    }

    /// <summary>
    /// Under <c>metadataOnly</c>, the logins and client hosts a status sentence quotes, pseudonymised like every
    /// other login and host of this answer (#5632 review L5, #6066). <see cref="MaskLiterals"/> keeps double-quoted and bracketed
    /// text as identifiers, and a <c>precondition</c> sentence appends the monitored server's own words (SQL Server 916:
    /// <c>The server principal "DOMAIN\login" is not able to access the database "db"</c>; PostgreSQL:
    /// <c>... for user "monitor"</c>), so the double-quoted name after <c>principal</c>, <c>user</c>, <c>login</c> or
    /// <c>role</c> becomes a login number and a <c>[CLIENT: ...]</c> address a host number. A PostgreSQL predefined role
    /// (<c>pg_</c> prefix, which no created role may carry) is the grant the sentence names, not a login, and is kept.
    /// The sentence is masked, never withheld. Under <c>maskLiterals</c> logins and hosts are kept, as everywhere.
    /// </summary>
    private string NumberEnvelopeNames(string message)
    {
        if (_mode != McpDataDetail.MetadataOnly) return message;
        string text = s_envelopeQuotedLogin.Replace(message, m =>
        {
            string name = m.Groups["name"].Value.Replace("\"\"", "\"", StringComparison.Ordinal);
            return name.StartsWith("pg_", StringComparison.Ordinal)
                ? m.Value
                : m.Groups["lead"].Value + "\"" + Pseudonym(Login, name) + "\"";
        });
        return s_envelopeClient.Replace(text, m =>
            m.Groups["lead"].Value + Pseudonym(Host, m.Groups["host"].Value.Trim()) + "]");
    }

    private static readonly Regex s_envelopeQuotedLogin = new(
        @"(?<lead>\b(?:principal|user|login|role)\s+)""(?<name>(?:[^""]|"""")*)""",
        RegexOptions.NonBacktracking | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    private static readonly Regex s_envelopeClient = new(
        @"(?<lead>\[CLIENT:\s*)(?<host>[^\]]*)\]",
        RegexOptions.NonBacktracking | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    /// <summary>True when <paramref name="output"/> is a JSON object whose top level carries a string <c>status</c>
    /// and a string <c>message</c>: the status envelope.</summary>
    private static bool TryEnvelope(string output, out JsonObject? root, out string? message)
    {
        root = null;
        message = null;
        if (!output.TrimStart().StartsWith('{')) return false;
        try
        {
            if (JsonNode.Parse(output) is not JsonObject parsed) return false;
            if (parsed["status"] is not JsonValue status || !status.TryGetValue(out string? _)) return false;
            if (parsed["message"] is not JsonValue text || !text.TryGetValue(out message)) return false;
            root = parsed;
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    /// <summary>One field of this answer, masked by <paramref name="name"/>'s class (a key or a context label); a name
    /// the table does not list is kept.</summary>
    public string? Field(string? name, string? value) =>
        _mode == McpDataDetail.Full || value is null ? value : Value(name ?? string.Empty, value, freeText: false);

    /// <summary>Text a person or an agent wrote, or engine prose that can quote collected text (a note, a report's
    /// root cause, a finding's story): statement text, so its literals are replaced under <c>maskLiterals</c> and it is
    /// withheld under <c>metadataOnly</c>; XML is masked by its shape.</summary>
    public string? FreeText(string? text) =>
        _mode == McpDataDetail.Full || text is null ? text : Value(null, text, freeText: false);

    /// <summary>Error text of this answer: statement text.</summary>
    public string ErrorText(string message) =>
        _mode == McpDataDetail.Full || string.IsNullOrEmpty(message) ? message : Masked(McpDataDetailClass.Statement, message);

    /// <summary>The value judge: XML by shape, everything else by its key's class. A null key is a whole answer that
    /// is not JSON, which is error text. A comma-joined list of logins or hosts (<c>Excluded Logins</c>,
    /// <see cref="McpDataDetailKeys.IsCommaList"/>) is masked name by name.</summary>
    private string? Value(string? key, string? value, bool freeText)
    {
        if (string.IsNullOrEmpty(value)) return value;
        if (StartsWithAngle(value)) return Xml(key, value);
        McpDataDetailClass cls = string.IsNullOrEmpty(key) ? McpDataDetailClass.Statement : McpDataDetailKeys.Classify(key);
        if (cls == McpDataDetailClass.Kept && freeText) cls = McpDataDetailClass.Statement;
        if (cls is McpDataDetailClass.Login or McpDataDetailClass.Host
            && _mode == McpDataDetail.MetadataOnly
            && McpDataDetailKeys.IsCommaList(key)
            && !IsOwnPlaceholder(value))
        {
            return string.Join(", ", value
                .Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
                .Select(name => Masked(cls, name)));
        }

        return Masked(cls, value);
    }

    private string Masked(McpDataDetailClass cls, string value) => IsOwnPlaceholder(value) ? value : cls switch
    {
        McpDataDetailClass.Statement => _mode == McpDataDetail.MetadataOnly ? WithheldText : MaskLiterals(value),
        McpDataDetailClass.ParameterValue => _mode == McpDataDetail.MetadataOnly ? WithheldText : ReplacedValue,
        McpDataDetailClass.Login => _mode == McpDataDetail.MetadataOnly ? Pseudonym(Login, value) : value,
        McpDataDetailClass.Host => _mode == McpDataDetail.MetadataOnly ? Pseudonym(Host, value) : value,
        _ => value,
    };

    /// <summary>The statement filter's own placeholders and this masker's own words hold no collected data, and
    /// masking their issue numbers or comment dashes would only garble them. A pseudonym this masker (or an earlier one on
    /// the same store) made is one too, and is never masked again (#6066): <c>login#</c> or <c>host#</c> plus 8 lowercase
    /// hex with an optional <c>-n</c>, and the per-answer <c>login#1</c> form a stored brief may carry.</summary>
    private static bool IsOwnPlaceholder(string value) =>
        string.Equals(value, SensitiveStatements.PlaceholderText, StringComparison.Ordinal)
        || string.Equals(value, SensitiveStatements.JsonRefusal, StringComparison.Ordinal)
        || string.Equals(value, SensitiveStatements.WithheldPlanSentence, StringComparison.Ordinal)
        || string.Equals(value, WithheldText, StringComparison.Ordinal)
        || string.Equals(value, Refusal, StringComparison.Ordinal)
        || s_pseudonym.IsMatch(value);

    private static readonly Regex s_pseudonym = new(
        @"\A(?:login|host)#(?:[0-9a-f]{8}(?:-[0-9]+)?|[0-9]+)\z",
        RegexOptions.NonBacktracking | RegexOptions.CultureInvariant);

    private const string Login = "login";
    private const string Host = "host";

    /// <summary>
    /// The pseudonym of <paramref name="value"/> as a <paramref name="kind"/> in this answer. With the store's key it is the
    /// keyed hash (<see cref="PseudonymKey.Hex8"/>), the same name in every answer; a different name that lands on a pseudonym
    /// this answer already gave another name takes the next free <c>-n</c> suffix. With no key it is the per-answer number
    /// <c>login#1</c> (#6066): still no name, never the real one.
    /// </summary>
    private string Pseudonym(string kind, string value)
    {
        Dictionary<string, string> seen = kind == Login ? _logins : _hosts;
        Dictionary<string, string> taken = kind == Login ? _loginsTaken : _hostsTaken;
        string prefix = kind + "#";
        if (_key is null)
        {
            if (!seen.TryGetValue(value, out string? numbered))
            {
                numbered = prefix + (seen.Count + 1).ToString(CultureInfo.InvariantCulture);
                seen[value] = numbered;
            }

            return numbered;
        }

        string name = value.Trim().ToLowerInvariant();
        if (seen.TryGetValue(name, out string? label)) return label;

        string basis = _key.Pseudonym(kind, name);
        label = basis;
        for (int suffix = 2; taken.ContainsKey(label); suffix++)
            label = basis + "-" + suffix.ToString(CultureInfo.InvariantCulture);
        seen[name] = label;
        taken[label] = name;
        return label;
    }

    /// <summary>
    /// An XML value. A showplan is withheld whole under <c>metadataOnly</c>. Otherwise every attribute and every
    /// text node is masked by its name's class (a text node takes its element's name, or the <c>name</c> attribute of
    /// its element or parent, the Extended Events <c>&lt;data name="sql_text"&gt;&lt;value&gt;</c> shape); the plan
    /// value attributes (<c>SensitiveStatements.IsPlanValueAttribute</c>) are parameter values, and a nested plan is
    /// masked as a plan. XML that does not parse is withheld.
    /// </summary>
    private string? Xml(string? key, string? value)
    {
        if (string.IsNullOrEmpty(value)) return value;

        XDocument doc;
        try
        {
            using var reader = XmlReader.Create(new StringReader(value), new XmlReaderSettings
            {
                DtdProcessing = DtdProcessing.Prohibit,
                XmlResolver = null,
            });
            doc = XDocument.Load(reader, LoadOptions.PreserveWhitespace);
        }
        catch (XmlException)
        {
            return WithheldText;
        }

        if (doc.Root is null) return value;
        if (_mode == McpDataDetail.MetadataOnly && IsShowplan(doc.Root)) return WithheldText;

        bool changed = false;
        foreach (XElement element in doc.Root.DescendantsAndSelf())
        {
            foreach (XAttribute attribute in element.Attributes())
            {
                if (attribute.IsNamespaceDeclaration) continue;
                string masked = MaskXmlValue(attribute.Name.LocalName, attribute.Value);
                if (string.Equals(masked, attribute.Value, StringComparison.Ordinal)) continue;
                attribute.Value = masked;
                changed = true;
            }

            foreach (XText text in element.Nodes().OfType<XText>())
            {
                if (string.IsNullOrWhiteSpace(text.Value)) continue;
                string masked = MaskXmlValue(TextKey(element), text.Value);
                if (string.Equals(masked, text.Value, StringComparison.Ordinal)) continue;
                text.Value = masked;
                changed = true;
            }
        }

        if (!changed) return value;
        return doc.Declaration is null
            ? doc.Root.ToString(SaveOptions.DisableFormatting)
            : doc.Declaration + doc.Root.ToString(SaveOptions.DisableFormatting);
    }

    private string MaskXmlValue(string name, string value)
    {
        if (SensitiveStatements.IsPlanValueAttribute(name))
        {
            if (name == "EdcShowplanXml") return Xml(name, value) ?? value;
            if (name == "ParameterizedText") return Masked(McpDataDetailClass.Statement, value);
            return Masked(McpDataDetailClass.ParameterValue, value);
        }

        return Masked(McpDataDetailKeys.Classify(name), value);
    }

    private static string TextKey(XElement element)
    {
        string? named = element.Attribute("name")?.Value ?? element.Parent?.Attribute("name")?.Value;
        return McpDataDetailKeys.Classify(element.Name.LocalName) == McpDataDetailClass.Kept && named is not null
            ? named
            : element.Name.LocalName;
    }

    private static bool IsShowplan(XElement root) =>
        string.Equals(root.Name.LocalName, "ShowPlanXML", StringComparison.OrdinalIgnoreCase);

    private static bool StartsWithAngle(string s)
    {
        foreach (char c in s)
        {
            if (char.IsWhiteSpace(c)) continue;
            return c == '<';
        }

        return false;
    }

    /// <summary>
    /// <paramref name="text"/> with every literal replaced: a quoted string (<c>'...'</c>, with <c>''</c> inside it) and
    /// a PostgreSQL dollar-quoted string become <c>'?'</c>; a number that starts a token (<c>42</c>, <c>1.5e3</c>,
    /// <c>0x1F</c>) becomes <c>?</c>; a comment becomes an empty comment, because a comment can hold anything.
    /// Identifiers are kept: bracketed and double-quoted names, and digits inside a name (<c>t1</c>, <c>@p1</c>,
    /// <c>$1</c>, <c>#tmp2</c>). The SAME instance comes back when there is nothing to replace.
    /// </summary>
    public static string MaskLiterals(string text) => MaskCore(text, prose: false);

    /// <summary>
    /// #7106: <see cref="MaskLiterals"/> for text an agent or a person wrote in sentences, such as a report notice's root
    /// cause. An apostrophe between two letters (<c>session's</c>, <c>doesn't</c>, <c>O'Brien</c>) is part of a word, not
    /// the start of a quoted string, so the rest of the sentence stays readable. A string prefix keeps its literal:
    /// <c>N'abc'</c>, <c>'abc'</c> and <c>'it''s'</c> are still masked. Everything else is as <see cref="MaskLiterals"/>.
    /// SQL text keeps using <see cref="MaskLiterals"/>, where an apostrophe is always a quote.
    /// </summary>
    public static string MaskProse(string text) => MaskCore(text, prose: true);

    private static string MaskCore(string text, bool prose)
    {
        if (string.IsNullOrEmpty(text)) return text;

        text = MaskUnquotedErrorValues(text);

        StringBuilder? sb = null;
        int copied = 0;
        int i = 0;
        while (i < text.Length)
        {
            char c = text[i];
            int end;
            string? replacement;
            if (c == '\'' && prose && IsWordApostrophe(text, i))
            {
                i++;
                continue;
            }
            else if (c == '\'')
            {
                end = SkipQuoted(text, i, '\'');
                replacement = "'?'";
            }
            else if (c == '"')
            {
                i = SkipQuoted(text, i, '"');
                continue;
            }
            else if (c == '[')
            {
                int close = text.IndexOf(']', i + 1);
                i = close < 0 ? text.Length : close + 1;
                continue;
            }
            else if (c == '-' && i + 1 < text.Length && text[i + 1] == '-')
            {
                int eol = text.IndexOfAny(['\r', '\n'], i);
                end = eol < 0 ? text.Length : eol;
                replacement = "--";
            }
            else if (c == '/' && i + 1 < text.Length && text[i + 1] == '*')
            {
                int close = text.IndexOf("*/", i + 2, StringComparison.Ordinal);
                end = close < 0 ? text.Length : close + 2;
                replacement = "/**/";
            }
            else if (c == '$' && !PrecededByName(text, i) && DollarTag(text, i) is { } tag)
            {
                int close = text.IndexOf(tag, i + tag.Length, StringComparison.Ordinal);
                end = close < 0 ? text.Length : close + tag.Length;
                replacement = "'?'";
            }
            else if (char.IsAsciiDigit(c) && !PrecededByName(text, i))
            {
                end = i + 1;
                while (end < text.Length && (char.IsAsciiLetterOrDigit(text[end]) || text[end] == '.'))
                {
                    end++;
                }

                replacement = ReplacedValue;
            }
            else
            {
                i++;
                continue;
            }

            sb ??= new StringBuilder(text.Length);
            sb.Append(text, copied, i - copied).Append(replacement);
            copied = end;
            i = end;
        }

        if (sb is null) return text;
        sb.Append(text, copied, text.Length - copied);
        return sb.ToString();
    }

    /// <summary>A PostgreSQL error DETAIL's key, <c>Key (col)=(value) already exists.</c> and its foreign-key and
    /// exclusion-constraint forms: the value is printed UNQUOTED, so the quoted-literal scan below misses a string in it.
    /// Matched by its <c>(cols)=(values)</c> shape, which every message catalogue keeps, and the value list ends at the
    /// first <c>)</c> that is followed by white space, a period, a comma or the end of a line (#5632).</summary>
    private static readonly Regex s_pgKeyValue = new(
        @"(\((?:[^()\r\n]|\([^()\r\n]*\))*\)=\()([^\r\n]*?)(\)(?:[\s.,;]|$))",
        RegexOptions.NonBacktracking | RegexOptions.CultureInvariant | RegexOptions.Multiline);

    /// <summary>A NOT NULL or CHECK failure's DETAIL, <c>Failing row contains (v1, v2, ...).</c>: the whole row,
    /// unquoted, to the last <c>)</c> on its line (#5632).</summary>
    private static readonly Regex s_pgFailingRow = new(
        @"(Failing row contains \()([^\r\n]*)(\))",
        RegexOptions.NonBacktracking | RegexOptions.CultureInvariant);

    /// <summary><paramref name="text"/> with the unquoted values of a PostgreSQL error DETAIL replaced by
    /// <c>?</c>: the <c>Key (col)=(value)</c> of a unique, foreign-key or exclusion violation and the row of a
    /// <c>Failing row contains (...)</c>. The column names are kept. The SAME instance comes back when neither is
    /// present.</summary>
    private static string MaskUnquotedErrorValues(string text)
    {
        if (text.Contains(")=(", StringComparison.Ordinal))
        {
            text = s_pgKeyValue.Replace(text, "$1" + ReplacedValue + "$3");
        }

        if (text.Contains("Failing row contains (", StringComparison.Ordinal))
        {
            text = s_pgFailingRow.Replace(text, "$1" + ReplacedValue + "$3");
        }

        return text;
    }

    /// <summary>The index just past the quote that closes the one at <paramref name="start"/>; a doubled quote is
    /// part of the text. An unclosed quote runs to the end.</summary>
    private static int SkipQuoted(string text, int start, char quote)
    {
        int i = start + 1;
        while (i < text.Length)
        {
            if (text[i] == quote)
            {
                if (i + 1 < text.Length && text[i + 1] == quote)
                {
                    i += 2;
                    continue;
                }

                return i + 1;
            }

            i++;
        }

        return text.Length;
    }

    /// <summary>True when the apostrophe at <paramref name="index"/> sits between two letters of one word (#7106). A single
    /// string-prefix letter that starts a token (<c>N'abc'</c>, <c>E'abc'</c>, <c>B'101'</c>, <c>X'1F'</c>) is not a word, so
    /// that apostrophe still opens a literal.</summary>
    private static bool IsWordApostrophe(string text, int index)
    {
        if (index == 0 || index + 1 >= text.Length) return false;
        if (!char.IsLetter(text[index - 1]) || !char.IsLetter(text[index + 1])) return false;
        bool prefixLetter = "NnEeBbXx".Contains(text[index - 1])
            && (index == 1 || !(char.IsLetterOrDigit(text[index - 2]) || text[index - 2] == '_'));
        return !prefixLetter;
    }

    /// <summary>True when the character before <paramref name="index"/> continues a name, so a digit there is part of
    /// the name (<c>t1</c>, <c>@p1</c>, <c>$1</c>, <c>#t2</c>) and a <c>$</c> is not a dollar quote.</summary>
    private static bool PrecededByName(string text, int index)
    {
        if (index == 0) return false;
        char p = text[index - 1];
        return char.IsLetterOrDigit(p) || p == '_' || p == '@' || p == '#' || p == '$';
    }

    /// <summary>The PostgreSQL dollar-quote tag that opens at <paramref name="start"/> (<c>$$</c> or <c>$tag$</c>), or
    /// null when the text there is not one (a positional parameter <c>$1</c> is not).</summary>
    private static string? DollarTag(string text, int start)
    {
        int i = start + 1;
        while (i < text.Length && (char.IsAsciiLetter(text[i]) || text[i] == '_' || (i > start + 1 && char.IsAsciiDigit(text[i]))))
        {
            i++;
        }

        return i < text.Length && text[i] == '$' ? text.Substring(start, i - start + 1) : null;
    }
}
