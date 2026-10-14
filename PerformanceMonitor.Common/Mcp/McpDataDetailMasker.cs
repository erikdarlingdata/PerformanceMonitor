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
/// <item>Under <see cref="McpDataDetail.MetadataOnly"/> a login or host becomes <c>login#n</c> or <c>host#n</c>,
/// numbered per answer, so one answer still shows which rows share one.</item>
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
    private readonly bool _freeText;
    private readonly Dictionary<string, string> _logins = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string> _hosts = new(StringComparer.Ordinal);

    private McpDataDetailMasker(McpDataDetail mode, bool freeText)
    {
        _mode = mode;
        _freeText = freeText;
    }

    /// <summary>
    /// Masks <paramref name="output"/> (a tool's answer, a brief, or an earlier report) at <paramref name="mode"/>.
    /// Under <see cref="McpDataDetail.Full"/>, and whenever nothing changes, the SAME instance comes back.
    /// <paramref name="freeText"/> is for text a person or an agent wrote (an earlier report composed into a later
    /// brief): every string the table does not list has its literals replaced too, under both masked levels.
    /// </summary>
    public static string MaskJson(string output, McpDataDetail mode, bool freeText = false)
    {
        if (mode == McpDataDetail.Full || string.IsNullOrEmpty(output)) return output;
        var masker = new McpDataDetailMasker(mode, freeText);
        return SensitiveStatements.JsonCore(output, masker.Value, masker.Xml, key => key, Refusal);
    }

    /// <summary>Masks one field's text (<paramref name="name"/> is its key or context label) at
    /// <paramref name="mode"/>; the composer uses it for fields it writes itself. Logins and hosts are numbered within
    /// this one call only.</summary>
    public static string? MaskField(string? name, string? value, McpDataDetail mode)
    {
        if (mode == McpDataDetail.Full || value is null) return value;
        return new McpDataDetailMasker(mode, freeText: false).Value(name ?? string.Empty, value);
    }

    /// <summary>Error text (an exception's message) at <paramref name="mode"/>.</summary>
    public static string MaskErrorText(string message, McpDataDetail mode) =>
        mode == McpDataDetail.Full || string.IsNullOrEmpty(message)
            ? message
            : new McpDataDetailMasker(mode, freeText: false).Masked(McpDataDetailClass.Statement, message);

    /// <summary>The value judge: XML by shape, everything else by its key's class. A null key is a whole answer that
    /// is not JSON, which is error text.</summary>
    private string? Value(string? key, string? value)
    {
        if (string.IsNullOrEmpty(value)) return value;
        if (StartsWithAngle(value)) return Xml(key, value);
        McpDataDetailClass cls = string.IsNullOrEmpty(key) ? McpDataDetailClass.Statement : McpDataDetailKeys.Classify(key);
        if (cls == McpDataDetailClass.Kept && _freeText) cls = McpDataDetailClass.Statement;
        return Masked(cls, value);
    }

    private string Masked(McpDataDetailClass cls, string value) => IsOwnPlaceholder(value) ? value : cls switch
    {
        McpDataDetailClass.Statement => _mode == McpDataDetail.MetadataOnly ? WithheldText : MaskLiterals(value),
        McpDataDetailClass.ParameterValue => _mode == McpDataDetail.MetadataOnly ? WithheldText : ReplacedValue,
        McpDataDetailClass.Login => _mode == McpDataDetail.MetadataOnly ? Numbered(_logins, "login#", value) : value,
        McpDataDetailClass.Host => _mode == McpDataDetail.MetadataOnly ? Numbered(_hosts, "host#", value) : value,
        _ => value,
    };

    /// <summary>The statement filter's own placeholders and this masker's own words hold no collected data, and
    /// masking their issue numbers or comment dashes would only garble them.</summary>
    private static bool IsOwnPlaceholder(string value) =>
        string.Equals(value, SensitiveStatements.PlaceholderText, StringComparison.Ordinal)
        || string.Equals(value, SensitiveStatements.JsonRefusal, StringComparison.Ordinal)
        || string.Equals(value, SensitiveStatements.WithheldPlanSentence, StringComparison.Ordinal)
        || string.Equals(value, WithheldText, StringComparison.Ordinal)
        || string.Equals(value, Refusal, StringComparison.Ordinal);

    private static string Numbered(Dictionary<string, string> seen, string prefix, string value)
    {
        if (!seen.TryGetValue(value, out string? label))
        {
            label = prefix + (seen.Count + 1).ToString(CultureInfo.InvariantCulture);
            seen[value] = label;
        }

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
    public static string MaskLiterals(string text)
    {
        if (string.IsNullOrEmpty(text)) return text;

        StringBuilder? sb = null;
        int copied = 0;
        int i = 0;
        while (i < text.Length)
        {
            char c = text[i];
            int end;
            string? replacement;
            if (c == '\'')
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
