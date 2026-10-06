/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Net;
using System.Text;
using System.Xml;

namespace PerformanceMonitor.Common;

/// <summary>
/// The XML walk of the statement filter (#4348): plan XML, blocked-process reports, deadlock graphs and
/// <c>system_health</c> event XML go through <see cref="XmlCore"/>. Every decision about WHAT is named is the
/// caller's <c>isNamed</c> predicate; this file only decides which values are judged, which are exempt and what is
/// written in place of a withheld value. Not wired to a caller yet.
/// </summary>
static partial class SensitiveStatements
{
    /// <summary>Output past the cut by this many characters is still produced, so a caller that cuts at
    /// <c>maxOutputChars</c> never sees a short result (P3).</summary>
    private const int XmlCutSlack = 64;

    /// <summary>
    /// Judges every attribute value and every text or CDATA node of <paramref name="xml"/> with
    /// <paramref name="isNamed"/> and returns the SAME instance when nothing is named. When something is named the
    /// document is rewritten with the named values replaced by <paramref name="placeholder"/>, and a statement
    /// element (<c>Stmt*</c>) whose <c>StatementText</c> or <c>ParameterizedText</c> attribute is named is withheld
    /// whole: the parameter and literal values inside it are the placeholder too (H2). Outside such a statement
    /// <c>ParameterCompiledValue</c> and <c>ParameterRuntimeValue</c> are never judged and are kept.
    /// Fails closed: a document that does not parse is judged as decoded text and withheld whole when named; any
    /// other failure, and a spent budget, withhold the whole document.
    /// </summary>
    /// <param name="xml">The document or fragment.</param>
    /// <param name="isNamed">The judge for one decoded value.</param>
    /// <param name="budgetSpent">Asked between nodes; true withholds the whole document.</param>
    /// <param name="placeholder">What a withheld value, or a withheld document, becomes.</param>
    /// <param name="chargeParse">Receives the time spent parsing and writing (everything outside
    /// <paramref name="isNamed"/>), so a budget can count it.</param>
    /// <param name="maxOutputChars">The caller will cut the result at this length: work stops once nothing before
    /// the cut can change (P3, L-I).</param>
    internal static string? XmlCore(
        string? xml,
        Func<string, bool> isNamed,
        Func<bool> budgetSpent,
        string placeholder,
        Action<TimeSpan>? chargeParse = null,
        int maxOutputChars = int.MaxValue)
    {
        if (string.IsNullOrEmpty(xml))
            return xml;

        var run = new XmlRun(isNamed, budgetSpent, placeholder, chargeParse);
        long cut = maxOutputChars == int.MaxValue ? long.MaxValue : (long)maxOutputChars + XmlCutSlack;

        XmlPassOutcome outcome;
        try
        {
            outcome = run.PassOne(xml, cut);
        }
        catch (XmlException)
        {
            // Cut, malformed, or a DTD (prohibited): judge the whole text, entities decoded.
            try
            {
                if (budgetSpent())
                    return placeholder;
                return isNamed(WebUtility.HtmlDecode(xml)) ? placeholder : xml;
            }
            catch (Exception)
            {
                return placeholder;
            }
        }
        catch (Exception)
        {
            return placeholder;
        }

        if (outcome == XmlPassOutcome.Spent)
            return placeholder;
        if (outcome == XmlPassOutcome.Clean)
            return xml;

        try
        {
            return run.PassTwo(xml, cut);
        }
        catch (Exception)
        {
            return placeholder;
        }
    }

    private enum XmlPassOutcome { Clean, Hit, Spent }

    /// <summary>One <see cref="XmlCore"/> call: the two passes share the judge, the budget and the parse timer.</summary>
    private sealed class XmlRun
    {
        private readonly Func<string, bool> _isNamed;
        private readonly Func<bool> _budgetSpent;
        private readonly string _placeholder;
        private readonly Action<TimeSpan>? _chargeParse;
        private long _mark;

        /// <summary><c>Stmt*</c> start tags seen so far, in document order. Both passes number them the same way;
        /// the auto-parameter scope (a later lane) decides from this number in <see cref="OpensScope"/>.</summary>
        private int _stmtOrdinal;

        public XmlRun(Func<string, bool> isNamed, Func<bool> budgetSpent, string placeholder, Action<TimeSpan>? chargeParse)
        {
            _isNamed = isNamed;
            _budgetSpent = budgetSpent;
            _placeholder = placeholder;
            _chargeParse = chargeParse;
        }

        private static XmlReaderSettings ReaderSettings() => new()
        {
            DtdProcessing = DtdProcessing.Prohibit,
            XmlResolver = null,
            ConformanceLevel = ConformanceLevel.Fragment,
            CheckCharacters = false,
        };

        private static bool IsStmt(string localName) => localName.StartsWith("Stmt", StringComparison.Ordinal);

        /// <summary>Kept outside a withheld statement: the values a plan stores for its own parameters.</summary>
        private static bool IsExempt(string localName) =>
            localName == "ParameterCompiledValue" || localName == "ParameterRuntimeValue";

        /// <summary>The H2 list: withheld whatever the pattern says, inside a withheld statement.</summary>
        private static bool IsScopeList(string localName) =>
            localName == "ParameterCompiledValue" || localName == "ParameterRuntimeValue"
            || localName == "ScalarString" || localName == "ConstValue" || localName == "ParameterizedText";

        private static bool IsNamespaceDeclaration(XmlReader reader) =>
            reader.Prefix == "xmlns" || (reader.Prefix.Length == 0 && reader.LocalName == "xmlns");

        /// <summary>Whether a statement element opens a withheld scope. Today: when its own text is named. The
        /// auto-parameter probe (a later lane) also opens it by <paramref name="ordinal"/>.</summary>
        private bool OpensScope(int ordinal, bool statementOrParameterizedTextNamed) => statementOrParameterizedTextNamed;

        private void ChargeParse()
        {
            if (_chargeParse is null)
                return;
            long now = Stopwatch.GetTimestamp();
            _chargeParse(Stopwatch.GetElapsedTime(_mark, now));
            _mark = now;
        }

        private bool BudgetSpent()
        {
            ChargeParse();
            return _budgetSpent();
        }

        private bool Judge(string value)
        {
            if (value.Length == 0)
                return false;
            ChargeParse();
            bool named = _isNamed(value);
            if (_chargeParse is not null)
                _mark = Stopwatch.GetTimestamp();
            return named;
        }

        /// <summary>Pass 1: read only. Returns at the first named value; nothing named returns Clean.</summary>
        public XmlPassOutcome PassOne(string xml, long cut)
        {
            int[]? lineStarts = cut == long.MaxValue ? null : LineStarts(xml);
            _mark = Stopwatch.GetTimestamp();
            _stmtOrdinal = 0;
            using var reader = XmlReader.Create(new StringReader(xml), ReaderSettings());
            var lineInfo = (IXmlLineInfo)reader;
            try
            {
                while (reader.Read())
                {
                    if (BudgetSpent())
                        return XmlPassOutcome.Spent;
                    var type = reader.NodeType;
                    if (type == XmlNodeType.Whitespace || type == XmlNodeType.SignificantWhitespace)
                        continue;
                    // L-I: stop when the START of this node is past the cut (logical position, never characters
                    // handed out: the reader reads ahead). Everything after it is cut by the caller.
                    if (lineStarts is not null && NodeStart(lineInfo, lineStarts, type) > cut)
                        return XmlPassOutcome.Clean;

                    switch (type)
                    {
                        case XmlNodeType.Element:
                            if (IsStmt(reader.LocalName))
                                _stmtOrdinal++;
                            if (!reader.HasAttributes)
                                break;
                            for (bool more = reader.MoveToFirstAttribute(); more; more = reader.MoveToNextAttribute())
                            {
                                if (IsNamespaceDeclaration(reader) || IsExempt(reader.LocalName))
                                    continue;
                                if (Judge(reader.Value))
                                    return XmlPassOutcome.Hit;
                            }
                            reader.MoveToElement();
                            break;
                        case XmlNodeType.Text:
                        case XmlNodeType.CDATA:
                            if (Judge(reader.Value))
                                return XmlPassOutcome.Hit;
                            break;
                    }
                }
                return XmlPassOutcome.Clean;
            }
            finally
            {
                ChargeParse();
            }
        }

        /// <summary>Pass 2 (a hit was found): streams the document into a writer, replacing named values and the
        /// values of a withheld statement. A budget spent in between returns the whole placeholder.</summary>
        public string PassTwo(string xml, long cut)
        {
            var sb = new StringBuilder(cut == long.MaxValue ? xml.Length + 64 : 1024);
            var sw = new StringWriter(sb, CultureInfo.InvariantCulture);
            var writer = XmlWriter.Create(sw, new XmlWriterSettings
            {
                ConformanceLevel = ConformanceLevel.Fragment,
                OmitXmlDeclaration = true,
                Indent = false,
                NewLineHandling = NewLineHandling.Entitize,
                CloseOutput = false,
                CheckCharacters = false,
            });
            _mark = Stopwatch.GetTimestamp();
            _stmtOrdinal = 0;
            var open = new Stack<string>();
            int scopeDepth = -1;
            using var reader = XmlReader.Create(new StringReader(xml), ReaderSettings());
            try
            {
                while (reader.Read())
                {
                    if (BudgetSpent())
                        return _placeholder;
                    bool scoped = scopeDepth >= 0;
                    switch (reader.NodeType)
                    {
                        case XmlNodeType.XmlDeclaration:
                            // The declaration is only ever the first node; kept exactly as it was written.
                            sw.Write("<?xml ");
                            sw.Write(reader.Value);
                            sw.Write("?>");
                            break;
                        case XmlNodeType.Element:
                            WriteElement(reader, writer, open, ref scopeDepth);
                            break;
                        case XmlNodeType.EndElement:
                            writer.WriteFullEndElement();
                            open.Pop();
                            if (scopeDepth >= 0 && reader.Depth == scopeDepth)
                                scopeDepth = -1;
                            break;
                        case XmlNodeType.Text:
                            writer.WriteString(TextValue(reader.Value, open.Count > 0 ? open.Peek() : "", scoped));
                            break;
                        case XmlNodeType.CDATA:
                            writer.WriteCData(TextValue(reader.Value, open.Count > 0 ? open.Peek() : "", scoped));
                            break;
                        case XmlNodeType.Whitespace:
                        case XmlNodeType.SignificantWhitespace:
                            writer.WriteWhitespace(reader.Value);
                            break;
                        case XmlNodeType.Comment:
                            writer.WriteComment(reader.Value);
                            break;
                        case XmlNodeType.ProcessingInstruction:
                            writer.WriteProcessingInstruction(reader.Name, reader.Value);
                            break;
                    }

                    // P3: stop once the output is past the cut. The writer buffers a few KB, so look at the
                    // length only when it could be near the cut.
                    if (cut != long.MaxValue && sb.Length + 8192 > cut)
                    {
                        writer.Flush();
                        if (sb.Length > cut)
                            return sb.ToString();
                    }
                }
                writer.Flush();
                return sb.ToString();
            }
            finally
            {
                ChargeParse();
            }
        }

        private void WriteElement(XmlReader reader, XmlWriter writer, Stack<string> open, ref int scopeDepth)
        {
            string local = reader.LocalName;
            bool isStmt = IsStmt(local);
            int ordinal = isStmt ? ++_stmtOrdinal : 0;
            bool empty = reader.IsEmptyElement;
            int depth = reader.Depth;
            bool scoped = scopeDepth >= 0;

            // A statement element opens the scope when its own StatementText or ParameterizedText is named. Its
            // attributes are judged here, once, ahead of the write, because the scope covers the start tag too.
            bool? stmtTextVerdict = null;
            bool? paramTextVerdict = null;
            bool opens = false;
            if (isStmt && !scoped && reader.HasAttributes)
            {
                for (bool more = reader.MoveToFirstAttribute(); more; more = reader.MoveToNextAttribute())
                {
                    if (reader.LocalName == "StatementText")
                        stmtTextVerdict = Judge(reader.Value);
                    else if (reader.LocalName == "ParameterizedText")
                        paramTextVerdict = Judge(reader.Value);
                }
                reader.MoveToElement();
                opens = OpensScope(ordinal, stmtTextVerdict == true || paramTextVerdict == true);
            }
            else if (isStmt && !scoped)
            {
                opens = OpensScope(ordinal, false);
            }

            bool inScope = scoped || opens;
            writer.WriteStartElement(reader.Prefix, local, reader.NamespaceURI);
            if (reader.HasAttributes)
            {
                for (bool more = reader.MoveToFirstAttribute(); more; more = reader.MoveToNextAttribute())
                {
                    string value = reader.Value;
                    if (!IsNamespaceDeclaration(reader))
                    {
                        string name = reader.LocalName;
                        bool? verdict = name == "StatementText" ? stmtTextVerdict
                            : name == "ParameterizedText" ? paramTextVerdict : null;
                        value = AttributeValue(name, value, inScope, verdict);
                    }
                    writer.WriteStartAttribute(reader.Prefix, reader.LocalName, reader.NamespaceURI);
                    writer.WriteString(value);
                    writer.WriteEndAttribute();
                }
                reader.MoveToElement();
            }

            if (empty)
            {
                writer.WriteEndElement();
            }
            else
            {
                open.Push(local);
                if (opens)
                    scopeDepth = depth;
            }
        }

        /// <summary>What an attribute value is written as. <paramref name="knownVerdict"/> is a verdict already
        /// taken for this value (the statement text of a statement element), so it is never judged twice.</summary>
        private string AttributeValue(string name, string value, bool inScope, bool? knownVerdict)
        {
            if (!inScope)
            {
                if (IsExempt(name))
                    return value;
                return (knownVerdict ?? Judge(value)) ? _placeholder : value;
            }
            if (IsScopeList(name) || value.Contains('\''))
                return _placeholder;
            return (knownVerdict ?? Judge(value)) ? _placeholder : value;
        }

        /// <summary>What a text or CDATA node is written as; <paramref name="parent"/> is its element's local name.</summary>
        private string TextValue(string value, string parent, bool inScope)
        {
            if (inScope && (IsScopeList(parent) || value.Contains('\'')))
                return _placeholder;
            return Judge(value) ? _placeholder : value;
        }

        private static int[] LineStarts(string text)
        {
            var starts = new List<int> { 0 };
            for (int i = 0; i < text.Length; i++)
            {
                char c = text[i];
                if (c == '\r')
                {
                    if (i + 1 < text.Length && text[i + 1] == '\n')
                        i++;
                    starts.Add(i + 1);
                }
                else if (c == '\n')
                {
                    starts.Add(i + 1);
                }
            }
            return starts.ToArray();
        }

        /// <summary>The offset of the node's first character. A tag's position points past its <c>&lt;</c>, so one
        /// is taken off for everything but text; the slack in the cut covers the rest.</summary>
        private static long NodeStart(IXmlLineInfo info, int[] lineStarts, XmlNodeType type)
        {
            int line = info.LineNumber;
            if (line < 1 || line > lineStarts.Length)
                return 0;
            long offset = lineStarts[line - 1] + info.LinePosition - 1;
            return type == XmlNodeType.Text ? offset : offset - 1;
        }
    }
}
