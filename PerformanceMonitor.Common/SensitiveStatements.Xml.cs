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
public static partial class SensitiveStatements
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
                // A plan that does not parse cannot be probed. When the probe could have mattered (an
                // auto-parameter token, and parameter values to put back) the stored text names nothing on its own,
                // so the document is withheld whole without asking the judge.
                if (XmlRun.ProbeCouldHaveMattered(xml))
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
        /// pass 1 records the ordinals the auto-parameter probe names and pass 2 opens a scope for each.</summary>
        private int _stmtOrdinal;

        /// <summary>Ordinals of the statements whose probe (or element-form <c>ParameterizedText</c>) is named.</summary>
        private readonly HashSet<int> _probeNamed = new();
        // #5320: pass 1 stops by INPUT offset and pass 2 by OUTPUT length, and pass 2 can write input that lies past
        // pass 1's stop (a withheld long text and every &apos; write back shorter than they were read). Those nodes
        // were never probed, so the values the filter exempts outside a scope are withheld from the stop on.
        private int[]? _lineStarts;
        private long _passOneStop = long.MaxValue;
        private bool _pastVetted;

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

        /// <summary>Whether a statement element opens a withheld scope: its own text is named, or pass 1 named its
        /// auto-parameter probe (or its element-form <c>ParameterizedText</c>).</summary>
        private bool OpensScope(int ordinal, bool statementOrParameterizedTextNamed) =>
            statementOrParameterizedTextNamed || _probeNamed.Contains(ordinal);

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

        /// <summary>One open element in pass 1: its local name, and for a <c>Stmt*</c> element the statement state.</summary>
        private readonly struct OpenElement
        {
            public OpenElement(string name, StmtFrame? stmt)
            {
                Name = name;
                Stmt = stmt;
            }

            public string Name { get; }
            public StmtFrame? Stmt { get; }
        }

        /// <summary>A statement open in pass 1. <see cref="Text"/> and <see cref="Values"/> exist only when its
        /// <c>StatementText</c> holds an auto-parameter token: they are what the probe is made from.</summary>
        private sealed class StmtFrame
        {
            public StmtFrame(int ordinal, string? text, bool takesValues)
            {
                Ordinal = ordinal;
                Text = text;
                TakesValues = takesValues;
            }

            public int Ordinal { get; }
            public string? Text { get; }

            /// <summary>The statement has a <c>StatementText</c> (with a token or without): its parameter values are
            /// kept so each can be judged, even when no token in the text puts one back (#5320).</summary>
            public bool TakesValues { get; }
            public Dictionary<string, (string? Compiled, string? Runtime)>? Values { get; private set; }

            public void SetValue(string column, string? compiled, string? runtime)
            {
                Values ??= new Dictionary<string, (string? Compiled, string? Runtime)>(StringComparer.Ordinal);
                Values[column] = (compiled, runtime);
            }
        }

        /// <summary>Whether the raw text could hold an auto-parameter token or an element-form
        /// <c>ParameterizedText</c>: when it cannot, pass 1 returns at the first hit as it always did. Cheap and
        /// one-sided (a false yes only costs a full read), and an entity-encoded <c>@</c> counts as a yes.</summary>
        private static bool MayNeedProbe(string xml)
        {
            if (xml.Contains("ParameterizedText>", StringComparison.Ordinal)
                || xml.Contains("&#64;", StringComparison.Ordinal)
                || xml.Contains("&#x40;", StringComparison.OrdinalIgnoreCase))
                return true;
            for (int i = xml.IndexOf('@'); i >= 0 && i + 1 < xml.Length; i = xml.IndexOf('@', i + 1))
            {
                if (xml[i + 1] >= '0' && xml[i + 1] <= '9')
                    return true;
            }
            return false;
        }

        private static bool IsWordChar(char c) => char.IsLetterOrDigit(c) || c == '_';

        /// <summary>The next auto-parameter token at or after <paramref name="from"/>: <c>@</c> and digits, not
        /// preceded by <c>@</c> or a word character, not followed by a word character. A linear loop, no regex.</summary>
        private static bool NextToken(string text, ref int from, out int start, out int end)
        {
            start = end = 0;
            int i = from < text.Length ? text.IndexOf('@', from) : -1;
            while (i >= 0)
            {
                bool tokenStart = i == 0 || (text[i - 1] != '@' && !IsWordChar(text[i - 1]));
                int j = i + 1;
                while (j < text.Length && text[j] >= '0' && text[j] <= '9')
                    j++;
                if (tokenStart && j > i + 1 && (j == text.Length || !IsWordChar(text[j])))
                {
                    start = i;
                    end = j;
                    from = j;
                    return true;
                }
                i = j < text.Length ? text.IndexOf('@', j) : -1;
            }
            from = text.Length;
            return false;
        }

        /// <summary>The parse-failure rule: the raw text holds an auto-parameter token AND a parameter value
        /// attribute, so a probe would have been made had the plan parsed.</summary>
        internal static bool ProbeCouldHaveMattered(string xml) =>
            (xml.Contains("ParameterCompiledValue", StringComparison.Ordinal)
                || xml.Contains("ParameterRuntimeValue", StringComparison.Ordinal))
            && HasToken(xml);

        private static bool HasToken(string text)
        {
            int from = 0;
            return NextToken(text, ref from, out _, out _);
        }

        /// <summary>The statement as SQL Server would have run it ad hoc: <c>StatementText</c> with every token
        /// replaced by that statement's compiled value, else its runtime value, else <c>N'?'</c> (also a value
        /// past the cut, which pass 1 never reached). With <paramref name="preferRuntime"/> the runtime value
        /// comes first: an actual plan's runtime value is the literal of the run that executed (#5320).
        /// <paramref name="used"/> collects the tokens the text holds.</summary>
        private static string ProbeOf(StmtFrame frame, bool preferRuntime, HashSet<string>? used = null)
        {
            string text = frame.Text!;
            var sb = new StringBuilder(text.Length + 16);
            int copied = 0;
            int from = 0;
            while (NextToken(text, ref from, out int start, out int end))
            {
                sb.Append(text, copied, start - copied);
                string value = "N'?'";
                string token = text.Substring(start, end - start);
                used?.Add(token);
                if (frame.Values is not null && frame.Values.TryGetValue(token, out var v))
                    value = (preferRuntime ? v.Runtime ?? v.Compiled : v.Compiled ?? v.Runtime) ?? "N'?'";
                sb.Append(value);
                copied = end;
            }
            sb.Append(text, copied, text.Length - copied);
            return sb.ToString();
        }

        private static bool IsAutoToken(string name)
        {
            if (name.Length < 2 || name[0] != '@')
                return false;
            for (int i = 1; i < name.Length; i++)
            {
                if (name[i] < '0' || name[i] > '9')
                    return false;
            }
            return true;
        }

        /// <summary>Judges one finished (or cut) statement's probe; a named one is recorded by ordinal. Three
        /// readings, any of which names the statement (#5320): the compiled-first probe; a runtime-first probe,
        /// built only when some value's runtime differs from its compiled one (an actual plan); and each
        /// auto-parameter value whose token the stored text does not hold (text shorter than the statement),
        /// judged on its own as text. Application-named parameters stay out.</summary>
        private bool JudgeProbe(StmtFrame frame)
        {
            if (frame.Text is null && frame.Values is null)
                return false;
            var used = new HashSet<string>(StringComparer.Ordinal);
            bool named = false;
            if (frame.Text is not null)
            {
                named = Judge(ProbeOf(frame, preferRuntime: false, used));
                if (!named && AnyRuntimeDiffers(frame))
                    named = Judge(ProbeOf(frame, preferRuntime: true));
            }
            if (!named && frame.Values is not null)
            {
                foreach (var entry in frame.Values)
                {
                    if (used.Contains(entry.Key) || !IsAutoToken(entry.Key))
                        continue;
                    if ((entry.Value.Compiled is not null && Judge(entry.Value.Compiled))
                        || (entry.Value.Runtime is not null && Judge(entry.Value.Runtime)))
                    {
                        named = true;
                        break;
                    }
                }
            }
            if (!named)
                return false;
            _probeNamed.Add(frame.Ordinal);
            return true;
        }

        private static bool AnyRuntimeDiffers(StmtFrame frame)
        {
            if (frame.Values is null)
                return false;
            foreach (var v in frame.Values.Values)
            {
                if (v.Compiled is not null && v.Runtime is not null && !string.Equals(v.Compiled, v.Runtime, StringComparison.Ordinal))
                    return true;
            }
            return false;
        }

        private static StmtFrame? InnermostStmt(Stack<OpenElement> open)
        {
            foreach (var element in open)
            {
                if (element.Stmt is not null)
                    return element.Stmt;
            }
            return null;
        }

        /// <summary>Pass 1: read only. With no auto-parameter token (and no element-form
        /// <c>ParameterizedText</c>) in the plan it returns at the first named value. Otherwise it keeps reading to
        /// finish the probe set: each <c>ParameterList</c> column belongs to the innermost open statement, and a
        /// statement's probe is judged at its end tag, or at the stop (the cut) for every statement still open. After
        /// the first hit nothing else is judged here; pass 2 judges the rest.</summary>
        public XmlPassOutcome PassOne(string xml, long cut)
        {
            int[]? lineStarts = cut == long.MaxValue ? null : LineStarts(xml);
            _lineStarts = lineStarts;
            _passOneStop = long.MaxValue;
            bool readOn = MayNeedProbe(xml);
            bool hit = false;
            _mark = Stopwatch.GetTimestamp();
            _stmtOrdinal = 0;
            _probeNamed.Clear();
            var open = new Stack<OpenElement>();
            int parameterLists = 0;
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
                    {
                        _passOneStop = NodeStart(lineInfo, lineStarts, type);
                        break;
                    }

                    switch (type)
                    {
                        case XmlNodeType.Element:
                        {
                            string local = reader.LocalName;
                            bool isStmt = IsStmt(local);
                            bool isColumn = readOn && parameterLists > 0 && local == "ColumnReference";
                            string? column = null, compiled = null, runtime = null, statementText = null;
                            if (reader.HasAttributes)
                            {
                                for (bool more = reader.MoveToFirstAttribute(); more; more = reader.MoveToNextAttribute())
                                {
                                    if (IsNamespaceDeclaration(reader))
                                        continue;
                                    string name = reader.LocalName;
                                    if (readOn)
                                    {
                                        if (isStmt && name == "StatementText")
                                            statementText = reader.Value;
                                        else if (isColumn && name == "Column")
                                            column = reader.Value;
                                        else if (isColumn && name == "ParameterCompiledValue")
                                            compiled = reader.Value;
                                        else if (isColumn && name == "ParameterRuntimeValue")
                                            runtime = reader.Value;
                                    }
                                    if (hit || IsExempt(name))
                                        continue;
                                    if (Judge(reader.Value))
                                    {
                                        if (!readOn)
                                            return XmlPassOutcome.Hit;
                                        hit = true;
                                    }
                                }
                                reader.MoveToElement();
                            }

                            StmtFrame? frame = null;
                            if (isStmt)
                            {
                                _stmtOrdinal++;
                                frame = new StmtFrame(_stmtOrdinal,
                                    statementText is not null && HasToken(statementText) ? statementText : null,
                                    statementText is not null);
                            }
                            else if (isColumn && column is not null)
                            {
                                var owner = InnermostStmt(open);
                                if (owner is not null && owner.TakesValues)
                                    owner.SetValue(column, compiled, runtime);
                            }

                            if (reader.IsEmptyElement)
                            {
                                if (frame is not null && JudgeProbe(frame))
                                    hit = true;
                            }
                            else
                            {
                                open.Push(new OpenElement(local, frame));
                                if (local == "ParameterList")
                                    parameterLists++;
                            }
                            break;
                        }
                        case XmlNodeType.EndElement:
                        {
                            if (open.Count == 0)
                                break;
                            var closed = open.Pop();
                            if (closed.Name == "ParameterList")
                                parameterLists--;
                            if (closed.Stmt is not null && JudgeProbe(closed.Stmt))
                                hit = true;
                            break;
                        }
                        case XmlNodeType.Text:
                        case XmlNodeType.CDATA:
                        case XmlNodeType.Comment:
                        case XmlNodeType.ProcessingInstruction:
                        {
                            // An element-form ParameterizedText is judged even after the first hit: pass 2 reads the
                            // statement's start tag before this text, so the statement is recorded by ordinal here.
                            bool paramText = open.Count > 0 && open.Peek().Name == "ParameterizedText";
                            if (hit && !paramText)
                                break;
                            if (!Judge(reader.Value))
                                break;
                            if (!readOn)
                                return XmlPassOutcome.Hit;
                            hit = true;
                            if (paramText)
                            {
                                var owner = InnermostStmt(open);
                                if (owner is not null)
                                    _probeNamed.Add(owner.Ordinal);
                            }
                            break;
                        }
                    }
                }

                // The end of the document or the cut: statements still open are judged with what was read.
                while (open.Count > 0)
                {
                    var closed = open.Pop();
                    if (closed.Stmt is not null && JudgeProbe(closed.Stmt))
                        hit = true;
                }
                return hit ? XmlPassOutcome.Hit : XmlPassOutcome.Clean;
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
            var info2 = (IXmlLineInfo)reader;
            _pastVetted = false;
            try
            {
                while (reader.Read())
                {
                    if (BudgetSpent())
                        return _placeholder;
                    if (!_pastVetted && _lineStarts is not null && NodeStart(info2, _lineStarts, reader.NodeType) >= _passOneStop)
                        _pastVetted = true;
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
                            writer.WriteComment(TextValue(reader.Value, "", scoped));
                            break;
                        case XmlNodeType.ProcessingInstruction:
                            writer.WriteProcessingInstruction(reader.Name, TextValue(reader.Value, "", scoped));
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
            // P13: a scope opened by the probe alone keeps its own StatementText (parameterized, no literal, the same
            // text the grids show), even when it holds a quote.
            bool keepStatementText = opens && stmtTextVerdict != true && paramTextVerdict != true;
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
                        value = AttributeValue(name, value, inScope, verdict, keepStatementText && name == "StatementText");
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
        private string AttributeValue(string name, string value, bool inScope, bool? knownVerdict, bool keep = false)
        {
            if (keep)
                return value;
            if (!inScope)
            {
                if (IsExempt(name))
                    return _pastVetted ? _placeholder : value;
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
