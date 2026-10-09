/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace Darling.Tests;

/// <summary>
/// Finds the test classes that start a clock, for the timing census of each test suite (#5602).
///
/// <para><b>Why.</b> xunit runs test classes in parallel, so a class that times its own work and fails when the
/// time passes a limit is judging the whole runner's load, not the code. #5561 and #5600 were that. The fix is a
/// single collection, <c>timing</c>, declared with <c>DisableParallelization</c>: xunit runs it alone, after the
/// parallel classes. The census (<c>TimingTestCensusTests</c> in each project) fails when a class starts a clock
/// outside that collection and is not on the allow list with its reason.</para>
///
/// <para><b>What counts as starting a clock.</b> A statement in a class body, after comments and string contents are
/// blanked, that calls <c>Stopwatch.StartNew</c>, <c>Stopwatch.GetTimestamp</c> or <c>Stopwatch.GetElapsedTime</c>,
/// constructs a <c>Stopwatch</c>, subtracts a <c>DateTime.UtcNow</c>/<c>Now</c> reading from a variable named like a start time (<c>started</c>, <c>start</c>, <c>began</c>; a heuristic, so "N minutes ago" arithmetic is not caught), or reads
/// <c>Environment.TickCount</c>. Source pins that quote those calls inside a string literal do not count. The rule is
/// wider than "asserts on the elapsed time": a class that only reports a time has to say so on the allow list, which
/// keeps one more place from growing an assertion unnoticed.</para>
///
/// <para>Linked into Lite.Tests (see <c>Lite.Tests.csproj</c> and the <c>lite_linked_shard</c> filter in build.yml).</para>
/// </summary>
internal static class TimingTestScanner
{
    /// <summary>The collection name every timing class carries.</summary>
    internal const string CollectionName = "timing";

    internal const string CollectionAttributeText = "[Collection(\"" + CollectionName + "\")]";

    private static readonly Regex s_clock = new(
        @"\bStopwatch\s*\.\s*(?:StartNew|GetTimestamp|GetElapsedTime)\s*\(|\bnew\s+(?:System\.Diagnostics\.)?Stopwatch\s*\(|\b(?:DateTime|DateTimeOffset)\s*\.\s*(?:UtcNow|Now)\s*-\s*(?:started|startedAt|start|startUtc|began|begun|t0)\b|\bEnvironment\s*\.\s*TickCount(?:64)?\b",
        RegexOptions.CultureInvariant);

    private static readonly Regex s_classHeader = new(
        @"^(?:(?:public|internal)\s+)?(?:(?:sealed|static|abstract|partial)\s+)*(?:class|record)\s+(?<name>\w+)",
        RegexOptions.CultureInvariant | RegexOptions.Multiline);

    /// <summary>One top-level class: its file, its name, whether it starts a clock, whether it is in <c>timing</c>.</summary>
    internal sealed record Row(string File, string Name, bool StartsClock, bool InTimingCollection);

    /// <summary>Every top-level class under <paramref name="testsRoot"/> (build output skipped), one row per declaration.</summary>
    internal static IReadOnlyList<Row> Scan(string testsRoot)
    {
        var rows = new List<Row>();
        foreach (var path in Directory.EnumerateFiles(testsRoot, "*.cs", SearchOption.AllDirectories).OrderBy(p => p, StringComparer.Ordinal))
        {
            var relative = Path.GetRelativePath(testsRoot, path).Replace('\\', '/');
            if (relative.StartsWith("bin/", StringComparison.Ordinal) || relative.StartsWith("obj/", StringComparison.Ordinal))
            {
                continue;
            }

            rows.AddRange(ScanText(relative, File.ReadAllText(path)));
        }

        return rows;
    }

    /// <summary>The rows of one file's text.</summary>
    internal static IReadOnlyList<Row> ScanText(string file, string text)
    {
        var withStrings = Blank(text, blankStrings: false);
        var code = Blank(text, blankStrings: true);
        var headers = s_classHeader.Matches(code);
        var rows = new List<Row>();
        for (var i = 0; i < headers.Count; i++)
        {
            var start = headers[i].Index;
            var end = i + 1 < headers.Count ? headers[i + 1].Index : code.Length;

            // The attribute lines directly above the declaration belong to it; the clock lines never sit there.
            var attrStart = start;
            while (attrStart > 0)
            {
                var lineEnd = attrStart - 1;
                var lineStart = withStrings.LastIndexOf('\n', Math.Max(lineEnd - 1, 0)) + 1;
                var line = withStrings.Substring(lineStart, lineEnd - lineStart).Trim();
                if (line.StartsWith('[') && line.EndsWith(']'))
                {
                    attrStart = lineStart;
                    continue;
                }

                break;
            }

            var attributes = withStrings.Substring(attrStart, start - attrStart);
            rows.Add(new Row(
                file,
                headers[i].Groups["name"].Value,
                s_clock.IsMatch(code.AsSpan(start, end - start)),
                attributes.Contains(CollectionAttributeText, StringComparison.Ordinal)));
        }

        return rows;
    }

    /// <summary>The problems with <paramref name="rows"/>: a clock outside <c>timing</c>, off the allow list.</summary>
    internal static List<string> Problems(IReadOnlyList<Row> rows, IReadOnlyDictionary<string, string> allowed)
    {
        var problems = new List<string>();
        foreach (var row in rows.Where(r => r.StartsClock && !r.InTimingCollection))
        {
            if (!allowed.ContainsKey(row.File))
            {
                problems.Add($"{row.File}: {row.Name} starts a clock outside {CollectionAttributeText} and is not on the allow list");
            }
        }

        return problems;
    }

    /// <summary>The allow-list entries that no longer name a file with a clock outside <c>timing</c>: the list only shrinks.</summary>
    internal static List<string> Stale(IReadOnlyList<Row> rows, IReadOnlyDictionary<string, string> allowed)
    {
        var live = rows.Where(r => r.StartsClock && !r.InTimingCollection).Select(r => r.File).ToHashSet(StringComparer.Ordinal);
        return allowed.Keys.Where(k => !live.Contains(k)).OrderBy(k => k, StringComparer.Ordinal).ToList();
    }

    /// <summary>
    /// <paramref name="text"/> with comments, and optionally the contents of string and character literals, replaced by
    /// spaces. Length and line breaks are kept, so an index into the result is an index into the source.
    /// </summary>
    internal static string Blank(string text, bool blankStrings)
    {
        var output = new StringBuilder(text.Length);
        var i = 0;
        while (i < text.Length)
        {
            var c = text[i];
            var next = i + 1 < text.Length ? text[i + 1] : '\0';

            if (c == '/' && next == '/')
            {
                while (i < text.Length && text[i] != '\n')
                {
                    output.Append(text[i] == '\r' ? '\r' : ' ');
                    i++;
                }

                continue;
            }

            if (c == '/' && next == '*')
            {
                output.Append("  ");
                i += 2;
                while (i < text.Length && !(text[i] == '*' && i + 1 < text.Length && text[i + 1] == '/'))
                {
                    output.Append(text[i] is '\n' or '\r' ? text[i] : ' ');
                    i++;
                }

                if (i < text.Length)
                {
                    output.Append("  ");
                    i += 2;
                }

                continue;
            }

            if (c == '"' && next == '"' && i + 2 < text.Length && text[i + 2] == '"')
            {
                // Raw string literal: three or more quotes to the same count.
                var quotes = 0;
                while (i + quotes < text.Length && text[i + quotes] == '"')
                {
                    quotes++;
                }

                var closer = new string('"', quotes);
                var close = text.IndexOf(closer, i + quotes, StringComparison.Ordinal);
                var stop = close < 0 ? text.Length : close + quotes;
                CopyString(text, i, stop, output, blankStrings, keep: quotes);
                i = stop;
                continue;
            }

            if (c == '"' || ((c == '@' || c == '$') && IsStringStart(text, i)))
            {
                var verbatim = false;
                var j = i;
                while (text[j] is '@' or '$')
                {
                    verbatim |= text[j] == '@';
                    j++;
                }

                // text[j] is the opening quote.
                var k = j + 1;
                while (k < text.Length)
                {
                    if (verbatim && text[k] == '"' && k + 1 < text.Length && text[k + 1] == '"')
                    {
                        k += 2;
                        continue;
                    }

                    if (!verbatim && text[k] == '\\')
                    {
                        k += 2;
                        continue;
                    }

                    if (text[k] == '"' || (!verbatim && text[k] == '\n'))
                    {
                        break;
                    }

                    k++;
                }

                var stop = Math.Min(k + 1, text.Length);
                CopyString(text, i, stop, output, blankStrings, keep: (j - i) + 1);
                i = stop;
                continue;
            }

            if (c == '\'')
            {
                var k = i + 1;
                while (k < text.Length && text[k] != '\'' && text[k] != '\n')
                {
                    k += text[k] == '\\' ? 2 : 1;
                }

                var stop = Math.Min(k + 1, text.Length);
                CopyString(text, i, stop, output, blankStrings, keep: 1);
                i = stop;
                continue;
            }

            output.Append(c);
            i++;
        }

        return output.ToString();
    }

    private static bool IsStringStart(string text, int index)
    {
        var j = index;
        while (j < text.Length && text[j] is '@' or '$')
        {
            j++;
        }

        return j < text.Length && text[j] == '"';
    }

    /// <summary>Appends text[from..to); with <paramref name="blank"/> the interior (past <paramref name="keep"/> opening characters and the closing one) becomes spaces.</summary>
    private static void CopyString(string text, int from, int to, StringBuilder output, bool blank, int keep)
    {
        for (var p = from; p < to; p++)
        {
            var ch = text[p];
            var isEdge = p - from < keep || p == to - 1;
            output.Append(!blank || isEdge || ch is '\n' or '\r' ? ch : ' ');
        }
    }
}
