using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5457: a tab's data call must not run on the UI thread. <c>LocalDataService.OpenConnectionAsync</c> takes the
/// process-wide store read lock with an untimed <c>EnterReadLock()</c> before its first await, and DuckDB.NET's
/// async methods complete synchronously, so a call awaited straight from a control's UI thread waits on that lock
/// there (a writer parked on it, archival or compaction, also holds back every new reader) and then runs the whole
/// query there. The window stops pumping WPF input while Windows still sees a message loop, which is how a user
/// saw a tab hang without "Not Responding". <c>ReaderWriterLockSlim</c> is thread-affine, so the call also has to
/// enter and leave the lock on one pool thread: <c>Task.Run(() =&gt; _dataService.XAsync())</c> does, because the
/// lambda's thread runs the read to its end. Job History, Alerts and every ServerTab refresh already read this way;
/// this census holds the rest of <c>Lite/Controls</c> to it.
/// </summary>
public class TabLoadsOffUiThreadTests
{
    /* Calls that sit outside a Task.Run lexically because the METHOD holding them is only ever called from inside
       one. Each entry is (file, data-service method, helper that holds the call). The census also checks that every
       reference to the helper is itself inside a Task.Run. */
    private static readonly (string File, string Call, string Helper)[] HelpersOnlyCalledOffTheUiThread =
    {
        ("Lite/Controls/AlertsHistoryTab.xaml.cs", "GetServerClockAsync", "ReadCollectedClocksAsync"),
        ("Lite/Controls/ServerTab.Refresh.cs", "GetBaselineDiscontinuitiesAsync", "SafeDiscontinuitiesAsync"),
    };

    private static readonly Regex DataServiceCall = new(
        @"\b_?dataService\s*\.\s*(?<method>[A-Za-z]+Async)\s*\(", RegexOptions.Compiled);

    [Fact]
    public void EveryTabDataCallRunsInsideTaskRun()
    {
        var root = RepoRoot();
        var violations = new List<string>();
        var seen = 0;

        foreach (var path in Directory.EnumerateFiles(Path.Combine(root, "Lite", "Controls"), "*.cs", SearchOption.AllDirectories)
                     .Where(p => !IsBuildOutput(p))
                     .OrderBy(p => p, StringComparer.Ordinal))
        {
            var relative = Path.GetRelativePath(root, path).Replace('\\', '/');
            var code = Strip(File.ReadAllText(path));
            foreach (var violation in FindCallsOutsideTaskRun(code))
            {
                seen++;
                if (HelpersOnlyCalledOffTheUiThread.Any(h => h.File == relative && h.Call == violation.Method))
                {
                    continue;
                }

                violations.Add($"{relative}:{LineOf(code, violation.Index)} {violation.Method}");
            }
        }

        Assert.True(violations.Count == 0,
            "These Lite/Controls data calls are awaited on the UI thread, where they wait on the store read lock (and "
            + "run the whole synchronous DuckDB query) on the dispatcher (#5457). Wrap each in Task.Run(() => ...), "
            + "reading any control value first on the UI thread:\n" + string.Join("\n", violations));

        /* Not vacuous: the allowlisted helpers' own calls were the ones seen. */
        Assert.Equal(HelpersOnlyCalledOffTheUiThread.Length, seen);
    }

    [Fact]
    public void TheHelpersTheCensusAllowsAreOnlyCalledInsideTaskRun()
    {
        var root = RepoRoot();
        foreach (var (file, call, helper) in HelpersOnlyCalledOffTheUiThread)
        {
            var code = Strip(File.ReadAllText(Path.Combine(root, file))
                .Replace("\r\n", "\n"));
            Assert.Contains(call + "(", code);

            /* A reference that follows the return type's closing '>' is the declaration; '=>' is a lambda body. */
            var reference = new Regex(@"\b" + helper + @"\s*\(");
            var callSites = reference.Matches(code).Cast<Match>()
                .Where(m => !Regex.IsMatch(code[..m.Index], @"(?<!=)>\s*$"))
                .ToList();
            Assert.NotEmpty(callSites);
            foreach (var site in callSites)
            {
                Assert.True(IsInsideTaskRun(code, site.Index),
                    $"{file}:{LineOf(code, site.Index)} {helper}() is outside Task.Run, and it holds {call}, which waits on the store lock.");
            }
        }
    }

    [Fact]
    public void TheCensusSeesALockWaitOnTheUiThreadAndNotOneInsideTaskRun()
    {
        var onUiThread = Strip("private async Task Load()\n{\n    var rows = await _dataService.GetFooAsync(1, \"(\");\n}");
        Assert.Single(FindCallsOutsideTaskRun(onUiThread));

        var inLambda = Strip(
            "var rows = await Task.Run(async () =>\n{\n    // a comment with ( and )\n    var a = await dataService.GetFooAsync(1);\n"
            + "    return (a, await dataService.GetBarAsync(2));\n});");
        Assert.Empty(FindCallsOutsideTaskRun(inLambda));

        var expressionBody = Strip("var t = System.Threading.Tasks.Task.Run(() => _dataService.GetFooAsync(1));");
        Assert.Empty(FindCallsOutsideTaskRun(expressionBody));

        /* The paren that closes a Task.Run must not cover a call written after it. */
        var after = Strip("var a = await Task.Run(() => Other());\nvar b = await _dataService.GetFooAsync(1);");
        Assert.Single(FindCallsOutsideTaskRun(after));
    }

    private static IEnumerable<(string Method, int Index)> FindCallsOutsideTaskRun(string strippedCode)
    {
        foreach (Match m in DataServiceCall.Matches(strippedCode))
        {
            if (!IsInsideTaskRun(strippedCode, m.Index))
            {
                yield return (m.Groups["method"].Value, m.Index);
            }
        }
    }

    /// <summary>True when an unclosed '(' before <paramref name="index"/> opens a Task.Run / StartNew argument list.</summary>
    private static bool IsInsideTaskRun(string code, int index)
    {
        var depth = 0;
        for (var i = index - 1; i >= 0; i--)
        {
            var c = code[i];
            if (c == ')')
            {
                depth++;
            }
            else if (c == '(')
            {
                if (depth > 0)
                {
                    depth--;
                    continue;
                }

                var before = code[Math.Max(0, i - 40)..i];
                if (Regex.IsMatch(before, @"Task\s*\.\s*Run\s*$") ||
                    Regex.IsMatch(before, @"Task\s*\.\s*Factory\s*\.\s*StartNew\s*$"))
                {
                    return true;
                }
            }
        }

        return false;
    }

    /// <summary>The source with comments and string and char literals blanked, so a parenthesis in one cannot unbalance the scan.</summary>
    private static string Strip(string source)
    {
        var sb = new StringBuilder(source.Length);
        var i = 0;
        while (i < source.Length)
        {
            var c = source[i];
            var next = i + 1 < source.Length ? source[i + 1] : '\0';
            if (c == '/' && next == '/')
            {
                while (i < source.Length && source[i] != '\n') { sb.Append(' '); i++; }
            }
            else if (c == '/' && next == '*')
            {
                while (i < source.Length && !(source[i] == '*' && i + 1 < source.Length && source[i + 1] == '/'))
                {
                    sb.Append(source[i] == '\n' ? '\n' : ' ');
                    i++;
                }

                if (i < source.Length) { sb.Append("  "); i += 2; }
            }
            else if (c == '"' || ((c == '@' || c == '$') && (next == '"' || (next is '@' or '$' && i + 2 < source.Length && source[i + 2] == '"'))))
            {
                var verbatim = false;
                while (i < source.Length && source[i] != '"') { verbatim |= source[i] == '@'; sb.Append(' '); i++; }
                sb.Append(' '); i++; /* opening quote */
                while (i < source.Length)
                {
                    if (source[i] == '\\' && !verbatim) { sb.Append("  "); i += 2; continue; }
                    if (source[i] == '"')
                    {
                        if (verbatim && i + 1 < source.Length && source[i + 1] == '"') { sb.Append("  "); i += 2; continue; }
                        break;
                    }

                    sb.Append(source[i] == '\n' ? '\n' : ' ');
                    i++;
                }

                sb.Append(' '); i++; /* closing quote */
            }
            else if (c == '\'' && next != '\0')
            {
                /* A char literal ('(' or '\'') and nothing else uses a quote here. */
                var end = next == '\\' ? i + 3 : i + 2;
                if (end < source.Length && source[end] == '\'')
                {
                    sb.Append(' ', end - i + 1);
                    i = end + 1;
                }
                else
                {
                    sb.Append(c); i++;
                }
            }
            else
            {
                sb.Append(c); i++;
            }
        }

        return sb.ToString();
    }

    private static int LineOf(string code, int index) => code.AsSpan(0, index).Count('\n') + 1;

    private static bool IsBuildOutput(string path) =>
        path.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar).Any(s => s is "bin" or "obj");

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        for (var dir = new DirectoryInfo(Path.GetDirectoryName(thisFile)!); dir is not null; dir = dir.Parent)
        {
            if (Directory.Exists(Path.Combine(dir.FullName, "PerformanceMonitor.Collectors")))
            {
                return dir.FullName;
            }
        }

        throw new DirectoryNotFoundException($"could not locate the repo root walking up from {thisFile}");
    }
}
