/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Every read in the alert evaluation pass must carry an EXPLICIT command deadline (#2874).
///
/// <para>All forty-five commands across the six alert-pass types ran with no
/// <c>CommandTimeout</c>, so every one inherited Npgsql's undocumented 30 s default. On 2026-09-04
/// the forced-plan read failed five times on the production store, each surfacing as "Exception
/// while reading from stream" — how Npgsql renders its own deadline, and the exact misdiagnosis
/// #2826 exists to prevent.</para>
///
/// <para><b>Why this pin matches TWO construction shapes.</b> #2874's census counted 133 untimed
/// sites by scanning for <c>new NpgsqlCommand(</c>. That shape is not the only one: an
/// <c>NpgsqlDataSource</c> also hands out commands via <c>CreateCommand(sql)</c>, which inherits the
/// same default and which the census therefore missed entirely — four of this group's own sites are
/// that shape, in <c>DarlingPostgresAlertReadAdapter</c>. A pin keyed on one spelling would have
/// declared this family clean while a fifth of one file stayed on the inherited default, which is
/// the #2786 failure exactly: a guard that names the arm it was written for. Both shapes are matched
/// here, and the repo-wide recount is recorded on #2874.</para>
///
/// <para>The VALUE is pinned separately below, and is bounded on both sides for reasons that do not
/// transfer from the two closed passes — see <c>DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds</c>.
/// The short version: this pass has no enclosing <c>CancelAfter</c>, so the per-command deadline is
/// the pass budget times the number of sequential reads, and it is deliberately set BELOW what it
/// inherited rather than above.</para>
/// </summary>
public sealed class AlertPassCommandTimeoutTests
{
    /// <summary>
    /// The six types that make up one alert evaluation pass. Named explicitly rather than globbed,
    /// because "runs inside <c>EvaluateAlertsAsync</c>" is a budget boundary that no filename pattern
    /// expresses — a future file in this directory may belong to a different budget. That is not
    /// hypothetical: <c>PgPlanForceActionStore</c> looks like a member of this family and is not one.
    /// Its only caller is <c>PlanForceBot.RunAfterAnalysisAsync</c>, dispatched as the analysis pass's
    /// post-pass hook over the plain stopping token, so it shares the unbudgeted shape but runs on the
    /// analysis interval rather than this pass's 30 s cadence — a different upper bound, and therefore
    /// a different group.
    /// </summary>
    private static readonly string[] s_alertPassSources =
    {
        "DarlingAlertReadAdapter.cs",
        "DarlingPostgresAlertReadAdapter.cs",
        "PgAlertStateStore.cs",
        "PgMuteRuleStore.cs",
        "PgAlertHistoryStore.cs",
        "DarlingSelfAlertEvaluator.cs",
    };

    /// <summary>
    /// Both ways a command is constructed in this codebase. <c>new NpgsqlCommand(</c> is the shape
    /// #2810's pin matched; <c>.CreateCommand(</c> is the one it did not, and which hid four sites
    /// from #2874's census.
    /// </summary>
    private static readonly Regex s_commandCtor = new(
        @"new NpgsqlCommand\s*\(|\.CreateCommand\s*\(",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex s_setsTimeout = new(
        @"CommandTimeout\s*=",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    [Fact]
    public void EveryAlertPassCommand_SetsAnExplicitDeadline()
    {
        var offenders = new List<string>();
        var total = 0;

        foreach (var path in AlertPassSources())
        {
            var text = File.ReadAllText(path);

            foreach (Match ctor in s_commandCtor.Matches(text))
            {
                total++;

                /* Scan to the END OF THE STATEMENT, not a fixed number of lines.

                   A line window cannot work here and the first draft of this pin proved it: these
                   sites embed verbatim SQL, so the construction routinely spans twenty-plus lines and
                   the initializer that carries the deadline sits past any window small enough not to
                   run into the following member. A window wide enough to catch it would instead read
                   the NEXT command's deadline and call an untimed site clean — the failure that
                   actually matters, because it reports success on the defect.

                   The statement span is exact and needs no tuning: the deadline is either an object
                   initializer on the construction, or — for the CreateCommand shape, whose method
                   result cannot take one — the statement immediately after it, so two statements are
                   examined. */
                var span = StatementSpanFrom(text, ctor.Index, statements: 2);

                if (!s_setsTimeout.IsMatch(span))
                {
                    var line = text.Take(ctor.Index).Count(c => c == '\n') + 1;
                    offenders.Add($"{Path.GetFileName(path)}:{line}");
                }
            }
        }

        Assert.True(total > 0, "the alert-pass scan matched no command constructions at all");

        Assert.True(
            offenders.Count == 0,
            $"{offenders.Count} alert-pass command(s) inherit Npgsql's 30s default instead of setting "
            + $"{nameof(DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds)}: "
            + string.Join(", ", offenders));
    }

    /// <summary>
    /// The value, bounded on both sides.
    ///
    /// <para>ABOVE the measured worst case: the shipped queries timed against the production store's
    /// three busiest servers put the whole pass's cost in one read — the forced-plan check at
    /// 1,744.9 ms cold over ~6.0 GB, with every other read under 3 ms. A floor of 5 s keeps real
    /// headroom over that.</para>
    ///
    /// <para>BELOW the 30 s <c>s_alertSweepInterval</c> this pass runs on, so one stalled read still
    /// leaves the pass able to finish inside the interval that restarts it — and so the value stays
    /// under the default it replaces, which is the whole point of this change.</para>
    /// </summary>
    [Fact]
    public void TheAlertPassDeadline_StaysInsideItsJustifiedBand()
    {
        var seconds = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds;

        Assert.True(
            seconds >= 5,
            $"alert-pass deadline {seconds}s is at or under the measured 1.7s worst case with no "
            + "meaningful headroom — a stall a little worse than normal would fail the read");

        Assert.True(
            seconds < 30,
            $"alert-pass deadline {seconds}s is at or above Npgsql's inherited 30s default, so it "
            + "buys nothing: this pass has no enclosing CancelAfter, and the per-command value is "
            + "the pass budget times the number of sequential reads");
    }

    /// <summary>
    /// The text from <paramref name="start"/> through the end of the Nth following statement,
    /// counting only semicolons that sit outside string literals and outside any nesting. Verbatim
    /// SQL in this family contains both semicolons and quote characters, so a naive scan for ';'
    /// stops in the middle of a query and reports every multi-line construction as untimed.
    /// </summary>
    private static string StatementSpanFrom(string text, int start, int statements)
    {
        var depth = 0;
        var seen = 0;
        var i = start;

        while (i < text.Length)
        {
            var c = text[i];

            if (c == '@' && i + 1 < text.Length && text[i + 1] == '"')
            {
                i = SkipVerbatimString(text, i + 2);
                continue;
            }

            if (c == '"')
            {
                i = SkipRegularString(text, i + 1);
                continue;
            }

            if (c is '(' or '[' or '{')
            {
                depth++;
            }
            else if (c is ')' or ']' or '}')
            {
                depth--;
            }
            else if (c == ';' && depth <= 0 && ++seen >= statements)
            {
                return text[start..(i + 1)];
            }

            i++;
        }

        return text[start..];
    }

    private static int SkipVerbatimString(string text, int i)
    {
        while (i < text.Length)
        {
            if (text[i] == '"')
            {
                /* "" is an escaped quote inside a verbatim string, not the end of it. */
                if (i + 1 < text.Length && text[i + 1] == '"')
                {
                    i += 2;
                    continue;
                }

                return i + 1;
            }

            i++;
        }

        return i;
    }

    private static int SkipRegularString(string text, int i)
    {
        while (i < text.Length)
        {
            if (text[i] == '\\')
            {
                i += 2;
                continue;
            }

            if (text[i] == '"')
            {
                return i + 1;
            }

            i++;
        }

        return i;
    }

    private static IEnumerable<string> AlertPassSources()
    {
        var dir = Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service");

        var paths = s_alertPassSources
            .Select(f => Path.Combine(dir, f))
            .ToArray();

        /* A renamed or moved alert-pass type must fail loudly here rather than silently shrinking the
           scan to the files that still resolve — an empty or partial sweep is how a guard starts
           reporting clean on code it no longer reads. */
        foreach (var path in paths)
        {
            Assert.True(File.Exists(path), $"alert-pass source not found: {path}");
        }

        return paths;
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null
               && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln"))
               && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
