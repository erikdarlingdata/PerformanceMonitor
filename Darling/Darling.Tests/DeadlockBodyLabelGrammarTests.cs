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
using System.Text.RegularExpressions;
using PerformanceMonitor.Alerting;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Every fact name in a rendered deadlock alert body is a LABEL to the web alert-history detail cell, which
/// re-parses the flattened body rather than receiving its structure.
///
/// <para><b>Why this needs a pin rather than a convention.</b> <c>parseDetailFields</c> in
/// <c>wwwroot/js/pages/alerts.js</c> matches a label with a pattern of letters and spaces — no digits — and
/// a line it does not match is not dropped: it is APPENDED to the preceding field's value. So a fact named
/// <c>Process 1</c> does not render as an unlabelled row a reader would notice; it disappears into
/// <c>Victim SQL</c>'s value as a run-on, and every following party chains onto the same one. #3442 was
/// written with numbered party labels and this is the check that changed them to letters.</para>
///
/// <para><b>The pattern is read out of the shipped script, not restated here.</b> A copy would be free to
/// drift from the parser it describes, and the drift direction that matters is the parser being TIGHTENED —
/// which a restated pattern cannot see. Failing to find the pattern at all is a failure rather than a skip,
/// because a pin reading nothing is indistinguishable from a passing one.</para>
///
/// <para>Scoped to the deadlock body, which is the body #3442 added fact names to. The other builders'
/// labels satisfy the same grammar today; asserting that for all of them needs the closed builder census
/// <c>Lite.Tests.AlertBodyClockFrameDisciplineTests</c> maintains, and a second copy of that census is the
/// thing this file's own argument says not to make.</para>
/// </summary>
public class DeadlockBodyLabelGrammarTests
{
    private const string Server = "SQL2022";

    /* Seven distinct parties, so the body renders the party cap, the omission fact, and an occurrence
       count — the widest label set a deadlock body produces. Every name invented. */
    private static string Graph(int parties)
    {
        var processes = string.Concat(Enumerable.Range(1, parties).Select(n =>
            $@"
    <process id=""process{n}"" spid=""{300 + n}"" currentdbname=""AppDb"" isolationlevel=""level {n}"" lockMode=""M{n}"">
      <inputbuf>UPDATE AppDb.dbo.Ledger SET Balance = {n}</inputbuf>
    </process>"));

        return $@"<deadlock>
  <victim-list><victimProcess id=""process1""/></victim-list>
  <process-list>{processes}
  </process-list>
  <resource-list>
    <keylock objectname=""AppDb.dbo.Ledger""><owner id=""process2"" mode=""X""/><waiter id=""process1"" mode=""U""/></keylock>
  </resource-list>
</deadlock>";
    }

    private static Regex WebLabelPattern()
    {
        var script = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "alerts.js");

        var literal = Regex.Match(script, @"const m = /(?<pattern>\^\(.*?\)\$)/\.exec\(line\);");
        Assert.True(literal.Success,
            "parseDetailFields' label pattern was not found in alerts.js — this pin would be reading nothing");

        return new Regex(literal.Groups["pattern"].Value);
    }

    private static List<(string Label, string Value)> DeadlockBodyFields(int parties, int occurrences)
    {
        var graph = Graph(parties);
        var rows = Enumerable.Range(0, occurrences).Select(_ => new DeadlockAlertRow
        {
            VictimProcessId = "process1",
            VictimSqlText = "UPDATE AppDb.dbo.Ledger SET Balance = 0 WHERE LedgerId = 9",
            DeadlockGraphXml = graph
        }).ToList();

        var context = AlertContextBuilders.BuildDeadlockContext(Server, rows, new List<string>());
        Assert.NotNull(context);

        return context!.Details.SelectMany(d => d.Fields).Select(f => (f.Item1, f.Item2)).ToList();
    }

    [Fact]
    public void EveryDeadlockBodyFactName_IsALabelToTheWebAlertHistoryParser()
    {
        var pattern = WebLabelPattern();
        var fields = DeadlockBodyFields(parties: 7, occurrences: 3);

        /* The floor is the whole label set, enumerated: a body that rendered nothing, or one that stopped
           short of the party cap and its omission fact, would satisfy the loop below while checking none of
           the names this pin exists for. A count alone would not say WHICH names it had reached. */
        Assert.Equal(
            new List<string>
            {
                "Database", "Victim SQL", "Processes",
                "Process A", "Process B", "Process C", "Process D",
                DeadlockGraphSummary.OmittedFactName,
                "Dedup Key", "Involved Objects", "Occurrences"
            },
            fields.Select(f => f.Label).ToList());

        foreach (var (label, value) in fields)
        {
            /* The parser sees the flattened line, so the check is on the line it would read. */
            var match = pattern.Match($"{label}: {value}");
            Assert.True(match.Success, $"'{label}' is not a label to parseDetailFields — its line folds into the previous field");
            Assert.Equal(label, match.Groups[1].Value);
        }
    }

    [Fact]
    public void ThePatternRead_RejectsADigitBearingLabel_SoTheCheckAboveDiscriminates()
    {
        /* The pin above passes if the pattern read from the script matched everything. This is the
           counter-example that says it does not: the numbered form #3442 started with. */
        var pattern = WebLabelPattern();

        Assert.DoesNotMatch(pattern, "Process 1: SPID 325");
        Assert.Matches(pattern, "Process A: SPID 325");
    }
}
