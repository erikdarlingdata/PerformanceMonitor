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
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4283: three OLDER paths (a tool's own caught exception through <c>ToHttpResult</c>, <c>/api/compose/run</c>'s
/// 500, the triage page's notes) still put <c>ex.Message</c> on the wire, on top of the six write endpoints and
/// two fleet-sweep reads the round-1 #4281 review's grep did not reach. This class is three things: a source
/// census that the web host never builds an HTTP answer from a caught exception's own text (an allow-list of
/// the two sites where the text is either safely contained or genuinely needed), unit tests for the new
/// sentence-based half of <see cref="DarlingWebFailureLog"/> and <see cref="DarlingWebEndpoints.ToHttpResult"/>,
/// and one live test proving a REAL tool-caught statement_timeout answers 503 with no exception text. The live
/// test opens DARLING_TEST_PG directly (no ScratchPostgres, no own cluster), so the class carries
/// <c>[Collection("live-postgres")]</c> alongside every other class that reaches the shared store
/// (<see cref="LivePostgresCollectionHygieneTests"/>'s own census).
/// </summary>
[Collection("live-postgres")]
public sealed class WebExceptionTextCensusTests
{
    /* ═══════════════════════════ the census ═══════════════════════════ */

    /// <summary>Every file that answers an HTTP request on the web/triage/fleet-sweep surface — the roster
    /// <c>#4283</c>'s own issue names (<c>DarlingWebEndpoints.cs</c>, the triage page) plus the one more this
    /// lane's own grep found (<c>DarlingFleetSweepEndpoints.cs</c>) and the OIDC exchange the issue asked to
    /// be traced. <c>Mcp/DarlingWebHostService.cs</c> is traced too (#4283) but NOT in the roster: every
    /// <c>ex.Message</c> there is a <c>logger.LogError</c>/<c>LogWarning</c> argument in the host's own
    /// startup/shutdown/OIDC-config lifecycle (service diagnostics), or the sign-in flow's own
    /// <c>DarlingHttpRefusalLog</c> line — never a byte written to an HTTP response; the ONE place it DOES
    /// answer a request, the #4281 backstop, already passes the <see cref="Exception"/> itself through
    /// <see cref="DarlingWebFailureLog.Report(ILogger,string,long,Exception)"/>, never its bare
    /// <c>.Message</c>. A file NOT in this list is not swept by the census — the same tradeoff
    /// <c>AlertReadFailureSurfaceTests</c>' fixed roster makes, over scanning every collector file that
    /// legitimately logs <c>ex.Message</c> with nothing to do with a browser.</summary>
    private static readonly string[] s_roster =
    {
        Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingWebEndpoints.cs"),
        Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingTriageEndpoint.cs"),
        Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingFleetSweepEndpoints.cs"),
        Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "Hosting", "DarlingWebOidc.cs"),
        // #4283 review round 1 (L2): widened alongside the pattern below, since H1/H2 touched both.
        Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingServerResolver.cs"),
        Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "Hosting", "DarlingWebFailureLog.cs"),
    };

    /// <summary>Each surviving <c>ex.Message</c> / <c>ex.MessageText</c> CODE occurrence (comments stripped),
    /// matched by a distinguishing snippet, with the reason it is not #4283's target.</summary>
    private static readonly (string File, string Snippet, string Reason)[] s_allowList =
    {
        (
            Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingWebEndpoints.cs"),
            "Query failed: {ex.MessageText}",
            "The 400 arm of /api/compose/run's PostgresException catch — a Custom Views author needs the real " +
            "syntax/statement_timeout error to fix their own panel; #4283's issue names this one to KEEP."
        ),
        (
            Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingWebEndpoints.cs"),
            "Error running query: {ex.Message}",
            "ComposeRunOutcome.ServerError's text, built once and shared by two consumers: the web route " +
            "reclassifies it through DarlingWebFailureLog before it ever reaches a browser (ServerErrorResult), " +
            "and the MCP run_custom_view_panel tool (DarlingMcpCustomViewTools) reads the same field unchanged " +
            "— #4283 does not touch what an MCP client sees."
        ),
        (
            Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "Hosting", "DarlingWebOidc.cs"),
            "return new ExchangeResult(null, ex.Message);",
            "Traced (#4283): ExchangeResult.Error's only reader is DarlingWebHostService's sign-in callback, " +
            "which routes it ONLY to ReportSignInRefusal (a log line) and answers the browser a fixed sentence " +
            "('The identity provider rejected the sign-in exchange...') regardless. Never reaches a browser."
        ),
        (
            Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingWebEndpoints.cs"),
            "conflict.Message",
            "CustomViewResult.Conflict / CustomAlertRuleResult.Conflict's own business-outcome text (409 body) " +
            "— the store-write layer's own sentence, never ex.Message."
        ),
        (
            Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingWebEndpoints.cs"),
            "invalid.Message",
            "CustomViewResult.Invalid / CustomAlertRuleResult.Invalid's own validation text (400 body) " +
            "— the store-write layer's own sentence, never ex.Message."
        ),
        (
            Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingWebEndpoints.cs"),
            "snapshot.Detail",
            "CollectorRuntimeState's own startup-step detail string (the /ping surface) — today it is the raw " +
            "ex.Message of a startup failure, verbatim (round-1 H4, tracked in #4316, fixed by PR #4326); " +
            "remove this entry once #4326 lands."
        ),
        (
            Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "Hosting", "DarlingWebFailureLog.cs"),
            "exception.InnerException",
            "IsStatementTimeout's own type-pattern check (exception.InnerException is TimeoutException) — never " +
            "reads .Message off it, nothing reaches the wire."
        ),
        (
            Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingServerResolver.cs"),
            "RegistryReadFaultPrefix}{ex.Message}",
            "LoadEnabledOrFaultAsync's fault sentence, built once and shared by two consumers, same shape as " +
            "ComposeRunOutcome.ServerError's text above: MCP reads it back unchanged (RegistryReadFaultPrefix " +
            "is pinned byte-identical to the pre-#4283 literal), and the web surface reclassifies it through " +
            "ToHttpResult/DarlingWebFailureLog before it ever reaches a browser (see " +
            "ToHttpResult_ResolverRegistryFault_MapsTo500_NotClientError_OneErrorLog below)."
        ),
    };

    /// <summary>Strips <c>/* ... */</c> and <c>// ...</c> so the census reads CODE, not this class's own doc
    /// comments quoting the pattern under discussion (every file above has several, post-#4283).</summary>
    private static string StripComments(string source)
    {
        var noBlock = Regex.Replace(source, @"/\*.*?\*/", "", RegexOptions.Singleline);
        return Regex.Replace(noBlock, @"//[^\n]*", "");
    }

    /// <summary>#4283 review round 1 (L2): widened past a fixed <c>ex</c>/<c>exception</c> identifier — a
    /// leading <c>.Message</c>/<c>.MessageText</c>/<c>.InnerException</c>/<c>.Detail</c>/<c>.Hint</c> is
    /// distinctive enough on its own to catch a differently-named caught exception, at the cost of the new
    /// allow-list entries above for the property names that collide (<c>conflict.Message</c> etc.).
    /// <c>.Where</c> needs the negative lookahead so plain LINQ <c>.Where(</c> calls do not flood the census.
    /// <c>.ToString()</c> is its OWN alternation, gated on an exception-shaped identifier immediately before
    /// the call (the review handoff's own literal regex folded it into the first alternation, unrestricted —
    /// that matched every unrelated <c>.ToString()</c> in the roster, e.g.
    /// <c>value.ToString()</c>/<c>m.Archetype.ToString()</c>/<c>builder.ToString()</c>; caught by
    /// <see cref="ExMessagePattern_MatchesEveryKnownExceptionTextShape_AndNothingElse"/>'s own
    /// <c>count.ToString()</c> negative case, which the handoff's own test table named but its regex did not
    /// actually satisfy). The third alternation catches a bare <c>{ex}</c>/<c>{pgEx}</c>/<c>{exception}</c>/
    /// <c>{e}</c> inside a <c>$"..."</c> interpolation — gated on the same ex-shaped fragment so it does not
    /// also match a structured-logging <c>{Name}</c> template placeholder (capitalized, never ex-shaped) or a
    /// route template segment like <c>{id}</c>.
    /// #4293 round 2 (R2-L4) widens four more ways. (1) A <c>?</c>, <c>!</c> or a closing <c>)</c> may now sit
    /// before the dot on the first alternation's generic receiver: <c>ex?.Message</c>, <c>(ex as
    /// PostgresException)?.MessageText</c>, <c>ex.GetBaseException().Message</c>. (2) A SECOND property list
    /// (<c>StackTrace</c>/<c>InternalQuery</c>/<c>TableName</c>/<c>SchemaName</c>/<c>ColumnName</c>/
    /// <c>ConstraintName</c>/<c>Routine</c>) gated on an exception-shaped receiver ONLY — <c>ex</c>/
    /// <c>exception</c>/<c>e</c>/<c>pgEx</c>/<c>fault</c>/<c>Fault</c>, bare or at the end of a dotted path like
    /// <c>outcome.Fault</c> — unlike the first list, <c>TableName</c>/<c>ColumnName</c> are common, harmless
    /// names on a non-exception receiver elsewhere in the codebase (see
    /// <c>DarlingObjectStatsReader</c>/<c>DarlingMcpPgIndexTools</c> etc., neither on the roster, but the same
    /// shape could land there), so this list cannot reuse the generic-receiver alternation. (3)
    /// <see cref="ExOrFaultName"/> (the <c>.ToString()</c> and <c>{...}</c> arms' shared name fragment) now also
    /// accepts <c>Fault</c>/<c>fault</c>, bare or dotted (<c>{outcome.Fault}</c>). (4) A fourth alternation
    /// catches a string literal concatenated onto an exception-shaped name — <c>"Query failed: " + ex</c> —
    /// gated by a negative lookahead so it defers to the other alternations when the name is ITSELF further
    /// accessed (<c>.</c>/<c>(</c>/<c>[</c> follows immediately).</summary>
    private const string ExOrFaultName = @"(?:[A-Za-z_]*[Ee]x(?:ception)?|e|(?:[A-Za-z_]\w*\.)*[Ff]ault)";

    private static readonly Regex s_exMessagePattern = new(
        @"(?:\b[A-Za-z_]\w*|\))[?!]?\.(Message\b|MessageText\b|InnerException\b|Detail\b|Hint\b|Where\b(?!\s*\())"
        + @"|\b(?:[A-Za-z_]\w*\.)*(?:ex|exception|e|pgEx|fault|Fault)\b[?!]?\.(StackTrace\b|InternalQuery\b|TableName\b|SchemaName\b|ColumnName\b|ConstraintName\b|Routine\b)"
        + @"|\b" + ExOrFaultName + @"\.ToString\(\)"
        + @"|\{" + ExOrFaultName + @"\}"
        + @"|""(?:[^""\\]|\\.)*""\s*\+\s*" + ExOrFaultName + @"\b(?![.(\[])",
        RegexOptions.Compiled);

    /// <summary>#4283 review round 1 (L2): the widened pattern itself, independent of what any roster file
    /// contains — a reintroduced <c>pgEx.MessageText</c> or <c>$"{e}"</c> must always be caught, and the LINQ
    /// <c>.Where(</c>/non-exception <c>.ToString()</c>/structured-logging-template cases must never be.
    /// #4293 round 2 (R2-L4) adds the ?/!/) receiver tail, the exception-shaped-receiver-only new property
    /// names (plus a non-exception-receiver negative for the same two names, since that gating is the whole
    /// point), Fault/fault (bare and dotted), and the string-literal concatenation arm.</summary>
    [Fact]
    public void ExMessagePattern_MatchesEveryKnownExceptionTextShape_AndNothingElse()
    {
        foreach (var positive in new[]
        {
            "pgEx.MessageText", "e.Message", "ex.InnerException", "ex.ToString()", "$\"{e}\"", "ex.Detail",
            "ex.Hint", "ex.Where",
            // #4293 round 2 (R2-L4):
            "ex?.Message", "(ex as PostgresException)?.MessageText", "ex.GetBaseException().Message",
            "ex.StackTrace", "pgEx.InternalQuery", "outcome.Fault.TableName", "outcome.Fault.ToString()",
            "$\"{outcome.Fault}\"", "\"Query failed: \" + ex",
        })
        {
            Assert.True(s_exMessagePattern.IsMatch(positive), $"expected a match in: {positive}");
        }

        foreach (var negative in new[]
        {
            "servers.Where(s => s.Enabled)", "count.ToString()", "$\"{route}\"",
            "logger.LogError(\"{Route} failed\", route)",
            // #4293 round 2 (R2-L4): TableName/ColumnName are common, harmless names on a NON-exception
            // receiver — the whole reason the new property list is gated on an exception-shaped one.
            "widget.TableName", "row.ColumnName",
        })
        {
            Assert.False(s_exMessagePattern.IsMatch(negative), $"expected no match in: {negative}");
        }
    }

    /// <summary>Revert-proof for the widening itself (#4283 L2): the OLD pattern
    /// (<c>\b(ex|exception)\.Message(Text)?\b</c>) would have missed a reintroduced <c>pgEx.MessageText</c>
    /// (different identifier) and a bare <c>$"{e}"</c> interpolation (no property access at all) — both real
    /// shapes the round-1 review found live in the roster once H1/H2/M1 landed. Proven by running the two
    /// cases the new pattern catches against the retired pattern here, once, rather than by hand-reverting
    /// <see cref="s_exMessagePattern"/> and re-running the suite.</summary>
    [Fact]
    public void ExMessagePattern_OldNarrowerPattern_WouldHaveMissedTheWidenedCases()
    {
        var old = new Regex(@"\b(ex|exception)\.Message(Text)?\b", RegexOptions.Compiled);

        Assert.False(old.IsMatch("pgEx.MessageText"), "the pre-L2 pattern was expected to miss a differently-named exception identifier");
        Assert.False(old.IsMatch("$\"{e}\""), "the pre-L2 pattern was expected to miss a bare interpolated exception with no property access");
    }

    /// <summary>Revert-proof for the round-2 widening (#4293 R2-L4): the pre-round-2 pattern would have missed
    /// every new shape this round adds — the ?/!/) receiver tail, the exception-shaped-receiver-only new
    /// property names, Fault/fault (bare and dotted), and the string-literal concatenation arm. Proven by
    /// running the round-1 pattern against the same cases here, once, rather than by hand-reverting
    /// <see cref="s_exMessagePattern"/> and re-running the suite (as the L2 revert-proof above already does for
    /// round 1).</summary>
    [Fact]
    public void ExMessagePattern_Round1Pattern_WouldHaveMissedTheRound2WidenedCases()
    {
        var round1 = new Regex(
            @"\b[A-Za-z_]\w*\.(Message\b|MessageText\b|InnerException\b|Detail\b|Hint\b|Where\b(?!\s*\())"
            + @"|\b(?:[A-Za-z_]*[Ee]x(?:ception)?|e)\.ToString\(\)"
            + @"|\{(?:[A-Za-z_]*[Ee]x(?:ception)?|e)\}",
            RegexOptions.Compiled);

        foreach (var missed in new[]
        {
            "ex?.Message", "(ex as PostgresException)?.MessageText", "ex.GetBaseException().Message",
            "ex.StackTrace", "pgEx.InternalQuery", "outcome.Fault.TableName", "outcome.Fault.ToString()",
            "$\"{outcome.Fault}\"", "\"Query failed: \" + ex",
        })
        {
            Assert.False(round1.IsMatch(missed), $"the pre-round-2 pattern was expected to miss: {missed}");
        }
    }

    /// <summary>#4293 round 2 (R2-L3): an allow-list snippet vouches only for the MATCH it actually contains on
    /// its line — the retired <c>line.Contains(snippet)</c> check waved through ANY other exception-text
    /// access sharing a line with an allow-listed one (see the revert-proof below). Scans every occurrence of
    /// <paramref name="snippet"/> on <paramref name="line"/> (not just the first) and accepts if the match's
    /// span (<paramref name="column"/>, <paramref name="length"/>) falls entirely inside one of them.</summary>
    private static bool SnippetCovers(string line, string snippet, int column, int length)
    {
        for (var at = line.IndexOf(snippet, StringComparison.Ordinal); at >= 0; at = line.IndexOf(snippet, at + 1, StringComparison.Ordinal))
        {
            if (column >= at && column + length <= at + snippet.Length)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>The per-file half of the census (#4293 round 2, R2-L3), pulled out of
    /// <see cref="NoWebEndpoint_BuildsAnAnswerFromExMessage_ExceptTheNamedAllowList"/> so
    /// <see cref="SnippetCovers_RealAllowList_OnASyntheticLine_LeavesExDetailUnaccounted"/> can run the SAME
    /// check against one fabricated line instead of the whole roster. Returns one description per
    /// <see cref="s_exMessagePattern"/> match that no <paramref name="relativePath"/> allow-list entry
    /// covers.</summary>
    private static List<string> ComputeUnaccounted(string relativePath, string code)
    {
        var unaccounted = new List<string>();
        var lines = code.Split('\n');
        var matches = s_exMessagePattern.Matches(code);

        foreach (Match match in matches)
        {
            // #4283 review round 1 (L2): match against the OFFENDING LINE, not a ±60-char window — a
            // window can spill the allow-listed snippet from a neighboring statement onto a line that
            // never contains it, silently marking a real hit accounted for.
            var lineIndex = 0;
            for (var i = 0; i < match.Index; i++)
            {
                if (code[i] == '\n')
                {
                    lineIndex++;
                }
            }

            var line = lines[lineIndex];
            // #4293 round 2 (R2-L3): the match's column ON THAT LINE, so SnippetCovers can bound-check it
            // against a snippet occurrence instead of asking only whether the snippet appears SOMEWHERE on
            // the line.
            var lineStart = match.Index == 0 ? 0 : code.LastIndexOf('\n', match.Index - 1) + 1;
            var column = match.Index - lineStart;
            var accounted = false;
            foreach (var (file, snippet, _) in s_allowList)
            {
                if (string.Equals(file, relativePath, StringComparison.Ordinal) && SnippetCovers(line, snippet, column, match.Length))
                {
                    accounted = true;
                    break;
                }
            }

            if (!accounted)
            {
                unaccounted.Add($"{relativePath}: ...{line.Trim()}...");
            }
        }

        return unaccounted;
    }

    [Fact]
    public void NoWebEndpoint_BuildsAnAnswerFromExMessage_ExceptTheNamedAllowList()
    {
        var root = RepoFile.Root;
        var unaccounted = new List<string>();

        foreach (var relative in s_roster)
        {
            var path = Path.Combine(root, relative);
            Assert.True(File.Exists(path), $"#4283 census target not found: {path}");

            unaccounted.AddRange(ComputeUnaccounted(relative, StripComments(File.ReadAllText(path))));
        }

        Assert.True(unaccounted.Count == 0,
            "#4283: an ex.Message/ex.MessageText reached web-surface code outside the named allow-list:\n"
            + string.Join("\n", unaccounted));
    }

    /// <summary>#4293 round 2 (R2-L3): SnippetCovers itself — a match is covered when it sits inside SOME
    /// occurrence of the snippet on the line, not only the first one SnippetCovers happens to find.</summary>
    [Fact]
    public void SnippetCovers_CoveredAndUncoveredAndSecondOccurrence()
    {
        const string line = "foo bar foo";

        // Covered: the match sits inside the FIRST occurrence of "foo" (columns 0-2).
        Assert.True(SnippetCovers(line, "foo", 0, 3));

        // Uncovered: "bar" (columns 4-6) is not inside ANY occurrence of "foo".
        Assert.False(SnippetCovers(line, "foo", 4, 3));

        // A second occurrence: the match sits inside the SECOND "foo" (columns 8-10) only - SnippetCovers must
        // keep scanning past the first occurrence rather than stopping there.
        Assert.True(SnippetCovers(line, "foo", 8, 3));
    }

    /// <summary>#4293 round 2 (R2-L3): the real allow-list's "Query failed: {ex.MessageText}" entry
    /// (DarlingWebEndpoints.cs) covers the ex.MessageText match on this synthetic line, but ex.Detail - a
    /// SECOND exception-text access sharing the same line - is not part of that snippet at all and must come
    /// back unaccounted. The retired whole-line check could not tell the two apart (see the revert-proof
    /// below).</summary>
    [Fact]
    public void SnippetCovers_RealAllowList_OnASyntheticLine_LeavesExDetailUnaccounted()
    {
        var relative = Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingWebEndpoints.cs");
        const string code = "$\"Query failed: {ex.MessageText} {ex.Detail}\"";

        var unaccounted = ComputeUnaccounted(relative, code);

        var single = Assert.Single(unaccounted);
        Assert.Contains("ex.Detail", single, StringComparison.Ordinal);
    }

    /// <summary>Revert-proof for R2-L3 (run once, by hand, against SnippetCovers deleted and
    /// ComputeUnaccounted's per-match check reverted to the pre-round-2
    /// <c>line.Contains(snippet, StringComparison.Ordinal)</c>): the retired whole-line check finds the
    /// allow-listed snippet ANYWHERE on <see cref="SnippetCovers_RealAllowList_OnASyntheticLine_LeavesExDetailUnaccounted"/>'s
    /// synthetic line and would have marked BOTH matches accounted - including ex.Detail, which the snippet has
    /// nothing to do with - so it would have reported zero unaccounted where the round-2 check correctly
    /// reports one.</summary>
    [Fact]
    public void SnippetCovers_OldLineContainsCheck_WouldHaveMarkedExDetailAccountedToo()
    {
        const string line = "$\"Query failed: {ex.MessageText} {ex.Detail}\"";
        const string snippet = "Query failed: {ex.MessageText}";

        Assert.True(line.Contains(snippet, StringComparison.Ordinal),
            "the retired whole-line check was expected to find the allow-listed snippet anywhere on the line, " +
            "which is exactly the false-accounting R2-L3 fixes");
    }

    /// <summary>The allow-list's OTHER direction: every entry's snippet must still be found (a snippet that
    /// vanished means the code moved or was fixed and the allow-list enn should shrink, not silently stop
    /// covering nothing).</summary>
    [Fact]
    public void AllowList_EveryEntry_IsStillPresentInItsFile()
    {
        var root = RepoFile.Root;
        foreach (var (file, snippet, reason) in s_allowList)
        {
            var code = StripComments(File.ReadAllText(Path.Combine(root, file)));
            Assert.True(code.Contains(snippet, StringComparison.Ordinal),
                $"#4283 allow-list entry no longer found in {file} (\"{reason}\"): {snippet}");
        }
    }

    /* ═══════════════════════════ DarlingWebFailureLog: the sentence-based twin ═══════════════════════════ */

    [Fact]
    public void IsStatementTimeoutSentence_CarriesTheSqlStateToken_IsTrue()
    {
        var sentence = McpHelpers.ErrorSentence("get_blocking",
            new PostgresException("canceling statement due to statement timeout", "ERROR", "ERROR", "57014"));

        Assert.True(DarlingWebFailureLog.IsStatementTimeoutSentence(sentence));
    }

    [Fact]
    public void IsStatementTimeoutSentence_OtherPostgresError_IsFalse()
    {
        var sentence = McpHelpers.ErrorSentence("get_blocking",
            new PostgresException("relation \"x\" does not exist", "ERROR", "ERROR", "42P01"));

        Assert.False(DarlingWebFailureLog.IsStatementTimeoutSentence(sentence));
    }

    [Fact]
    public void IsStatementTimeoutSentence_DigitRunThatIsNot57014_IsFalse()
    {
        /* The word-boundary defense (MigrationDataMovingRungCensusPins' s_cancelTrap precedent): a row count
           or identifier that merely CONTAINS 57014 as a substring must not false-positive. */
        Assert.False(DarlingWebFailureLog.IsStatementTimeoutSentence("Error during get_x: 5701400 rows affected"));
        Assert.False(DarlingWebFailureLog.IsStatementTimeoutSentence("Error during get_x: table_157014 missing"));
    }

    [Fact]
    public void IsStatementTimeoutSentence_57014AppearsOnlyAsATailValue_IsFalse()
    {
        /* #4283 L1: the anchor's real target — a digit run that IS 57014, with word boundaries either side
           (so the pre-anchor \b57014\b regex false-positived here), but appearing in the TAIL of the sentence
           as a quoted value rather than right after a known prefix. The real SQLSTATE here is 22P02. */
        Assert.False(DarlingWebFailureLog.IsStatementTimeoutSentence(
            "Error during get_x: 22P02: invalid input syntax for type integer: \"57014\""));
        Assert.False(DarlingWebFailureLog.IsStatementTimeoutSentence(
            $"{DarlingServerResolver.RegistryReadFaultPrefix}22P02: invalid input syntax for type integer: \"57014\""));
    }

    [Fact]
    public void IsStatementTimeoutSentence_ResolverConnectionErrorNamingPort57014_IsFalse()
    {
        /* A non-Postgres connection exception (no "SqlState: " shape at all) that happens to name port 57014
           must not match either. */
        Assert.False(DarlingWebFailureLog.IsStatementTimeoutSentence(
            $"{DarlingServerResolver.RegistryReadFaultPrefix}Failed to connect to 10.0.0.5:57014"));
    }

    [Fact]
    public void IsStatementTimeoutSentence_RealTimeout_OnEachKnownPrefix_IsTrue()
    {
        const string tail = "57014: canceling statement due to statement timeout";
        Assert.True(DarlingWebFailureLog.IsStatementTimeoutSentence($"Error during get_x: {tail}"));
        Assert.True(DarlingWebFailureLog.IsStatementTimeoutSentence($"Error running query: {tail}"));

        var resolverSentence = $"{DarlingServerResolver.RegistryReadFaultPrefix}{tail}";
        Assert.True(DarlingWebFailureLog.IsStatementTimeoutSentence(resolverSentence));
        Assert.Equal(StatusCodes.Status503ServiceUnavailable, DarlingWebFailureLog.StatusCode(resolverSentence));
    }

    [Fact]
    public void StatusCodeAndBody_SentenceOverload_MatchTheExceptionOverload()
    {
        var ex = new PostgresException("cancelled", "ERROR", "ERROR", "57014");
        var sentence = McpHelpers.ErrorSentence("get_x", ex);

        Assert.Equal(DarlingWebFailureLog.StatusCode(ex), DarlingWebFailureLog.StatusCode(sentence));
        Assert.Equal(DarlingWebFailureLog.Body(ex)["error"]!.GetValue<string>(), DarlingWebFailureLog.Body(sentence)["error"]!.GetValue<string>());
    }

    [Fact]
    public void Body_SentenceOverload_NeverCarriesTheExceptionText()
    {
        var sentence = McpHelpers.ErrorSentence("get_object_locking",
            new InvalidOperationException("Host=10.0.0.5;Port=5432 connection refused"));

        var wire = DarlingWebFailureLog.Body(sentence).ToJsonString();
        Assert.DoesNotContain("10.0.0.5", wire, StringComparison.Ordinal);
        Assert.Equal(DarlingWebFailureLog.GenericMessage, DarlingWebFailureLog.Body(sentence)["error"]!.GetValue<string>());
    }

    [Fact]
    public void Report_SentenceOverload_WritesExactlyOneWarningForATimeoutToken()
    {
        var logger = new CapturingTestLogger();
        var sentence = McpHelpers.ErrorSentence("get_blocking",
            new PostgresException("canceling statement due to statement timeout", "ERROR", "ERROR", "57014"));

        DarlingWebFailureLog.Report(logger, "/api/read/get_blocking", 12, sentence);

        Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Error));
        Assert.Contains("/api/read/get_blocking", logger.Joined, StringComparison.Ordinal);
        Assert.Contains(sentence, logger.Joined, StringComparison.Ordinal);
    }

    [Fact]
    public void Report_SentenceOverload_WritesExactlyOneErrorWhenNoTimeoutToken()
    {
        var logger = new CapturingTestLogger();
        var sentence = McpHelpers.ErrorSentence("get_object_locking", new InvalidOperationException("boom"));

        DarlingWebFailureLog.Report(logger, "/api/read/get_object_locking", 3, sentence);

        Assert.Equal(1, logger.CountAtLevel(LogLevel.Error));
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
    }

    /* ═══════════════════════════ DarlingWebEndpoints.ToHttpResult: the ServerError arm ═══════════════════════════ */

    [Fact]
    public void ToHttpResult_ToolCaughtTimeout_MapsTo503_NoExceptionText_OneWarning()
    {
        var logger = new CapturingTestLogger();
        var ex = new PostgresException("canceling statement due to statement timeout", "ERROR", "ERROR", "57014");
        var envelope = McpHelpers.FormatError("get_blocking", ex);

        var result = DarlingWebEndpoints.ToHttpResult(envelope, "/api/read/get_blocking", logger, 77);

        var json = ResultBody(result, out var statusCode);
        Assert.Equal(StatusCodes.Status503ServiceUnavailable, statusCode);
        Assert.Equal(DarlingWebFailureLog.TimeoutMessage, json.RootElement.GetProperty("error").GetString());
        Assert.DoesNotContain("57014", json.RootElement.GetProperty("error").GetString(), StringComparison.Ordinal);
        Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
    }

    [Fact]
    public void ToHttpResult_ToolCaughtGenericFailure_MapsTo500_NoExceptionText_OneError()
    {
        var logger = new CapturingTestLogger();
        var ex = new InvalidOperationException("Host=store.internal;Port=5432 role \"app_rw\" failed");
        var envelope = McpHelpers.FormatError("get_object_locking", ex);

        var result = DarlingWebEndpoints.ToHttpResult(envelope, "/api/read/get_object_locking", logger, 5);

        var json = ResultBody(result, out var statusCode);
        Assert.Equal(StatusCodes.Status500InternalServerError, statusCode);
        Assert.Equal(DarlingWebFailureLog.GenericMessage, json.RootElement.GetProperty("error").GetString());
        Assert.DoesNotContain("store.internal", json.RootElement.GetProperty("error").GetString(), StringComparison.Ordinal);
        Assert.Equal(1, logger.CountAtLevel(LogLevel.Error));
    }

    /// <summary>#4315 round 1, Low 3: the <c>/api/read</c> body on a <c>get_sweep_reports</c> store fault,
    /// built exactly as the tool's own catch builds it (<see cref="McpHelpers.FormatError"/> from a
    /// <see cref="PostgresException"/> with SqlState 42P01, the undefined_table code a dropped relation
    /// raises) and fed through the same <see cref="DarlingWebEndpoints.ToHttpResult"/> the dispatcher
    /// uses. No database: the envelope is hand-built, so this pins the wire shape without a live store.</summary>
    [Fact]
    public void ToHttpResult_ToolCaughtUndefinedTable_MapsTo500_NoRelationOrSqlStateText_OneError()
    {
        var logger = new CapturingTestLogger();
        var ex = new PostgresException("relation \"x\" does not exist", "ERROR", "ERROR", "42P01");
        var envelope = McpHelpers.FormatError("get_sweep_reports", ex);

        var result = DarlingWebEndpoints.ToHttpResult(envelope, "/api/read/get_sweep_reports", logger, 9);

        var json = ResultBody(result, out var statusCode);
        Assert.Equal(StatusCodes.Status500InternalServerError, statusCode);
        var body = json.RootElement.GetProperty("error").GetString();
        Assert.Equal(DarlingWebFailureLog.GenericMessage, body);
        Assert.DoesNotContain("42P01", body, StringComparison.Ordinal);
        Assert.DoesNotContain("relation", body, StringComparison.Ordinal);
        Assert.Equal(1, logger.CountAtLevel(LogLevel.Error));
    }

    [Fact]
    public void ToHttpResult_Refusal_IsUnchanged_StillTheEnvelopeAt400()
    {
        var logger = new CapturingTestLogger();
        var refusal = McpHelpers.Refusal("hours_back", "hours_back must be between 1 and 8760.");

        var result = DarlingWebEndpoints.ToHttpResult(refusal, "/api/read/get_x", logger, 1);

        ResultBody(result, out var statusCode);
        Assert.Equal(StatusCodes.Status400BadRequest, statusCode);
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Error));
    }

    [Fact]
    public void ToHttpResult_ResolverRegistryFault_MapsTo500_NotClientError_OneErrorLog()
    {
        var logger = new CapturingTestLogger();
        var sentence = $"{DarlingServerResolver.RegistryReadFaultPrefix}Host=store.internal;Port=5432 role \"app_rw\" failed";

        var result = DarlingWebEndpoints.ToHttpResult(sentence, "/api/triage", logger, 5);

        var json = ResultBody(result, out var statusCode);
        Assert.Equal(StatusCodes.Status500InternalServerError, statusCode);
        Assert.Equal(DarlingWebFailureLog.GenericMessage, json.RootElement.GetProperty("error").GetString());
        Assert.DoesNotContain("store.internal", json.RootElement.GetProperty("error").GetString(), StringComparison.Ordinal);
        Assert.Equal(1, logger.CountAtLevel(LogLevel.Error));
    }

    [Fact]
    public void ToHttpResult_ResolverRegistryFaultTimeout_MapsTo503()
    {
        var logger = new CapturingTestLogger();
        var sentence = $"{DarlingServerResolver.RegistryReadFaultPrefix}57014: canceling statement due to statement timeout";

        var result = DarlingWebEndpoints.ToHttpResult(sentence, "/api/triage", logger, 5);

        var json = ResultBody(result, out var statusCode);
        Assert.Equal(StatusCodes.Status503ServiceUnavailable, statusCode);
        Assert.Equal(DarlingWebFailureLog.TimeoutMessage, json.RootElement.GetProperty("error").GetString());
        Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
    }

    /// <summary>The resolver's own fault-sentence prefix, pinned byte-identical to what
    /// <c>DarlingServerResolver.LoadEnabledOrFaultAsync</c>'s catch built inline before #4283 H2 factored it out
    /// — an MCP caller (which reads this sentence back unchanged through <c>McpHelpers.ErrorMessageOf</c>, never
    /// through <see cref="DarlingWebEndpoints.ClassifyToolResponse"/>) sees no text change from this fix.</summary>
    [Fact]
    public void RegistryReadFaultPrefix_IsPinnedByteIdentical_ToThePreFixLiteral()
    {
        Assert.Equal("Could not read the servers registry from the Postgres store: ", DarlingServerResolver.RegistryReadFaultPrefix);
    }

    /* ═══════════════════════════ DarlingWebEndpoints.MuteRuleToolResult: the ServerError arm (#4283 H1) ═══════════════════════════ */

    [Fact]
    public void MuteRuleToolResult_ServerError_MapsTo500_NoExceptionText_OneErrorLog()
    {
        var logger = new CapturingTestLogger();
        var ex = new InvalidOperationException("Host=store.internal;Port=5432 role \"app_rw\" failed");
        var envelope = McpHelpers.FormatError("create_mute_rule", ex);

        var result = DarlingWebEndpoints.MuteRuleToolResult(envelope, "/api/mute-rules", logger, 5, StatusCodes.Status201Created);

        var json = ResultBody(result, out var statusCode);
        Assert.Equal(StatusCodes.Status500InternalServerError, statusCode);
        Assert.Equal(DarlingWebFailureLog.GenericMessage, json.RootElement.GetProperty("error").GetString());
        Assert.DoesNotContain("store.internal", json.RootElement.GetProperty("error").GetString(), StringComparison.Ordinal);
        Assert.Equal(1, logger.CountAtLevel(LogLevel.Error));
    }

    [Fact]
    public void MuteRuleToolResult_ServerErrorTimeout_MapsTo503()
    {
        var logger = new CapturingTestLogger();
        var ex = new PostgresException("canceling statement due to statement timeout", "ERROR", "ERROR", "57014");
        var envelope = McpHelpers.FormatError("create_mute_rule", ex);

        var result = DarlingWebEndpoints.MuteRuleToolResult(envelope, "/api/mute-rules", logger, 5, StatusCodes.Status201Created);

        var json = ResultBody(result, out var statusCode);
        Assert.Equal(StatusCodes.Status503ServiceUnavailable, statusCode);
        Assert.Equal(DarlingWebFailureLog.TimeoutMessage, json.RootElement.GetProperty("error").GetString());
        Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
    }

    /* ═══════════════════════════ DarlingWebEndpoints.ComposeRunFailureResult / IsComposeRunAuthorActionable (#4283 M1) ═══════════════════════════ */

    /// <summary>The ruled design (round-1 review, M1): 42601 (syntax error) is a class-42 SQLSTATE, not 42501,
    /// so it stays author-actionable — the existing "Query failed: {MessageText}" 400 pin must stay green,
    /// verbatim, with no <see cref="DarlingWebEndpoints.ComposeRunOutcome.Fault"/> attached.</summary>
    [Fact]
    public void IsComposeRunAuthorActionable_SyntaxError42601_IsTrue_AndStaysBadRequest_MessageTextVerbatim()
    {
        Assert.True(DarlingWebEndpoints.IsComposeRunAuthorActionable("42601"));

        var logger = new CapturingTestLogger();
        var ex = new PostgresException("syntax error at or near \"selct\"", "ERROR", "ERROR", "42601");
        var outcome = DarlingWebEndpoints.ComposeRunOutcome.BadRequest($"Query failed: {ex.MessageText}");

        var result = DarlingWebEndpoints.ComposeRunFailureResult(outcome, "/api/compose/run", logger, 5);

        var json = ResultBody(result, out var statusCode);
        Assert.Equal(StatusCodes.Status400BadRequest, statusCode);
        Assert.Equal("Query failed: syntax error at or near \"selct\"", json.RootElement.GetProperty("error").GetString());
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Error));
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
    }

    /// <summary>42501 (insufficient_privilege) is the one class-42 SQLSTATE the ruling carves OUT of
    /// author-actionable: a STORE role problem, not the panel.</summary>
    [Fact]
    public void IsComposeRunAuthorActionable_InsufficientPrivilege42501_IsFalse()
    {
        Assert.False(DarlingWebEndpoints.IsComposeRunAuthorActionable("42501"));
    }

    /// <summary>28P01 (auth failure) is not author-actionable — the ruled design's <c>Fault</c> arm answers
    /// through the SAME fixed-body backstop #4276 gives an uncaught exception: 500, the generic message, one
    /// error log, and the role name in the synthetic exception's own text never reaches the wire.</summary>
    [Fact]
    public void ComposeRunFailureResult_AuthFailure28P01_AnswersFixedBody_LogsOnce_NoRoleText()
    {
        Assert.False(DarlingWebEndpoints.IsComposeRunAuthorActionable("28P01"));

        var logger = new CapturingTestLogger();
        var ex = new PostgresException("password authentication failed for user \"app_rw\"", "FATAL", "FATAL", "28P01");
        var outcome = DarlingWebEndpoints.ComposeRunOutcome.BadRequest($"Query failed: {ex.MessageText}", ex);

        var result = DarlingWebEndpoints.ComposeRunFailureResult(outcome, "/api/compose/run", logger, 5);

        var json = ResultBody(result, out var statusCode);
        Assert.Equal(StatusCodes.Status500InternalServerError, statusCode);
        Assert.Equal(DarlingWebFailureLog.GenericMessage, json.RootElement.GetProperty("error").GetString());
        Assert.DoesNotContain("app_rw", json.RootElement.GetProperty("error").GetString(), StringComparison.Ordinal);
        Assert.Equal(1, logger.CountAtLevel(LogLevel.Error));
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
    }

    /// <summary>57P01 (admin shutdown) is 28P01's twin here — not author-actionable, same fixed body, one log
    /// line, and the host name in the synthetic exception's own text never reaches the wire.</summary>
    [Fact]
    public void ComposeRunFailureResult_AdminShutdown57P01_AnswersFixedBody_LogsOnce_NoHostText()
    {
        Assert.False(DarlingWebEndpoints.IsComposeRunAuthorActionable("57P01"));

        var logger = new CapturingTestLogger();
        var ex = new PostgresException("terminating connection due to administrator command on host db-primary.internal", "FATAL", "FATAL", "57P01");
        var outcome = DarlingWebEndpoints.ComposeRunOutcome.BadRequest($"Query failed: {ex.MessageText}", ex);

        var result = DarlingWebEndpoints.ComposeRunFailureResult(outcome, "/api/compose/run", logger, 5);

        var json = ResultBody(result, out var statusCode);
        Assert.Equal(StatusCodes.Status500InternalServerError, statusCode);
        Assert.Equal(DarlingWebFailureLog.GenericMessage, json.RootElement.GetProperty("error").GetString());
        Assert.DoesNotContain("db-primary.internal", json.RootElement.GetProperty("error").GetString(), StringComparison.Ordinal);
        Assert.Equal(1, logger.CountAtLevel(LogLevel.Error));
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
    }

    /// <summary>Revert-proof for M1, extended by round 2 (R2-L1, R2-L2): reverting the catch body to the
    /// pre-M1 single-arm <c>ComposeRunOutcome.BadRequest($"Query failed: {ex.MessageText}")</c> for every
    /// SQLSTATE fails the 28P01 and 57P01 tests above (the role/host text they assert absent is exactly what
    /// the old catch put on the wire) — and, since round 2 pulled the decision into
    /// <see cref="DarlingWebEndpoints.FromPostgresException"/>, that same revert also fails
    /// <c>RunComposedPanelAsync_PostgresExceptionCatch_CallsFromPostgresException</c>'s source pin and every
    /// <c>FromPostgresException_*</c> test below — not only a revert that deletes
    /// <c>ComposeRunOutcome.AuthorQueryError</c> outright.</summary>
    [Fact]
    public void ComposeRunFailureResult_NonActionableFault_IsServerErrorFalse_ButStillMapsThroughFault()
    {
        // A Fault-carrying outcome is a BadRequest shape (IsServerError false) that ComposeRunFailureResult
        // must still route through the Fault arm, not the plain 400 ErrorResult arm — pinning the ordering
        // the doc comment above ComposeRunFailureResult describes.
        var ex = new PostgresException("too many connections for role \"app_rw\"", "FATAL", "FATAL", "53300");
        var outcome = DarlingWebEndpoints.ComposeRunOutcome.BadRequest($"Query failed: {ex.MessageText}", ex);

        Assert.False(outcome.IsServerError);
        Assert.NotNull(outcome.Fault);

        var result = DarlingWebEndpoints.ComposeRunFailureResult(outcome, "/api/compose/run", new CapturingTestLogger(), 5);
        ResultBody(result, out var statusCode);
        Assert.Equal(StatusCodes.Status500InternalServerError, statusCode);
    }

    /* ═══════════════════════════ DarlingWebEndpoints.FromPostgresException / ComposeRunOutcome.AuthorSqlState (#4293 round 2, R2-L1/R2-L2) ═══════════════════════════ */

    /// <summary>#4293 round 2 (R2-L2): a FATAL or PANIC severity is a connection-level store fault whatever its
    /// class - a startup parameter the server rejects answers FATAL 22023 or 42704, which IsComposeRunAuthorActionable
    /// alone would call author-actionable because it only looks at the SQLSTATE. Those two, plus the
    /// pre-existing ERROR-severity non-actionable codes (28P01/57P01/53300/42501), all resolve to the SAME
    /// Fault-carrying arm: a non-null Fault (the same PostgresException instance) and a null AuthorSqlState.</summary>
    [Fact]
    public void FromPostgresException_FatalOrPanicOrNonActionable_CarriesFault_NoAuthorSqlState()
    {
        foreach (var (message, severity, sqlState) in new[]
        {
            ("password authentication failed for user \"app_rw\"", "ERROR", "28P01"),
            ("terminating connection due to administrator command", "ERROR", "57P01"),
            ("too many connections for role \"app_rw\"", "ERROR", "53300"),
            ("permission denied for table t", "ERROR", "42501"),
            ("invalid value for parameter \"statement_timeout\": \"-1\"", "FATAL", "22023"),
            ("unrecognized configuration parameter \"nonexistent.setting\"", "FATAL", "42704"),
        })
        {
            var ex = new PostgresException(message, severity, severity, sqlState);
            var outcome = DarlingWebEndpoints.FromPostgresException(ex);

            Assert.False(outcome.IsServerError, $"sqlState {sqlState} at {severity}");
            Assert.Same(ex, outcome.Fault);
            Assert.Null(outcome.AuthorSqlState);
        }
    }

    /// <summary>#4293 round 2 (R2-L1): the author-actionable arm, ERROR severity only (57014 / class-22 /
    /// class-42 except 42501) - a null Fault, AuthorSqlState set to the code, and Error carrying the verbatim
    /// MessageText, unchanged from round 1's single-arm ternary.</summary>
    [Fact]
    public void FromPostgresException_AuthorActionableAtErrorSeverity_CarriesAuthorSqlState_NoFault()
    {
        foreach (var (message, sqlState) in new[]
        {
            ("canceling statement due to statement timeout", "57014"),
            ("invalid input syntax for type numeric", "22012"),
            ("syntax error at or near \"selct\"", "42601"),
            ("relation \"x\" does not exist", "42P01"),
        })
        {
            var ex = new PostgresException(message, "ERROR", "ERROR", sqlState);
            var outcome = DarlingWebEndpoints.FromPostgresException(ex);

            Assert.False(outcome.IsServerError, $"sqlState {sqlState}");
            Assert.Null(outcome.Fault);
            Assert.Equal(sqlState, outcome.AuthorSqlState);
            Assert.Equal($"Query failed: {message}", outcome.Error);
        }
    }

    /// <summary>Source pin (R2-L1, R2-L2): the PostgresException catch inside RunComposedPanelAsync must call
    /// FromPostgresException - not re-inline the old single-arm ternary - so a revert of the catch fails a
    /// test even when it keeps ComposeRunOutcome.AuthorQueryError intact (see the extended revert-proof above
    /// ComposeRunFailureResult_NonActionableFault_IsServerErrorFalse_ButStillMapsThroughFault).</summary>
    [Fact]
    public void RunComposedPanelAsync_PostgresExceptionCatch_CallsFromPostgresException()
    {
        var root = RepoFile.Root;
        var code = StripComments(File.ReadAllText(
            Path.Combine(root, "Darling", "PerformanceMonitor.Darling.Service", "DarlingWebEndpoints.cs")));

        var catchStart = code.IndexOf("catch (PostgresException ex)", StringComparison.Ordinal);
        Assert.True(catchStart >= 0, "RunComposedPanelAsync's PostgresException catch was not found");

        var catchEnd = code.IndexOf("catch (Exception ex)", catchStart, StringComparison.Ordinal);
        Assert.True(catchEnd > catchStart, "the generic Exception catch bounding the PostgresException catch was not found");

        var catchBody = code.Substring(catchStart, catchEnd - catchStart);
        Assert.Contains("return FromPostgresException(ex);", catchBody, StringComparison.Ordinal);
    }

    /// <summary>#4293 round 2 (R2-L1): an AuthorQueryError outcome still answers 400 with the panel author's
    /// own text (unchanged from round 1's "Query failed: {MessageText}"), but ComposeRunFailureResult now logs
    /// it once - so store drift (42P01/42703 after a migration that did not finish) reaches the service log,
    /// not only the one browser that hit it. A plain BadRequest with no AuthorSqlState still logs nothing,
    /// same as every M1 test above.</summary>
    [Fact]
    public void ComposeRunFailureResult_AuthorQueryError_AnswersItsText400_LogsOneWarning()
    {
        var logger = new CapturingTestLogger();
        var outcome = DarlingWebEndpoints.ComposeRunOutcome.AuthorQueryError("Query failed: syntax error at or near \"selct\"", "42601");

        var result = DarlingWebEndpoints.ComposeRunFailureResult(outcome, "/api/compose/run", logger, 5);

        var json = ResultBody(result, out var statusCode);
        Assert.Equal(StatusCodes.Status400BadRequest, statusCode);
        Assert.Equal("Query failed: syntax error at or near \"selct\"", json.RootElement.GetProperty("error").GetString());
        Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Error));
    }

    [Fact]
    public void ComposeRunFailureResult_PlainBadRequest_LogsNothing()
    {
        var logger = new CapturingTestLogger();
        var outcome = DarlingWebEndpoints.ComposeRunOutcome.BadRequest("bad spec");

        var result = DarlingWebEndpoints.ComposeRunFailureResult(outcome, "/api/compose/run", logger, 5);

        var json = ResultBody(result, out var statusCode);
        Assert.Equal(StatusCodes.Status400BadRequest, statusCode);
        Assert.Equal("bad spec", json.RootElement.GetProperty("error").GetString());
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Warning));
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Error));
    }

    /// <summary>Reads an <see cref="IResult"/> built by <c>Results.Json</c>/<c>Results.Text</c> the same way
    /// the pipeline would, via <see cref="DefaultHttpContext"/>'s response body.</summary>
    private static JsonDocument ResultBody(IResult result, out int statusCode)
    {
        /* Results.Json/Results.Text resolve IOptions<JsonOptions> from RequestServices; AddOptions() alone
           supplies the default-valued IOptions<T> that resolves to (no explicit configuration needed). */
        var services = new ServiceCollection();
        services.AddOptions();
        services.AddLogging();
        var context = new DefaultHttpContext { RequestServices = services.BuildServiceProvider() };
        var stream = new MemoryStream();
        context.Response.Body = stream;

        result.ExecuteAsync(context).GetAwaiter().GetResult();

        statusCode = context.Response.StatusCode;
        stream.Position = 0;
        using var reader = new StreamReader(stream);
        return JsonDocument.Parse(reader.ReadToEnd());
    }

    /* ═══════════════════════════ live: a REAL tool-caught statement_timeout, on the rig ═══════════════════════════ */

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>
    /// #4283's centerpiece, end to end against the rig: a route registered AFTER the REAL <c>MapAll</c> (the
    /// same after-ConfigurePipeline registration <see cref="DarlingWebFailureHandlingTests"/>'s own
    /// <c>/api/__test/*</c> routes use) runs <c>SELECT pg_sleep(5)</c> under a connection-string
    /// <c>statement_timeout</c> of 100 ms — deterministic regardless of the test database's own size or
    /// speed, unlike racing a real tool's own (possibly sub-millisecond, on an empty test database) query
    /// against the clock. The server cancels it at SQLSTATE 57014, the route's catch is the SAME shape every
    /// real PostgreSQL tool uses (<c>catch (Exception ex) { return McpHelpers.FormatError(op, ex); }</c>), and
    /// the result is handed to the REAL <see cref="DarlingWebEndpoints.ToHttpResult"/> the <c>/api/read/*</c>
    /// dispatch loop calls — proving THIS lane's new sentence classifier against a REAL exception's REAL
    /// <see cref="PostgresException.Message"/> text, not a hand-built stand-in.
    /// </summary>
    [Fact]
    public async Task RealToolCaughtStatementTimeout_OnTheRig_Returns503_NoExceptionText_OneLogLine()
    {
        var cs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live #4283 timeout test.");

        var ct = TestContext.Current.CancellationToken;
        await using (var setupConnection = new NpgsqlConnection(cs))
        {
            await setupConnection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(setupConnection, ct);
        }

        var timeoutCs = new NpgsqlConnectionStringBuilder(cs) { Options = "-c statement_timeout=100" }.ToString();
        await using var postgres = NpgsqlDataSource.Create(timeoutCs);

        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            ContentRootPath = Path.Combine(RepoFile.Root, "Darling", "PerformanceMonitor.Darling.Service"),
            WebRootPath = "wwwroot",
        });
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(postgres);

        var app = builder.Build();
        var logger = new CapturingTestLogger();

        DarlingWebEndpoints.MapAll(app, postgres, new CollectorRuntimeState(), logger);

        app.MapGet("/api/__test/tool-caught-timeout", async (HttpContext context) =>
        {
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            string result;
            try
            {
                await using var connection = await postgres.OpenConnectionAsync(context.RequestAborted);
                await using var command = connection.CreateCommand();
                command.CommandText = "SELECT pg_sleep(5)";
                await command.ExecuteNonQueryAsync(context.RequestAborted);
                result = "{\"status\":\"empty\"}";
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                /* The tools' own shape (McpHelpers.FormatError, #3653 Q11) — this route exists ONLY to force a
                   deterministic real 57014 into that shape; it is not itself a tool. */
                result = McpHelpers.FormatError("test_tool", ex);
            }

            return DarlingWebEndpoints.ToHttpResult(result, "/api/__test/tool-caught-timeout", logger, stopwatch.ElapsedMilliseconds);
        });

        await app.StartAsync(ct);
        using var server = app.GetTestServer();

        var httpContext = await server.SendAsync(request =>
        {
            request.Request.Method = "GET";
            request.Request.Path = "/api/__test/tool-caught-timeout";
            request.Request.Headers.Host = "localhost";
        });

        Assert.Equal(StatusCodes.Status503ServiceUnavailable, httpContext.Response.StatusCode);

        using var reader = new StreamReader(httpContext.Response.Body);
        var bodyText = await reader.ReadToEndAsync(ct);
        using var body = JsonDocument.Parse(bodyText);

        Assert.Equal(DarlingWebFailureLog.TimeoutMessage, body.RootElement.GetProperty("error").GetString());
        Assert.DoesNotContain("57014", bodyText, StringComparison.Ordinal);
        Assert.DoesNotContain("pg_sleep", bodyText, StringComparison.Ordinal);
        Assert.DoesNotContain("canceling statement", bodyText, StringComparison.Ordinal);

        Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Error));
        Assert.Contains("57014", logger.Joined, StringComparison.Ordinal);
        Assert.Contains("/api/__test/tool-caught-timeout", logger.Joined, StringComparison.Ordinal);

        await app.StopAsync(ct);
    }
}
