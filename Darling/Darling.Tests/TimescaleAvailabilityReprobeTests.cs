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
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3815: the TimescaleDB availability latch is re-probed on the running service, and the re-probe is
/// reachable in the only state it is for.
///
/// <para><b>The lie this file pins against.</b> "TimescaleDB is detected at startup" was true and read as
/// harmless, because the extension cannot appear or vanish under a running service. What the flag actually
/// records is whether one detection ATTEMPT succeeded, and the start-path block force-clears it on any fault
/// — a store restart, a failover, a lock held elsewhere, a momentary connection failure. Nothing re-decided
/// it, so a moment's trouble at start ran a genuinely TimescaleDB store in plain-PostgreSQL mode for the life
/// of a service designed to run for months.</para>
///
/// <para><b>And the ordering trap that makes this a one-line fix with a wrong answer.</b> The hourly store
/// background-job health check is GATED on that latch, and that check is the entire reporting surface for the
/// condition: the compression-job self-heal (#1581 — the field incident where uncompressed data grew until
/// the disk filled and collection stopped for the whole fleet), Store Job Over Cadence (#2136) and Retention
/// Held (#2813). A re-probe written inside that gate is unreachable in exactly the state it exists for: the
/// false value suppresses its own correction, and the service is degraded with its backstop off and no way to
/// say so. So the tick's outer guard is the due time alone, the probe runs first, and the flag gate moved one
/// level in behind it.</para>
///
/// <para><b>Why the pins here are source-parsed.</b> The tick is a private block on a loop that needs a host,
/// a store and a clock to drive, and what regresses is a call site written the wrong way round — which is
/// textual. <see cref="EnclosingGuards"/> answers the reachability question structurally rather than by
/// matching a literal: it reconstructs the <c>if</c> conditions whose braces enclose a statement, over
/// <see cref="CSharpSourceWalker.StripCommentsAndStrings"/>'s output so the prose around the block (which
/// names the flag repeatedly) is not mistaken for a guard. Its own correctness is asserted in the same pins
/// rather than assumed — the compression read, which IS behind the flag, is the positive control, and a walk
/// that found nothing would fail there instead of passing everywhere.</para>
/// </summary>
public sealed class TimescaleAvailabilityReprobeTests
{
    private const string ReprobeCall = "await ReprobeTimescaleAvailabilityAsync(stoppingToken);";
    private const string CompressionCall = "await EvaluateCompressionJobHealthAsync(stoppingToken);";
    private const string RetentionCall = "await ReevaluateRetentionPoliciesAsync(stoppingToken);";
    private const string ConvergenceCall = "await ConvergeStoreObjectsAsync(stoppingToken);";
    private const string TickGuard = "if (DateTime.UtcNow >= _nextCompressionCheckUtc)";
    private const string Stamp = "_nextCompressionCheckUtc = TimescaleSupport.NextCompressionCheckUtc(DateTime.UtcNow, s_compressionCheckInterval);";
    private const string Latch = "_timescaleAvailable";
    private const string ReprobeSignature = "private async Task ReprobeTimescaleAvailabilityAsync(CancellationToken cancellationToken)";

    /// <summary>
    /// THE ordering pin. No <c>if</c> whose braces enclose the re-probe call tests the latch positively, so
    /// the correction is reachable while the latch reads <c>false</c> — and the same walk, run on the
    /// compression read, DOES find such a guard, which is what makes the first claim a measurement rather
    /// than the walk failing to look.
    ///
    /// <para>Three failure routes are covered, because the obvious one is not the only one. The re-probe
    /// moved inside <c>if (_timescaleAvailable)</c> fails the guard scan. The re-probe left where it is but
    /// written as the body of a BRACELESS <c>if (_timescaleAvailable)</c> would sit in no block at all, so
    /// the guard scan would find nothing to object to — the preceding-token check is what closes that, since
    /// a statement governed by a braceless guard is preceded by <c>)</c> rather than by <c>{</c>, <c>;</c> or
    /// <c>}</c>. And a walk that silently returned an empty list for everything would satisfy both; the
    /// positive control and the two guards the probe is REQUIRED to be under close that one.</para>
    /// </summary>
    [Fact]
    public void TheReprobe_IsNotReachableOnlyFromBehindTheLatchItCorrects()
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(ReadWorkerSource());

        var reprobeAt = code.IndexOf(ReprobeCall, StringComparison.Ordinal);
        Assert.True(reprobeAt > 0, "could not find the re-probe call site in DarlingWorker - this pin cannot silently pass on a parse miss");
        Assert.Equal(1, CountOf(code, ReprobeCall));

        var reprobeGuards = EnclosingGuards(code, reprobeAt);

        /* INSTRUMENT FIRST. The probe is required to be under exactly two conditions of its own - the tick's
           due time, and the negated latch - so a walk that returned nothing, or that lost the innermost
           frame, fails here rather than reporting the absence of a positive guard it never looked for. */
        Assert.True(
            reprobeGuards.Any(g => g.Contains("_nextCompressionCheckUtc", StringComparison.Ordinal)),
            "the guard walk did not find the store-maintenance tick's due-time condition around the re-probe. "
          + "Either the probe has been given a cadence of its own - which is a second timer on a tick that "
          + $"exists to hold them - or the walk is not reading this block. Guards seen: {Describe(reprobeGuards)}");

        Assert.True(
            reprobeGuards.Any(g => g.Replace(" ", string.Empty, StringComparison.Ordinal).Contains("!" + Latch, StringComparison.Ordinal)),
            "the re-probe is no longer guarded on the latch being FALSE, so a store already in hypertable mode "
          + $"pays a CREATE EXTENSION every hour for nothing. Guards seen: {Describe(reprobeGuards)}");

        /* THE CLAIM. Not one enclosing condition may require the latch to be TRUE, or the correction is
           unreachable in the state it corrects. */
        var positive = reprobeGuards.Where(TestsTheLatchPositively).ToArray();
        Assert.True(
            positive.Length == 0,
            "the re-probe sits inside a guard that requires _timescaleAvailable to already be true, so a "
          + "falsely-degraded service can never correct itself and the store-job health check stays off for "
          + $"the life of the process (#3815): {Describe(positive)}");

        /* The braceless route: a statement governed by `if (cond)` with no braces is preceded by the `)` that
           closes that condition, and sits in no block the walk above can see. */
        var preceding = code[..reprobeAt].TrimEnd();
        var precedingToken = preceding.Length == 0 ? '\0' : preceding[^1];
        Assert.True(
            precedingToken is '{' or ';' or '}',
            $"the re-probe call is preceded by '{precedingToken}', which means it is the body of a braceless "
          + "guard rather than a statement in a block - the guard scan above cannot see such a condition, so "
          + "this is the shape that would defeat it");

        /* POSITIVE CONTROL for the walk and for TestsTheLatchPositively together: the compression read is
           behind the flag by design, and the same two helpers must say so. */
        var compressionAt = code.IndexOf(CompressionCall, StringComparison.Ordinal);
        Assert.True(compressionAt > 0, "could not find the compression-job health check's call site");
        var compressionGuards = EnclosingGuards(code, compressionAt);
        Assert.True(
            compressionGuards.Any(TestsTheLatchPositively),
            "the compression-job health check is no longer behind the availability latch. Either the gate has "
          + "gone - in which case a plain-PostgreSQL store now runs a TimescaleDB-only catalog read every hour "
          + "- or this file's guard walk sees nothing, in which case the assertion above passed for the wrong "
          + $"reason. Guards seen: {Describe(compressionGuards)}");
    }

    /// <summary>
    /// The cadence is the tick's, not a second timer, and the due time advances whatever the latch says.
    ///
    /// <para>The stamp's position is the half that is easy to get wrong and impossible to see afterwards.
    /// Left behind the flag gate, the due time on a store whose latch reads <c>false</c> never moves, so the
    /// probe fires on every sweep pass - once every fifteen seconds, forever, on the one store shape that can
    /// never benefit from it. Hoisted above the gate it is stamped once an hour on every store, which is what
    /// makes "hourly is ample" true rather than aspirational.</para>
    /// </summary>
    [Fact]
    public void TheTick_StampsItsDueTimeWhateverTheLatchSays_AndTheProbeRunsAheadOfTheGatedHalf()
    {
        var raw = ReadWorkerSource();
        var code = CSharpSourceWalker.StripCommentsAndStrings(raw);

        /* One tick, one stamp, one of each tenant - the block's own contract, and the thing a second cadence
           would break without breaking anything else. */
        Assert.Equal(1, CountOf(code, TickGuard));
        Assert.Equal(1, CountOf(code, Stamp));
        Assert.Equal(1, CountOf(code, CompressionCall));
        Assert.Equal(1, CountOf(code, RetentionCall));
        /* #3817's store-object convergence is the fourth tenant, and the count is here for the same reason
           the other three are: a second call site is a second pass per hour. */
        Assert.Equal(1, CountOf(code, ConvergenceCall));

        /* The latch is gone from the tick's OUTER condition; it now sits one level in. */
        Assert.DoesNotContain(Latch + " && DateTime.UtcNow >= _nextCompressionCheckUtc", code, StringComparison.Ordinal);

        var guardAt = code.IndexOf(TickGuard, StringComparison.Ordinal);
        var stampAt = code.IndexOf(Stamp, StringComparison.Ordinal);
        var reprobeAt = code.IndexOf(ReprobeCall, StringComparison.Ordinal);
        var compressionAt = code.IndexOf(CompressionCall, StringComparison.Ordinal);
        var retentionAt = code.IndexOf(RetentionCall, StringComparison.Ordinal);
        var convergenceAt = code.IndexOf(ConvergenceCall, StringComparison.Ordinal);
        Assert.True(
            guardAt > 0 && stampAt > guardAt && reprobeAt > stampAt && compressionAt > reprobeAt && retentionAt > compressionAt
            && convergenceAt > retentionAt,
            "the tick runs: due-time guard, stamp, availability re-probe, compression read, retention pass, "
          + "store-object convergence - in that order (got "
          + $"{guardAt}, {stampAt}, {reprobeAt}, {compressionAt}, {retentionAt}, {convergenceAt})");

        /* And the stamp itself is not behind the latch, which is the cadence claim proper. The instrument
           check comes first for the reason it does above: "no guard requires the latch" is satisfied just as
           well by a walk that found no guards at all, and the stamp is REQUIRED to be inside the tick's own
           due-time condition, so a walk that cannot see that one cannot be trusted about the absence of the
           other. */
        var stampGuards = EnclosingGuards(code, stampAt);
        Assert.True(
            stampGuards.Any(g => g.Contains("_nextCompressionCheckUtc", StringComparison.Ordinal)),
            "the guard walk did not find the tick's due-time condition around the stamp, so the assertion "
          + $"below would pass on an empty list: {Describe(stampGuards)}");
        Assert.True(
            !stampGuards.Any(TestsTheLatchPositively),
            "the due time is stamped forward from inside a guard that requires _timescaleAvailable, so on a "
          + "store in plain-PostgreSQL mode it never advances and the re-probe fires on every 15-second sweep "
          + $"pass instead of hourly: {Describe(stampGuards)}");

        /* The flag gate is still there, between the probe and the two tenants that need it. Searched FROM the
           stamp: the start-path block carries the same text thousands of lines earlier. */
        var flagGateAt = code.IndexOf("if (" + Latch + ")", stampAt, StringComparison.Ordinal);
        Assert.True(flagGateAt > reprobeAt && flagGateAt < compressionAt,
            "the compression read, the retention pass and the store-object convergence must still be behind the "
          + "latch, one level in from the tick");
    }

    /// <summary>
    /// The method: a connection of its own, the #1922 discipline on it, one whole-pass budget derived from
    /// the tick's phase, three failure outcomes that read differently, and no rethrow.
    ///
    /// <para>The #1922 half is the one with teeth. <c>CREATE EXTENSION IF NOT EXISTS timescaledb</c>
    /// TERMINATES the backend when the library is on disk but absent from <c>shared_preload_libraries</c>,
    /// and <see cref="TimescaleSupport.TryEnableAsync"/> reports that as <c>false</c> like any other failure -
    /// so a caller that keeps using the connection fails with "Connection is not open" several frames away,
    /// naming the cause nowhere. The start path is safe by construction and
    /// <see cref="TimescaleEnableConnectionSafetyTests"/> pins it there; this is the second caller, on an
    /// hourly cadence rather than once a start, so the same property is pinned here.</para>
    /// </summary>
    [Fact]
    public void TheReprobeMethod_TakesItsOwnConnection_StopsUsingItOnAFalse_AndIsFailureIsolated()
    {
        var worker = ReadWorkerSource();
        var body = MethodBody(CSharpSourceWalker.StripCommentsAndStrings(worker), ReprobeSignature);
        Assert.False(string.IsNullOrEmpty(body), "could not locate ReprobeTimescaleAvailabilityAsync - this pin cannot silently pass on a parse miss");

        /* Its own connection out of the worker's pool: the start path's is scoped to the setup block and
           disposed with it, which is why this method exists rather than a line beside that call. */
        Assert.Contains("await using var connection = await _postgres!.OpenConnectionAsync(budget.Token);", body, StringComparison.Ordinal);

        /* #1922: the result is captured and nothing touches the connection after it. Asserted on the text
           AFTER the probe rather than on a count, because the property is about what follows the check. */
        const string Probe = "var available = await TimescaleSupport.TryEnableAsync(connection, null, budget.Token);";
        var probeAt = body.IndexOf(Probe, StringComparison.Ordinal);
        Assert.True(probeAt > 0, "the re-probe must call TryEnableAsync on its own connection with no logger of its own");
        Assert.DoesNotContain("connection", body[(probeAt + Probe.Length)..], StringComparison.Ordinal);

        /* And TryEnableAsync is the ONLY thing this method calls on TimescaleSupport. Since #3817 the setup
           sequence - conversion, compression, aggregates, tuning - runs on this same tick, but from its OWN
           tenant (ConvergeStoreObjectsAsync) on its OWN connection and budget, not from inside the probe:
           this method's whole job is the latch, and a second call here would re-introduce the #1922 shape on
           the one connection in the process most likely to have just been killed by a CREATE EXTENSION. */
        var touched = Regex.Matches(body, @"TimescaleSupport\.(\w+)").Select(m => m.Groups[1].Value).Distinct(StringComparer.Ordinal).ToArray();
        Assert.Equal(new[] { "TryEnableAsync" }, touched);

        /* One linked budget for the whole pass (#2327's shape), and its value DERIVED against the constant it
           was reasoned from rather than restated: the compression read behind this probe samples at
           CompressionCheckPhaseSeconds past the minute, half a grid step from the :MM:00 the policies fire
           on, so a probe awaited ahead of it must finish well inside that half-step. */
        Assert.Contains("using var budget = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);", body, StringComparison.Ordinal);
        Assert.Contains("budget.CancelAfter(s_timescaleReprobeBudget);", body, StringComparison.Ordinal);

        var declared = Regex.Match(worker, @"s_timescaleReprobeBudget = TimeSpan\.FromSeconds\((\d+)\);");
        Assert.True(declared.Success, "s_timescaleReprobeBudget is no longer declared in seconds - the derivation below cannot read it");
        var budgetSeconds = int.Parse(declared.Groups[1].Value, CultureInfo.InvariantCulture);
        Assert.True(
            budgetSeconds > 0 && budgetSeconds < TimescaleSupport.CompressionCheckPhaseSeconds,
            $"the re-probe budget is {budgetSeconds}s against a compression-check phase of "
          + $"{TimescaleSupport.CompressionCheckPhaseSeconds}s. A probe that can hold the tick for a whole "
          + "half-step walks the compression sample toward the :MM:00 instant the policies start on, which is "
          + "the false page #3575 removed");

        /* Three catches, in the order that makes each filter mean what it says, and nothing rethrown - the
           sweep loop must never see this pass fail. */
        var shutdownAt = body.IndexOf("catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)", StringComparison.Ordinal);
        var budgetAt = body.IndexOf("catch (OperationCanceledException) when (budget.IsCancellationRequested)", StringComparison.Ordinal);
        var otherAt = body.IndexOf("catch (Exception ex)", StringComparison.Ordinal);
        Assert.True(shutdownAt > 0 && budgetAt > shutdownAt && otherAt > budgetAt, "shutdown, then budget, then everything else");
        Assert.DoesNotContain("throw", body, StringComparison.Ordinal);
        Assert.Equal(2, CountOf(body, "_logger.LogWarning("));
    }

    /// <summary>
    /// The logging, which is half the deliverable: a service running falsely degraded says so once at its
    /// start and then looks entirely normal, so the RECOVERY has to be loud and the non-event has to be
    /// quiet.
    ///
    /// <para>Loud means Information, because an operator reading the log needs to see that the service left
    /// plain-PostgreSQL mode without anyone restarting it. Quiet means Debug, because on a store that is
    /// genuinely plain PostgreSQL - a fully supported configuration - the unchanged pass happens every hour
    /// forever; that is also why the probe hands
    /// <see cref="TimescaleSupport.TryEnableAsync"/> a null logger, since that method writes its own
    /// Information line on EVERY outcome and would otherwise put one in the log an hour for the life of the
    /// service.</para>
    ///
    /// <para>The issue numbers in the recovery line are checked against the health check they name rather
    /// than trusted: the method the latch gates calls exactly four self-alert evaluators (#4299 added the
    /// fourth), so a fifth arriving makes the line an undercount and reds here.</para>
    /// </summary>
    [Fact]
    public void TheRecoveryIsInformation_TheUnchangedPassIsDebug_AndTheLineNamesWhatItRestores()
    {
        var worker = ReadWorkerSource();
        var body = MethodBody(CSharpSourceWalker.StripCommentsAndStrings(worker), ReprobeSignature);
        Assert.False(string.IsNullOrEmpty(body));

        /* Exactly one of each: a second Information line would put the hourly no-op back in the log. */
        Assert.Equal(1, CountOf(body, "_logger.LogInformation("));
        Assert.Equal(1, CountOf(body, "_logger.LogDebug("));

        /* The literals live in the raw source - StripCommentsAndStrings blanks them by design - so the level
           is read off the stripped body and the text off the raw one, anchored on the same method. */
        var rawBody = MethodBody(worker, ReprobeSignature);
        const string Recovery = "\"TimescaleDB is available after all - ";
        const string Unchanged = "\"TimescaleDB re-probe after {ElapsedMs} ms: still unavailable,";
        AssertLoggedAt("_logger.LogInformation(", rawBody, Recovery);
        AssertLoggedAt("_logger.LogDebug(", rawBody, Unchanged);

        /* The flip happens, and it happens before the line that announces it. */
        var flipAt = body.IndexOf(Latch + " = true;", StringComparison.Ordinal);
        var informationAt = body.IndexOf("_logger.LogInformation(", StringComparison.Ordinal);
        Assert.True(flipAt > 0 && informationAt > flipAt, "the latch flips, and the Information line follows the flip");

        /* And no logger is handed down, or TryEnableAsync writes its own Information line every hour. */
        Assert.Contains("TryEnableAsync(connection, null,", body, StringComparison.Ordinal);

        /* The line's claim about WHAT comes back, checked against the method that comes back. Counted by
           enumerating the self-alert evaluator calls in the gated method's own body - four today (#4299 added
           the raw-purge-over-horizon evaluator to this same gated check) - and the line has to name one issue
           per evaluator. */
        var gated = MethodBody(CSharpSourceWalker.StripCommentsAndStrings(worker), "private async Task EvaluateCompressionJobHealthAsync(CancellationToken cancellationToken)");
        Assert.False(string.IsNullOrEmpty(gated));
        var evaluators = CountOf(gated, "_selfAlerts!.Evaluate");
        var named = Regex.Matches(rawBody[rawBody.IndexOf(Recovery, StringComparison.Ordinal)..], @"\(#\d{4}\)").Count;
        Assert.True(
            evaluators == 4 && named == evaluators + 2,
            $"the health check the latch gates runs {evaluators} self-alert evaluators and the recovery line "
          + $"names {named - 2} of them beside its own issue number and the convergence tenant's. The line tells "
          + "an operator what has been off since the start; a check that gained an evaluator without the line "
          + "gaining its number leaves them believing less was lost than was");

        /* The two numbers that are NOT an evaluator's, named so the arithmetic above cannot be satisfied by
           any two: the probe's own issue, and #3817's convergence pass — which is what the line now points
           an operator at in place of the "still applied on the start path only" sentence it used to carry. */
        Assert.Contains("(#3815)", rawBody, StringComparison.Ordinal);
        Assert.Contains("(#3817)", rawBody, StringComparison.Ordinal);

        /* The sentence the line must not lose: that no restart was needed. Its former companion - "hypertable
           conversion, compression policies, continuous aggregates and retention policies are still applied on
           the start path only" - was TRUE when #3815 shipped and is now FALSE: #3817 put the convergence pass
           on this same tick, immediately behind the latch this probe just flipped, so the recovery no longer
           leaves anything for the next start. The banned phrase is asserted absent rather than quietly
           deleted, because that sentence was load-bearing for a real operator decision (restart now, or
           wait) and its removal is the deliverable of the issue that removed it. */
        Assert.Contains("WITHOUT a restart (#3815)", rawBody, StringComparison.Ordinal);
        Assert.DoesNotContain("are still applied on the start path only", rawBody, StringComparison.Ordinal);
        Assert.Contains("#3817", rawBody, StringComparison.Ordinal);
    }

    /// <summary>The <c>if</c> conditions whose braces enclose <paramref name="index"/>, outermost first.
    /// Built over stripped source, so a brace inside a literal or a comment cannot desynchronise the walk and
    /// prose naming the latch cannot be read as a guard. Conditions belonging to <c>while</c>, <c>foreach</c>,
    /// <c>using</c> and the like are dropped: this asks which DECISIONS a statement is behind.</summary>
    private static IReadOnlyList<string> EnclosingGuards(string code, int index)
    {
        var open = new List<string?>();
        for (var i = 0; i < index; i++)
        {
            if (code[i] == '{')
            {
                open.Add(ConditionBefore(code, i));
            }
            else if (code[i] == '}' && open.Count > 0)
            {
                open.RemoveAt(open.Count - 1);
            }
        }

        return open.Where(c => c is not null).Select(c => c!).ToList();
    }

    /// <summary>The <c>if</c> condition immediately before the brace at <paramref name="braceAt"/>, or null
    /// when that brace opens anything else. Walks the parenthesis nesting backwards so a condition carrying
    /// calls of its own is taken whole.</summary>
    private static string? ConditionBefore(string code, int braceAt)
    {
        var i = braceAt - 1;
        while (i >= 0 && char.IsWhiteSpace(code[i]))
        {
            i--;
        }

        if (i < 0 || code[i] != ')')
        {
            return null;
        }

        var closeAt = i;
        var depth = 0;
        for (; i >= 0; i--)
        {
            if (code[i] == ')')
            {
                depth++;
            }
            else if (code[i] == '(')
            {
                depth--;
                if (depth == 0)
                {
                    break;
                }
            }
        }

        if (i < 0)
        {
            return null;
        }

        var openAt = i;
        var j = openAt - 1;
        while (j >= 0 && char.IsWhiteSpace(code[j]))
        {
            j--;
        }

        var end = j + 1;
        while (j >= 0 && (char.IsLetterOrDigit(code[j]) || code[j] == '_'))
        {
            j--;
        }

        return code[(j + 1)..end] == "if" ? code[(openAt + 1)..closeAt] : null;
    }

    /// <summary>Whether <paramref name="condition"/> reads the latch in a way that requires it to be true:
    /// any mention of the field whose nearest preceding non-space character is not <c>!</c>.</summary>
    private static bool TestsTheLatchPositively(string condition)
    {
        for (var at = condition.IndexOf(Latch, StringComparison.Ordinal); at >= 0; at = condition.IndexOf(Latch, at + Latch.Length, StringComparison.Ordinal))
        {
            var i = at - 1;
            while (i >= 0 && char.IsWhiteSpace(condition[i]))
            {
                i--;
            }

            if (i < 0 || condition[i] != '!')
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>The nearest <c>_logger.Log*</c> before <paramref name="literal"/> is the expected one - the
    /// RetentionReevaluationTests idiom for reading a line's LEVEL off the source.</summary>
    private static void AssertLoggedAt(string expected, string body, string literal)
    {
        var at = body.IndexOf(literal, StringComparison.Ordinal);
        Assert.True(at > 0, $"the re-probe no longer writes a line starting {literal}");
        var callAt = body.LastIndexOf("_logger.Log", at, StringComparison.Ordinal);
        Assert.True(callAt > 0, $"no _logger.Log call precedes {literal}");
        Assert.StartsWith(expected, body[callAt..], StringComparison.Ordinal);
    }

    private static string Describe(IEnumerable<string> guards)
    {
        var listed = guards.Select(g => "`" + Regex.Replace(g, @"\s+", " ").Trim() + "`").ToArray();
        return listed.Length == 0 ? "(none)" : string.Join(", ", listed);
    }

    private static int CountOf(string text, string needle)
    {
        var count = 0;
        for (var at = text.IndexOf(needle, StringComparison.Ordinal); at >= 0; at = text.IndexOf(needle, at + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }

    /// <summary>A method's body from its signature to the brace that closes it, at nesting depth zero. Empty
    /// when the signature is not found, so a caller can FAIL rather than silently pass on a parse miss.</summary>
    private static string MethodBody(string source, string signature)
    {
        var start = source.IndexOf(signature, StringComparison.Ordinal);
        if (start < 0)
        {
            return string.Empty;
        }

        var open = source.IndexOf('{', start);
        if (open < 0)
        {
            return string.Empty;
        }

        var depth = 0;
        for (var i = open; i < source.Length; i++)
        {
            if (source[i] == '{')
            {
                depth++;
            }
            else if (source[i] == '}')
            {
                depth--;
                if (depth == 0)
                {
                    return source[start..(i + 1)];
                }
            }
        }

        return string.Empty;
    }

    /// <summary>Every anchor read off this file sits on one line (the CI checkout is CRLF).</summary>
    private static string ReadWorkerSource() =>
        RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
}
