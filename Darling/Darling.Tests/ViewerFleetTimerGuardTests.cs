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
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The viewer's two fleet timers must not be able to stack store reads (#2907).
///
/// <para><b>Why this is a SOURCE pin and not a behavioural test.</b> The defect was never in any method's
/// logic: <c>OnRefreshTimerTick</c> fired <c>RefreshServerStatusAsync</c> and <c>RefreshStoreSizeAsync</c>
/// unawaited while its two siblings in the same fan-out were both guarded, and <c>OnOverviewTimerTick</c>
/// fired one of them a second time at the same interval. Every one of those methods was individually
/// correct. What was wrong is that a fan-out site and a guard were written in two different places with
/// nothing tying them together — the seam, not the arithmetic. There is also nowhere to stand a behavioural
/// test up: this is <c>MainWindow</c> code-behind on a <c>DispatcherTimer</c>, with no seam to instantiate
/// and no injectable clock, which is precisely how the gap survived #2901's review of the same methods.</para>
///
/// <para><b>The invariant, stated once:</b> no path from a fleet-timer fan-out target to a
/// <c>_dataService</c> read may exist without a single-flight guard on it. That is a property of the shipped
/// source, so it is checked by walking the shipped source — and the walk STOPS at the first guard it finds,
/// which is what lets it be honest about a guard that legitimately lives one level down (the AG probe's
/// <c>_loading</c> is inside <c>AvailabilityGroupsTab.LoadAsync</c>, not in <c>MainWindow</c>).</para>
///
/// <para><b>The guard is matched by SHAPE, never by field name.</b> The three that predate this pin are
/// <c>_refreshInFlight</c>, <c>_alertPollInFlight</c> and <c>_loading</c>, so a name pattern would already
/// have needed an exception on the day it landed. The shape is the actual contract: the field is TESTED,
/// then claimed with <c>&lt;field&gt; = true</c>, with a bail-out <c>return</c>, all three before the body's
/// first <c>await</c> — a flag set after the first suspension point cannot exclude a second entrant, and one
/// that is only ever written is a flag rather than a gate.</para>
///
/// <para><b>Stacking and leaking are checked separately, and that split is load-bearing.</b> Releasing the
/// guard in a <c>finally</c> is a second requirement with an opposite failure mode — a guard released only
/// on the success path does not stack, it wedges the refresh for the lifetime of the window the first time a
/// read throws, which #2901's deadline makes routine rather than hypothetical. Folding the release into the
/// stacking predicate looked tidier and was wrong twice over: it made the release assertion unreachable
/// (it only ever saw bodies that had already satisfied the release check) and it pointed a leaked guard at
/// the stacking message, sending the reader to look for a guard that is sitting right there.</para>
///
/// <para>Proven red before it was proven green, six ways, each mutation failing exactly one assertion with
/// the message that names its own fix: dropping either new guard makes the walk descend past it and report
/// the read one level down (<c>RefreshServerStatusAsync -&gt; ReadAndApplyServerStatusAsync</c> — the case a
/// naive "does this method touch <c>_dataService</c>" check would have excused, since the fix moved that
/// read into a helper); moving the <c>= false</c> reset out of its <c>finally</c> fails the release
/// assertion; putting <c>RefreshServerStatusAsync</c> back on the Overview tick fails the double-fire
/// assertion; having a fleet timer pass <c>replayIfBusy</c> fails the replay assertion; and stripping
/// <c>AvailabilityGroupsTab</c>'s <c>_loading</c> reports
/// <c>RefreshAvailabilityGroupsAsync -&gt; RefreshAgAsync -&gt; LoadAsync</c>.</para>
///
/// <para><b>That last mutation is why the walk follows ANY invocation and not just the fan-out's two
/// shapes.</b> It was added to confirm a claim this comment was already making — that the walk reaches a
/// guard living one level down — and it did not fail. <c>RefreshAgAsync</c> is <c>=&gt; LoadAsync();</c>, a
/// plain call in an expression body, matching neither <c>_ = X()</c> nor <c>await X()</c>, so the walk had
/// been running out of edges there and calling the AG path safe WITHOUT ever reaching the <c>_loading</c>
/// that makes it safe. The assertion was passing for the wrong reason, which is worth less than a failure,
/// and only a mutation aimed at the claim rather than at the fix could tell the two apart.</para>
/// </summary>
public sealed class ViewerFleetTimerGuardTests
{
    /// <summary>The two <c>DispatcherTimer</c> ticks that run at <c>NocRefreshIntervalSeconds</c>.</summary>
    private static readonly string[] s_fleetTimerTicks = ["OnRefreshTimerTick", "OnOverviewTimerTick"];

    /// <summary>A fire-and-forget call — <c>_ = SomethingAsync(...)</c>, the shape with no caller to await it
    /// and so the shape that can overlap itself.</summary>
    private static readonly Regex s_fireAndForget = new(
        @"_\s*=\s*(\w+)\s*\(",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex s_awaited = new(
        @"await\s+(\w+)\s*\(",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>Any invocation, whatever syntax called it — see <see cref="CalledNames"/> for why the walk
    /// needs this and not just the two fan-out shapes.</summary>
    private static readonly Regex s_invocation = new(
        @"\b(\w+)\s*\(",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>A store read: any member access on the viewer's data service.</summary>
    private static readonly Regex s_storeRead = new(
        @"_dataService\s*\??\s*\.",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>Sets a bool field to true — the guard's claim half.</summary>
    private static readonly Regex s_claimsGuard = new(
        @"(_\w+)\s*=\s*true\s*;",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    [Fact]
    public void EveryFleetTimerFanOutTarget_IsSingleFlightOnEveryPathToAStoreRead()
    {
        var methods = ViewerMethods();
        var targets = FleetTimerFanOutTargets(methods);

        /* Floor, not an equality: the pin is "whatever these ticks fan out to is guarded", and the tick
           bodies are allowed to change. Four at the time this landed (status, store size, alert poll, AG
           probe) plus the awaited visible-tab refresh. A zero here means the tick bodies were not parsed. */
        Assert.True(
            targets.Count >= 4,
            $"only {targets.Count} fan-out target(s) parsed out of the fleet timer ticks — the sweep is not reading MainWindow");

        var offenders = new List<string>();

        foreach (var target in targets.OrderBy(t => t, StringComparer.Ordinal))
        {
            var path = UnguardedPathToStoreRead(methods, target, new HashSet<string>(StringComparer.Ordinal));

            if (path is not null)
            {
                offenders.Add(path);
            }
        }

        Assert.True(
            offenders.Count == 0,
            "the fleet timers can stack store reads — an unguarded path from a fan-out target to a "
            + $"_dataService read: {string.Join("; ", offenders)}");
    }

    /// <summary>
    /// Every guard the fan-out relies on must be released in a <c>finally</c>. A guard released on the
    /// success path only survives every test that does not throw, and then wedges its refresh for the
    /// lifetime of the window the first time the store errors mid-read — which the deadline #2901 added
    /// makes a routine event rather than a hypothetical one.
    /// </summary>
    [Fact]
    public void EveryFleetTimerGuard_IsReleasedInAFinally()
    {
        var methods = ViewerMethods();
        var checked_ = 0;
        var offenders = new List<string>();

        foreach (var (name, body) in GuardedMethodsReachableFromTheFleetTimers(methods))
        {
            checked_++;

            var field = GuardField(body);

            /* The walk only yields bodies that claimed a guard, so a null here would mean the two readings
               of "claims a guard" disagree — worth failing on rather than skipping. */
            if (field is null)
            {
                offenders.Add($"{name} (no guard field resolvable)");
                continue;
            }

            var released = FinallyBlocks(body)
                .Any(f => Regex.IsMatch(f, @"\b" + Regex.Escape(field) + @"\s*=\s*false\s*;"));

            if (!released)
            {
                offenders.Add($"{name} ({field} is not reset in a finally)");
            }
        }

        Assert.True(checked_ >= 3, $"only {checked_} guarded method(s) found on the fan-out — the sweep is not reading the project");

        Assert.True(
            offenders.Count == 0,
            $"guard(s) that a throwing read would leave latched forever: {string.Join("; ", offenders)}");
    }

    /// <summary>
    /// The two fleet timers run at the SAME interval, and <c>OnRefreshTimerTick</c>'s tab early-return sits
    /// BELOW its fan-out — so anything both ticks fire runs twice per cycle, concurrently, forever. That is
    /// how <c>RefreshServerStatusAsync</c> came to issue two freshness read-pairs per Overview cycle, on the
    /// one tab that ships selected.
    ///
    /// <para>Compared against the UNCONDITIONAL region of the refresh tick — everything above its first
    /// <c>return</c> — because that is the part no tab selection can skip. <c>RefreshVisibleAsync</c> lives
    /// below that return and is therefore mutually exclusive with the Overview tick by construction, which
    /// is exactly the distinction the "never double-refresh the same grid" comment was making and the
    /// reason it was true of the grid while being false of these reads.</para>
    /// </summary>
    [Fact]
    public void NoStoreRead_IsFiredByBothFleetTimers()
    {
        var methods = ViewerMethods();

        var refreshTick = Body(methods, "OnRefreshTimerTick");
        var overviewTick = Body(methods, "OnOverviewTimerTick");

        /* Everything above the first return is what every tick runs regardless of the visible tab. */
        var firstReturn = Regex.Match(refreshTick, @"\breturn\s*;");
        Assert.True(firstReturn.Success, "OnRefreshTimerTick no longer has a tab early-return — this pin's split is stale");

        var unconditional = refreshTick[..firstReturn.Index];

        var both = FiredNames(unconditional).Intersect(FiredNames(overviewTick), StringComparer.Ordinal)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToList();

        Assert.True(
            both.Count == 0,
            "fired by BOTH fleet timers at the same interval, so it runs twice per cycle: "
            + string.Join(", ", both));
    }

    /// <summary>
    /// <c>RefreshServerStatusAsync</c>'s replay is opt-in per call site, and the split is the whole point:
    /// a PERIODIC caller must drop, because the next tick is already its retry and replaying one would
    /// remove the interval's gap exactly when the store is slowest. Only the connect/reload path may ask
    /// for a replay, because a freshness dictionary fetched before an add/edit/remove has no entry for the
    /// new server and paints its dot as never-collected.
    ///
    /// <para>This is the assertion most likely to be "simplified" away — a reader who sees one caller pass
    /// the flag will reasonably wonder why they all do not.</para>
    /// </summary>
    [Fact]
    public void OnlyANonPeriodicCaller_AsksTheStatusRefreshToReplay()
    {
        var methods = ViewerMethods();
        var shell = StrippedSource(ShellFiles().Single(f => Path.GetFileName(f) == "MainWindow.xaml.cs"));

        var tickSpans = s_fleetTimerTicks
            .Select(t => SpanOf(shell, t))
            .ToList();

        var replayingTimerCallers = new List<string>();
        var replayingCallers = 0;
        var callSites = 0;

        foreach (Match call in Regex.Matches(shell, @"RefreshServerStatusAsync\s*\(([^)]*)\)"))
        {
            callSites++;

            var asksForReplay = call.Groups[1].Value.Contains("replayIfBusy", StringComparison.Ordinal);

            if (asksForReplay)
            {
                replayingCallers++;
            }

            var tick = tickSpans.FirstOrDefault(s => call.Index >= s.Start && call.Index < s.End);

            if (asksForReplay && tick.Name is not null)
            {
                replayingTimerCallers.Add(tick.Name);
            }
        }

        Assert.True(callSites >= 2, $"only {callSites} RefreshServerStatusAsync call site(s) parsed — the sweep is not reading MainWindow");

        Assert.True(
            replayingTimerCallers.Count == 0,
            "a fleet timer asks RefreshServerStatusAsync to replay, which turns a slow store into "
            + $"back-to-back reads instead of leaving the interval's gap: {string.Join(", ", replayingTimerCallers)}");

        /* And the state-changing caller must still be asking, or a just-added server's dot stays wrong for
           a whole interval — the reason the drop is opt-out rather than universal. */
        Assert.True(
            replayingCallers >= 1,
            "no caller asks RefreshServerStatusAsync to replay; the connect/reload path needs it, because a "
            + "freshness read that started before an add/edit/remove has no entry for the new server");

        /* Guard against BOTH sides being wrong the same way: the method must still offer the parameter. */
        Assert.True(
            Body(methods, "RefreshServerStatusAsync").Contains("replayIfBusy", StringComparison.Ordinal),
            "RefreshServerStatusAsync no longer reads replayIfBusy, so the opt-in replay is dead code");
    }

    // ── The walk ──────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// The first unguarded path from <paramref name="name"/> to a <c>_dataService</c> read, or null when
    /// every path is guarded. Returns the PATH rather than a bool so a failure names the call chain instead
    /// of just the entry point — the fix moved one read into a helper, so the entry point alone would not
    /// say where to look.
    ///
    /// <para>Stops at the first guard on a path, which is what makes a guard one level down legitimate
    /// rather than invisible. Callees resolve in the declaring FILE first and then to a project-wide unique
    /// declaration, because <c>LoadAsync</c> is not a unique name in this project while
    /// <c>AvailabilityGroupsTab</c>'s own <c>LoadAsync</c> is unambiguous from inside that file.</para>
    /// </summary>
    private static string? UnguardedPathToStoreRead(
        IReadOnlyDictionary<string, List<ViewerMethod>> methods,
        string name,
        HashSet<string> visiting)
    {
        if (!visiting.Add(name))
        {
            return null; /* A cycle re-enters through a call already on the stack; it adds no new path. */
        }

        try
        {
            if (!methods.TryGetValue(name, out var declarations))
            {
                /* Not declared in the viewer project (a framework or shared-library call). Nothing here can
                   reach _dataService, which is a private field of this project's own types. */
                return null;
            }

            foreach (var declaration in declarations)
            {
                if (IsSingleFlight(declaration.Body))
                {
                    continue; /* Guarded — no read below this can overlap itself through this path. */
                }

                if (s_storeRead.IsMatch(declaration.Body))
                {
                    return name;
                }

                foreach (var callee in CalledNames(declaration.Body).OrderBy(n => n, StringComparer.Ordinal))
                {
                    var resolved = Resolve(methods, callee, declaration.File);

                    if (resolved is null)
                    {
                        continue;
                    }

                    var deeper = UnguardedPathToStoreRead(resolved, callee, visiting);

                    if (deeper is not null)
                    {
                        return $"{name} -> {deeper}";
                    }
                }
            }

            return null;
        }
        finally
        {
            visiting.Remove(name);
        }
    }

    /// <summary>
    /// The declarations a callee name refers to from inside <paramref name="fromFile"/>: same-file first
    /// (which disambiguates the common <c>LoadAsync</c> / <c>RefreshAsync</c> names), otherwise the
    /// project-wide set — and when that set has several same-named declarations across files, ALL of them,
    /// so an ambiguous name is treated conservatively and over-reports rather than passing silently.
    /// </summary>
    private static IReadOnlyDictionary<string, List<ViewerMethod>>? Resolve(
        IReadOnlyDictionary<string, List<ViewerMethod>> methods,
        string callee,
        string fromFile)
    {
        if (!methods.TryGetValue(callee, out var all))
        {
            return null;
        }

        var sameFile = all.Where(m => string.Equals(m.File, fromFile, StringComparison.Ordinal)).ToList();

        return sameFile.Count > 0
            ? new Dictionary<string, List<ViewerMethod>>(StringComparer.Ordinal) { [callee] = sameFile }
            : methods;
    }

    private static IEnumerable<(string Name, string Body)> GuardedMethodsReachableFromTheFleetTimers(
        IReadOnlyDictionary<string, List<ViewerMethod>> methods)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var queue = new Queue<string>(FleetTimerFanOutTargets(methods));

        while (queue.Count > 0)
        {
            var name = queue.Dequeue();

            if (!seen.Add(name) || !methods.TryGetValue(name, out var declarations))
            {
                continue;
            }

            foreach (var declaration in declarations)
            {
                if (IsSingleFlight(declaration.Body))
                {
                    yield return (name, declaration.Body);
                    continue; /* The walk stops at a guard here too, mirroring UnguardedPathToStoreRead. */
                }

                foreach (var callee in CalledNames(declaration.Body))
                {
                    queue.Enqueue(callee);
                }
            }
        }
    }

    private static HashSet<string> FleetTimerFanOutTargets(IReadOnlyDictionary<string, List<ViewerMethod>> methods)
    {
        var targets = new HashSet<string>(StringComparer.Ordinal);

        foreach (var tick in s_fleetTimerTicks)
        {
            foreach (var name in FiredNames(Body(methods, tick)))
            {
                if (methods.ContainsKey(name))
                {
                    targets.Add(name);
                }
            }
        }

        return targets;
    }

    /// <summary>
    /// Every name this body invokes. Deliberately ANY <c>Identifier(</c> rather than only the two shapes
    /// the fan-out itself uses (<c>_ = X()</c> and <c>await X()</c>), because the walk has to follow
    /// delegation and delegation is not written in either shape: <c>RefreshAgAsync</c> is
    /// <c>=&gt; LoadAsync();</c>, a plain call in an expression body, and matching only the fan-out's two
    /// shapes made the walk run out of edges there and call the AG path safe WITHOUT ever reaching the
    /// <c>_loading</c> guard that actually makes it safe.
    ///
    /// <para>Over-matching is free here: keywords and framework calls are filtered out downstream by
    /// having to resolve to a method this project declares, and a name that does resolve is worth walking
    /// whichever syntax called it.</para>
    /// </summary>
    private static IEnumerable<string> CalledNames(string body) =>
        s_invocation.Matches(body).Select(m => m.Groups[1].Value).Distinct(StringComparer.Ordinal);

    /// <summary>Only the two shapes the fan-out itself uses, for deciding which methods a tick FIRES (as
    /// opposed to which methods a body reaches). A tick's fan-out is exactly its unawaited and awaited
    /// calls; the loop conditions and property reads around them are not fan-out.</summary>
    private static IEnumerable<string> FiredNames(string body) =>
        s_fireAndForget.Matches(body).Select(m => m.Groups[1].Value)
            .Concat(s_awaited.Matches(body).Select(m => m.Groups[1].Value))
            .Distinct(StringComparer.Ordinal);

    // ── Guard shape ───────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Whether a body excludes a second entrant — which is the CLAIM alone, deliberately, and not the
    /// release. <c>if (busy) return; busy = true;</c> stops the overlap whether or not the reset is in a
    /// <c>finally</c>; a missing <c>finally</c> is the opposite defect (the refresh wedges rather than
    /// stacks) and <see cref="EveryFleetTimerGuard_IsReleasedInAFinally"/> is what reports it.
    ///
    /// <para>Keeping the two apart is what makes both assertions load-bearing. Folding the release into
    /// this predicate made the release test unreachable — it only ever saw bodies that had already
    /// satisfied the release check — and pointed a leaked guard at the stacking message, which sends the
    /// reader looking for a guard that is right there.</para>
    /// </summary>
    private static bool IsSingleFlight(string body) => GuardField(body) is not null;

    /// <summary>
    /// The field a body claims as its single-flight guard, or null. Requires the claim AND a bail-out
    /// <c>return</c> ahead of the first <c>await</c>, and that the same field is TESTED before it is
    /// claimed — a bool that is only ever written is a flag, not a gate. Checking all of that before the
    /// first <c>await</c> is what separates a guard from an unrelated bool: a flag set after the first
    /// suspension point cannot exclude a second entrant.
    /// </summary>
    private static string? GuardField(string body)
    {
        var firstAwait = body.IndexOf("await", StringComparison.Ordinal);
        var prelude = firstAwait < 0 ? body : body[..firstAwait];

        if (!Regex.IsMatch(prelude, @"\breturn\s*;"))
        {
            return null;
        }

        foreach (Match claim in s_claimsGuard.Matches(prelude))
        {
            var field = claim.Groups[1].Value;

            /* Tested before it is claimed: the read has to sit ahead of the assignment in the prelude. */
            var test = Regex.Match(prelude[..claim.Index], @"\b" + Regex.Escape(field) + @"\b");

            if (test.Success)
            {
                return field;
            }
        }

        return null;
    }

    private static IEnumerable<string> FinallyBlocks(string body)
    {
        foreach (Match m in Regex.Matches(body, @"\bfinally\b"))
        {
            var open = body.IndexOf('{', m.Index);

            if (open < 0)
            {
                continue;
            }

            yield return BraceBalanced(body, open);
        }
    }

    // ── Source access ─────────────────────────────────────────────────────────────────────

    private readonly record struct ViewerMethod(string File, string Body);

    private static Dictionary<string, List<ViewerMethod>>? s_methods;

    /// <summary>
    /// Every method declared in the viewer project, by name, with comments and string literals stripped so
    /// the prose in this file's own subject matter cannot satisfy or break a match. Built once: the walk
    /// re-enters it per target.
    /// </summary>
    private static Dictionary<string, List<ViewerMethod>> ViewerMethods()
    {
        if (s_methods is not null)
        {
            return s_methods;
        }

        var methods = new Dictionary<string, List<ViewerMethod>>(StringComparer.Ordinal);
        var declaration = new Regex(
            @"\b(?:void|Task|Task\s*<[^>()]*>)\s+(\w+)\s*\(",
            RegexOptions.Compiled | RegexOptions.CultureInvariant);

        foreach (var path in ViewerSources())
        {
            var code = StrippedSource(path);

            foreach (Match m in declaration.Matches(code))
            {
                var body = BodyAfterSignature(code, m.Index + m.Length - 1);

                if (body is null)
                {
                    continue; /* An interface member, a delegate type, or a local declaration with no body. */
                }

                if (!methods.TryGetValue(m.Groups[1].Value, out var list))
                {
                    methods[m.Groups[1].Value] = list = [];
                }

                list.Add(new ViewerMethod(path, body));
            }
        }

        Assert.True(methods.Count >= 400, $"only {methods.Count} viewer method name(s) parsed — the sweep is not reading the project");

        return s_methods = methods;
    }

    /// <summary>
    /// The body that follows a signature whose open paren sits at <paramref name="openParen"/> — a
    /// brace-balanced block, or the expression of an expression-bodied member (<c>=&gt; Foo();</c>, which
    /// is how the AG probe's one-line delegation is written).
    /// </summary>
    private static string? BodyAfterSignature(string code, int openParen)
    {
        var depth = 0;
        var i = openParen;

        for (; i < code.Length; i++)
        {
            if (code[i] == '(')
            {
                depth++;
            }
            else if (code[i] == ')')
            {
                depth--;

                if (depth == 0)
                {
                    break;
                }
            }
        }

        if (i >= code.Length)
        {
            return null;
        }

        /* Skip whitespace and any constraint clause between the signature and the body. */
        var j = i + 1;

        while (j < code.Length && (char.IsWhiteSpace(code[j]) || code[j] == 'w'))
        {
            if (code[j] == 'w')
            {
                if (!code.AsSpan(j).StartsWith("where "))
                {
                    break;
                }

                var brace = code.IndexOfAny(['{', '=', ';'], j);

                if (brace < 0)
                {
                    return null;
                }

                j = brace;
                continue;
            }

            j++;
        }

        if (j >= code.Length)
        {
            return null;
        }

        if (code[j] == '{')
        {
            return BraceBalanced(code, j);
        }

        if (code[j] == '=' && j + 1 < code.Length && code[j + 1] == '>')
        {
            var end = code.IndexOf(';', j);

            return end < 0 ? null : code[(j + 2)..end];
        }

        return null; /* `;` — an abstract/partial/interface declaration with no body here. */
    }

    private static string BraceBalanced(string code, int open)
    {
        var depth = 0;

        for (var i = open; i < code.Length; i++)
        {
            if (code[i] == '{')
            {
                depth++;
            }
            else if (code[i] == '}')
            {
                depth--;

                if (depth == 0)
                {
                    return code[open..(i + 1)];
                }
            }
        }

        return code[open..];
    }

    private static string Body(IReadOnlyDictionary<string, List<ViewerMethod>> methods, string name)
    {
        Assert.True(methods.TryGetValue(name, out var declarations), $"{name} is no longer declared in the viewer project");
        Assert.True(declarations!.Count == 1, $"{name} has {declarations.Count} declarations; this pin assumes one");

        return declarations[0].Body;
    }

    /// <summary>The character span a named method occupies in one file's stripped source, for deciding
    /// whether a call site sits inside a timer tick.</summary>
    private static (string? Name, int Start, int End) SpanOf(string code, string name)
    {
        var m = Regex.Match(code, @"\b(?:void|Task|Task\s*<[^>()]*>)\s+" + Regex.Escape(name) + @"\s*\(");

        if (!m.Success)
        {
            return (null, 0, 0);
        }

        var body = BodyAfterSignature(code, m.Index + m.Length - 1);

        return body is null ? (null, 0, 0) : (name, m.Index, code.IndexOf(body, m.Index, StringComparison.Ordinal) + body.Length);
    }

    private static readonly Dictionary<string, string> s_stripped = new(StringComparer.Ordinal);

    private static string StrippedSource(string path)
    {
        if (s_stripped.TryGetValue(path, out var cached))
        {
            return cached;
        }

        return s_stripped[path] = StripCommentsAndStrings(File.ReadAllText(path));
    }

    private static IEnumerable<string> ShellFiles() =>
        ViewerSources().Where(p => Path.GetFileName(p).StartsWith("MainWindow", StringComparison.Ordinal));

    private static string[]? s_viewerSources;

    private static string[] ViewerSources()
    {
        if (s_viewerSources is not null)
        {
            return s_viewerSources;
        }

        var dir = Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Viewer");

        Assert.True(Directory.Exists(dir), $"viewer project directory not found: {dir}");

        var paths = Directory.EnumerateFiles(dir, "*.cs", SearchOption.AllDirectories)
            .Where(p => !IsBuildOutput(dir, p))
            .OrderBy(p => p, StringComparer.Ordinal)
            .ToArray();

        Assert.True(paths.Length >= 150, $"the viewer sweep found only {paths.Length} files — the project has moved");

        return s_viewerSources = paths;
    }

    /// <summary>True when a path sits under the project's <c>bin</c> or <c>obj</c> tree, compared as path
    /// SEGMENTS so a source file that merely has "obj" in its name is not excluded.</summary>
    private static bool IsBuildOutput(string projectDir, string path)
    {
        var relative = Path.GetRelativePath(projectDir, path);
        var segments = relative.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);

        return segments.Any(s =>
            string.Equals(s, "bin", StringComparison.OrdinalIgnoreCase)
            || string.Equals(s, "obj", StringComparison.OrdinalIgnoreCase));
    }

    private static string StripCommentsAndStrings(string text)
    {
        var sb = new StringBuilder(text.Length);
        var i = 0;

        while (i < text.Length)
        {
            var c = text[i];

            if (c == '@' && i + 1 < text.Length && text[i + 1] == '"')
            {
                var end = SkipVerbatimString(text, i + 2);
                Blank(sb, text, i, end);
                i = end;
                continue;
            }

            if (c == '"')
            {
                var end = SkipRegularString(text, i + 1);
                Blank(sb, text, i, end);
                i = end;
                continue;
            }

            if (c == '/' && i + 1 < text.Length && text[i + 1] == '/')
            {
                var nl = text.IndexOf('\n', i);
                var end = nl < 0 ? text.Length : nl;
                Blank(sb, text, i, end);
                i = end;
                continue;
            }

            if (c == '/' && i + 1 < text.Length && text[i + 1] == '*')
            {
                var close = text.IndexOf("*/", i + 2, StringComparison.Ordinal);
                var end = close < 0 ? text.Length : close + 2;
                Blank(sb, text, i, end);
                i = end;
                continue;
            }

            sb.Append(c);
            i++;
        }

        return sb.ToString();
    }

    private static void Blank(StringBuilder sb, string text, int start, int end)
    {
        for (var j = start; j < end; j++)
        {
            sb.Append(text[j] == '\n' ? '\n' : ' ');
        }
    }

    private static int SkipVerbatimString(string text, int i)
    {
        while (i < text.Length)
        {
            if (text[i] == '"')
            {
                if (i + 1 < text.Length && text[i + 1] == '"')
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

    private static int SkipRegularString(string text, int i)
    {
        while (i < text.Length)
        {
            var c = text[i];

            if (c == '\\')
            {
                i += 2;
                continue;
            }

            if (c == '"' || c == '\n')
            {
                return i + 1;
            }

            i++;
        }

        return text.Length;
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
