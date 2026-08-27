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
using System.Net;
using System.Runtime.CompilerServices;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Rejected HTTP requests get a rate-limited WARN naming the gate that refused them (#2479, item 5).
///
/// <para><b>The gap.</b> All five refusal sites across the MCP and web listeners were a bare
/// <c>StatusCode = …; return;</c>. So "is my token wrong, or my CIDR wrong?" was answerable only from the
/// client, which sees one opaque status code and cannot tell a Host-allowlist 400 from a malformed
/// request, or a CIDR 403 from anything else — while the person who can fix either is on the server, and
/// the server said nothing.</para>
///
/// <para><b>Why the policy is tested this hard.</b> The rate limit is not a nicety here. These ports are
/// LAN-exposed on purpose, so an unthrottled line per refusal makes the service log a denial-of-service
/// target: a scanner fills the file an operator debugs from, and the log stops being readable exactly when
/// it matters. Equally, a limiter tuned the other way is worthless — if a tester's own first refusal waits
/// out a window, the feature has not shipped. Both halves are measured below rather than asserted about.</para>
/// </summary>
public class DarlingHttpRefusalLogTests
{
    private static readonly DateTime T0 = new(2026, 8, 21, 12, 0, 0, DateTimeKind.Utc);
    private static readonly TimeSpan Window = TimeSpan.FromMinutes(10);

    /// <summary>
    /// The tester's own first refusal is never delayed. A limiter that makes the operator wait out a
    /// window before their answer appears has not fixed anything.
    /// </summary>
    [Fact]
    public void FirstSighting_LogsImmediately_ForEveryGate()
    {
        var log = new DarlingHttpRefusalLog(Window, 16);

        foreach (DarlingRefusalGate gate in Enum.GetValues<DarlingRefusalGate>())
        {
            Assert.True(log.Observe(gate, "10.0.0.5", T0).Log, $"the first {gate} refusal must be logged at once");
        }
    }

    /// <summary>
    /// Repeats fold, and the count rides out on the next line that does emit — so the volume is visible
    /// without being written. And the count resets once reported, or every later line over-reports.
    /// </summary>
    [Fact]
    public void RepeatsFold_AndTheSuppressedCountRidesTheNextLine()
    {
        var log = new DarlingHttpRefusalLog(Window, 16);
        Assert.True(log.Observe(DarlingRefusalGate.Token, "10.0.0.5", T0).Log);

        for (var i = 1; i <= 500; i++)
        {
            Assert.False(
                log.Observe(DarlingRefusalGate.Token, "10.0.0.5", T0.AddSeconds(i)).Log,
                "a repeat inside the window must not produce a line");
        }

        var next = log.Observe(DarlingRefusalGate.Token, "10.0.0.5", T0.Add(Window).AddSeconds(1));
        Assert.True(next.Log);
        Assert.Equal(500, next.SuppressedSinceLastLog);

        var after = log.Observe(DarlingRefusalGate.Token, "10.0.0.5", T0.Add(Window).AddSeconds(2));
        Assert.False(after.Log);
        Assert.Equal(0, after.SuppressedSinceLastLog);
    }

    /// <summary>
    /// A different gate is a different key, so iterating stays responsive: fix the token, now fail the
    /// CIDR check, and the new answer appears at once instead of behind the old gate's window.
    /// </summary>
    [Fact]
    public void ADifferentGate_FromTheSameSource_LogsAtOnce()
    {
        var log = new DarlingHttpRefusalLog(Window, 16);

        Assert.True(log.Observe(DarlingRefusalGate.Token, "10.0.0.5", T0).Log);
        Assert.True(log.Observe(DarlingRefusalGate.SourceCidr, "10.0.0.5", T0.AddSeconds(1)).Log);
        Assert.True(log.Observe(DarlingRefusalGate.HostAllowlist, "10.0.0.5", T0.AddSeconds(2)).Log);
    }

    /// <summary>
    /// Per SOURCE, not per gate alone. Keyed per gate only, one scanner's line wins the window and hides
    /// the tester's own refusal completely — the failure this exists to fix, reintroduced by the fix.
    /// </summary>
    [Fact]
    public void OneNoisySource_DoesNotHideAnother()
    {
        var log = new DarlingHttpRefusalLog(Window, 16);

        for (var i = 0; i < 100; i++)
        {
            log.Observe(DarlingRefusalGate.Token, "203.0.113.9", T0.AddSeconds(i));
        }

        Assert.True(
            log.Observe(DarlingRefusalGate.Token, "10.0.0.5", T0.AddSeconds(101)).Log,
            "the tester's first refusal must survive a noisy neighbour on the same gate");
    }

    /// <summary>
    /// The ceiling, measured rather than asserted about: an hour of 30 refusals per second from a few
    /// hundred sources — 108,000 refused requests — must not be able to fill the log, and the tracked
    /// state must stay inside the cap.
    /// </summary>
    [Fact]
    public void AScanner_CannotFillTheLog()
    {
        var log = new DarlingHttpRefusalLog(Window, 16);
        var random = new Random(1);
        var lines = 0;

        for (var second = 0; second < 3600; second++)
        {
            for (var burst = 0; burst < 10; burst++)
            {
                var source = $"203.0.113.{random.Next(1, 255)}:{random.Next(1, 10)}";
                foreach (DarlingRefusalGate gate in Enum.GetValues<DarlingRefusalGate>())
                {
                    if (log.Observe(gate, source, T0.AddSeconds(second)).Log)
                    {
                        lines++;
                    }
                }
            }
        }

        Assert.True(lines < 400, $"108,000 refused requests produced {lines} log lines — the ceiling is not holding");
        Assert.True(
            log.TrackedCount <= 16 + Enum.GetValues<DarlingRefusalGate>().Length,
            $"tracked entries grew to {log.TrackedCount} — the per-source map is unbounded");
    }

    /// <summary>
    /// Overflow is ANNOUNCED. Past the cap a source still gets a line, and that line says it speaks for
    /// several sources — because "this port is being scanned" is the more urgent fact, and silently
    /// dropping the surplus would leave the operator reading a log that looks quiet.
    /// </summary>
    [Fact]
    public void PastTheCap_TheLineSaysItSpeaksForSeveralSources()
    {
        var log = new DarlingHttpRefusalLog(Window, 2);

        var first = log.Observe(DarlingRefusalGate.Token, "a", T0);
        var second = log.Observe(DarlingRefusalGate.Token, "b", T0);
        Assert.True(first.Log);
        Assert.False(first.Aggregated);
        Assert.True(second.Log);
        Assert.False(second.Aggregated);

        var overflow = log.Observe(DarlingRefusalGate.Token, "c", T0);
        Assert.True(overflow.Log, "a source past the cap must still be reported, not silently dropped");
        Assert.True(overflow.Aggregated, "and the line must say it is speaking for more than one source");
    }

    /// <summary>
    /// The aggregate bucket folds MANY DIFFERENT addresses, so its suppression clause must not claim they
    /// were repeats from one.
    ///
    /// <para>Review catch on #2479: <c>PastTheCap_…</c> above only exercises the FIRST overflow, before any
    /// folding has happened on the aggregate — so the wording that ships with the folded count was
    /// untested, and it said "from this source". That is false in the direction that matters. An operator
    /// reading "400 further refusals from this source" goes looking for one busy client; what is actually
    /// happening is a broad scan, which is the thing the aggregate bucket exists to tell them.</para>
    /// </summary>
    [Fact]
    public void TheAggregateSuppressionClause_DoesNotClaimTheyCameFromOneSource()
    {
        var log = new DarlingHttpRefusalLog(Window, 2);

        /* Fill the per-source budget, then push the overflow through one gate from many addresses. */
        log.Observe(DarlingRefusalGate.Token, "10.0.0.1", T0);
        log.Observe(DarlingRefusalGate.Token, "10.0.0.2", T0);

        var firstOverflow = log.Observe(DarlingRefusalGate.Token, "203.0.113.1", T0);
        Assert.True(firstOverflow.Log);
        Assert.True(firstOverflow.Aggregated);

        for (var i = 2; i <= 400; i++)
        {
            Assert.False(log.Observe(DarlingRefusalGate.Token, $"203.0.113.{i}", T0.AddSeconds(i)).Log);
        }

        var next = log.Observe(DarlingRefusalGate.Token, "203.0.113.401", T0.Add(Window).AddSeconds(1));
        Assert.True(next.Log);
        Assert.True(next.Aggregated);
        Assert.Equal(399, next.SuppressedSinceLastLog);

        var clause = DarlingHttpRefusalLog.DescribeSuppression(next, Window);
        Assert.Contains("399", clause, StringComparison.Ordinal);
        Assert.Contains("from other sources", clause, StringComparison.Ordinal);
        Assert.DoesNotContain("from this source", clause, StringComparison.Ordinal);

        /* And the per-source wording is still right where it IS one source. */
        var perSource = log.Observe(DarlingRefusalGate.Token, "10.0.0.1", T0.Add(Window).AddSeconds(2));
        Assert.True(perSource.Log);
        Assert.False(perSource.Aggregated);
        Assert.Contains(
            "from this source",
            DarlingHttpRefusalLog.DescribeSuppression(perSource with { SuppressedSinceLastLog = 7 }, Window),
            StringComparison.Ordinal);
    }

    /// <summary>
    /// Eviction is keyed on LAST SEEN, not last logged. Keyed on last logged, a source still being refused
    /// every second would be dropped at the two-window horizon and re-log as a fresh first sighting —
    /// turning the rate limit into a rate multiplier, which is worse than no limit because it looks like
    /// one is working.
    /// </summary>
    [Fact]
    public void AContinuouslyRefusedSource_IsNeverEvictedIntoAFreshFirstSighting()
    {
        var log = new DarlingHttpRefusalLog(Window, 2);

        var lines = 0;
        var everyLaterLineReportedItsFolds = true;
        for (var half = 0; half <= 50; half++)
        {
            var decision = log.Observe(DarlingRefusalGate.Token, "a", T0.AddSeconds(half * 30));
            if (!decision.Log)
            {
                continue;
            }

            lines++;
            if (lines > 1 && decision.SuppressedSinceLastLog == 0)
            {
                everyLaterLineReportedItsFolds = false;
            }
        }

        /* 25 minutes at 2 refusals a minute: one line per 10-minute window, and not one more. */
        Assert.Equal(3, lines);
        Assert.True(everyLaterLineReportedItsFolds, "a later line reported nothing folded — it re-armed as a first sighting");
        Assert.Equal(1, log.TrackedCount);
    }

    /// <summary>And silence does free the budget again, or a finished scan holds the slots forever.</summary>
    [Fact]
    public void ASourceSilentForTwoWindows_IsForgotten()
    {
        var log = new DarlingHttpRefusalLog(Window, 2);
        log.Observe(DarlingRefusalGate.Token, "a", T0);
        log.Observe(DarlingRefusalGate.Token, "b", T0);

        var later = log.Observe(DarlingRefusalGate.Token, "c", T0.AddMinutes(21));
        Assert.True(later.Log);
        Assert.False(later.Aggregated, "the budget should have been freed by eviction");
    }

    /// <summary>A remote address ASP.NET Core cannot report is a real state (the CIDR gate fails closed on
    /// it), so it needs a stable key rather than an exception. IPv4-mapped IPv6 is unwrapped so one client
    /// is one key.</summary>
    [Fact]
    public void SourceDescription_HandlesTheAddressesTheGatesActuallySee()
    {
        Assert.Equal("unknown", DarlingHttpRefusalLog.DescribeSource(null));
        Assert.Equal("10.0.0.5", DarlingHttpRefusalLog.DescribeSource(IPAddress.Parse("10.0.0.5")));
        Assert.Equal("10.0.0.5", DarlingHttpRefusalLog.DescribeSource(IPAddress.Parse("::ffff:10.0.0.5")));
        Assert.Equal("::1", DarlingHttpRefusalLog.DescribeSource(IPAddress.IPv6Loopback));
    }

    /// <summary>
    /// A log file is a text file. The Host header is attacker-supplied and the only request-derived value
    /// echoed into a line, so a header carrying CR/LF must not be able to forge entries — and a megabyte
    /// header must not become a megabyte of log.
    /// </summary>
    [Fact]
    public void AHostHeader_CannotForgeLogLines()
    {
        var forged = DarlingHttpRefusalLog.Sanitize("evil\r\n2026-08-21 12:00:00 WARN  Everything is fine");
        Assert.DoesNotContain('\r', forged);
        Assert.DoesNotContain('\n', forged);

        Assert.True(DarlingHttpRefusalLog.Sanitize(new string('x', 5000)).Length <= DarlingHttpRefusalLog.MaxEchoedLength + 1);
        Assert.Equal("(none)", DarlingHttpRefusalLog.Sanitize(null));
        Assert.Equal("(none)", DarlingHttpRefusalLog.Sanitize(""));

        /* And an ordinary hostname survives unchanged, or the line stops being useful. */
        Assert.Equal("darling.example.com", DarlingHttpRefusalLog.Sanitize("darling.example.com"));
        Assert.Equal("10.197.53.214:5152", DarlingHttpRefusalLog.Sanitize("10.197.53.214:5152"));
    }

    /// <summary>Every gate is named the way an operator would have to name it to fix it — the CIDR gate
    /// by its config key, not by "403".</summary>
    [Fact]
    public void EveryGate_IsNamedInTermsTheOperatorCanActOn()
    {
        foreach (DarlingRefusalGate gate in Enum.GetValues<DarlingRefusalGate>())
        {
            var described = DarlingHttpRefusalLog.Describe(gate);
            Assert.False(string.IsNullOrWhiteSpace(described));
            Assert.NotEqual(gate.ToString(), described);
        }

        Assert.Contains("allowFrom", DarlingHttpRefusalLog.Describe(DarlingRefusalGate.SourceCidr), StringComparison.Ordinal);
        Assert.Contains("Host header", DarlingHttpRefusalLog.Describe(DarlingRefusalGate.HostAllowlist), StringComparison.Ordinal);
    }

    /// <summary>
    /// A bearer refusal distinguishes THREE client states, not two.
    ///
    /// <para>Review catch on #2479. <c>ExtractBearerToken</c> answers null for both "no header" and "a
    /// header that is not a well-formed Bearer", so a refusal line built on it alone told an operator that
    /// nothing was presented while their client was sending <c>Authorization: Basic …</c> every second —
    /// collapsing exactly the ambiguity this feature exists to resolve. Each state is a different next
    /// step: no token configured, token configured wrong, or the wrong token.</para>
    /// </summary>
    [Theory]
    /* Nothing sent at all. */
    [InlineData(null, "no 'Authorization: Bearer <token>' header was presented")]
    [InlineData("", "no 'Authorization: Bearer <token>' header was presented")]
    [InlineData("   ", "no 'Authorization: Bearer <token>' header was presented")]
    /* Something WAS sent, in the wrong shape. Each of these used to read as "nothing was presented". */
    [InlineData("Basic dXNlcjpwYXNz", "an Authorization header WAS presented but is not a 'Bearer <token>'")]
    [InlineData("abc123", "an Authorization header WAS presented but is not a 'Bearer <token>'")]
    [InlineData("Bearer ", "an Authorization header WAS presented but is not a 'Bearer <token>'")]
    [InlineData("Bearer    ", "an Authorization header WAS presented but is not a 'Bearer <token>'")]
    /* A well-formed Bearer that simply does not match — including the case-insensitive scheme. */
    [InlineData("Bearer wrong-token", "does not match mcp.network.encryptedToken")]
    [InlineData("bearer wrong-token", "does not match mcp.network.encryptedToken")]
    public void ABearerRefusal_TellsNoTokenFromAMalformedOne_FromAWrongOne(string? header, string expected)
    {
        var described = DarlingMcpHostService.DescribeBearerRefusal(header);

        Assert.Contains(expected, described, StringComparison.Ordinal);

        /* And it can never carry the value, because it only ever reads the header's SHAPE. */
        if (header is not null && header.Contains("wrong-token", StringComparison.Ordinal))
        {
            Assert.DoesNotContain("wrong-token", described, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The verb agrees with the status code.
    ///
    /// <para>Review catch on #2479: not every gate rejection ends in a 4xx. The web dashboard answers an
    /// in-CIDR request with a WRONG <c>?token=</c> using a 200 and the login page, deliberately — and
    /// "refused a request … with 200" is self-contradictory on its face, misleading anyone reading the log
    /// cold or filtering for denials on status.</para>
    /// </summary>
    [Theory]
    [InlineData(400, "refused")]
    [InlineData(401, "refused")]
    [InlineData(403, "refused")]
    [InlineData(200, "did not authorize")]
    [InlineData(302, "did not authorize")]
    public void TheOutcomeVerb_AgreesWithTheStatusCode(int statusCode, string expected) =>
        Assert.Equal(expected, DarlingHttpRefusalLog.DescribeOutcome(statusCode));

    /// <summary>
    /// Every refusal site on both hosts reports. This is the half that decays: a gate added later is a
    /// gate that refuses silently again, and no behavioural test can see one that does not exist yet.
    /// </summary>
    [Theory]
    [InlineData("DarlingMcpHostService.cs")]
    [InlineData("DarlingWebHostService.cs")]
    public void EveryRefusalSite_ReportsBeforeItRefuses(string fileName)
    {
        var source = ReadHostSource(fileName);

        var unreported = new List<string>();
        var at = 0;
        var found = 0;
        const string Marker = "context.Response.StatusCode = StatusCodes.Status4";
        while ((at = source.IndexOf(Marker, at, StringComparison.Ordinal)) >= 0)
        {
            found++;

            /* Report sits immediately above the status assignment at every site, so a short lookback is
               both sufficient and specific — long enough to span the call's arguments, short enough that
               a neighbouring site's Report cannot satisfy this one. */
            var from = Math.Max(0, at - 1200);
            if (!source[from..at].Contains("refusals.Report(", StringComparison.Ordinal))
            {
                unreported.Add(source.Substring(at, Math.Min(72, source.Length - at)));
            }

            at += Marker.Length;
        }

        Assert.True(found > 0, $"{fileName} no longer refuses anything with a 4xx — this pin needs rewriting");
        Assert.True(
            unreported.Count == 0,
            $"{fileName} has refusal sites that log nothing (#2479):\n  " + string.Join("\n  ", unreported));
    }

    /// <summary>
    /// The token, any prefix of it, and its length are never logged. A refusal may say whether a
    /// credential was PRESENTED; it may never say anything about its value.
    /// </summary>
    [Theory]
    [InlineData("DarlingMcpHostService.cs")]
    [InlineData("DarlingWebHostService.cs")]
    public void NoRefusalLine_CanCarryTheToken(string fileName)
    {
        var source = ReadHostSource(fileName);

        var at = 0;
        var calls = 0;
        while ((at = source.IndexOf("refusals.Report(", at, StringComparison.Ordinal)) >= 0)
        {
            calls++;
            var arguments = ArgumentsAt(source, source.IndexOf('(', at));

            foreach (var forbidden in new[] { "bearerToken", "accessToken", "presentedToken", ", token", "{token}", ".Length" })
            {
                Assert.False(
                    arguments.Contains(forbidden, StringComparison.Ordinal),
                    $"{fileName}: a refusal line passes '{forbidden}' into the log — the token's value, "
                    + "prefix and length are all off limits (#2479)");
            }

            at += "refusals.Report(".Length;
        }

        Assert.True(calls >= 3, $"{fileName} reports only {calls} refusals — expected every gate to report");
    }

    /// <summary>
    /// The budget is per started server, not process-wide static: a rebind (a control-plane port change,
    /// or a restart of a listener that failed to bind) must start with a clean budget rather than
    /// inheriting the previous listener's scan and swallowing the first refusals of the new one.
    /// </summary>
    [Theory]
    [InlineData("DarlingMcpHostService.cs")]
    [InlineData("DarlingWebHostService.cs")]
    public void TheBudget_IsPerStartedServer_NotProcessWide(string fileName)
    {
        var source = ReadHostSource(fileName);

        var built = source.IndexOf("_app = builder.Build();", StringComparison.Ordinal);
        var created = source.IndexOf("var refusals = new DarlingHttpRefusalLog();", StringComparison.Ordinal);

        Assert.True(built >= 0, $"{fileName} no longer builds a host — this pin needs rewriting");
        Assert.True(created > built, $"{fileName} must create its refusal log with the server it belongs to (#2479)");
        Assert.DoesNotContain("static readonly DarlingHttpRefusalLog", source, StringComparison.Ordinal);
    }

    /// <summary>The argument list of the call whose '(' is at <paramref name="open"/>.</summary>
    private static string ArgumentsAt(string source, int open)
    {
        Assert.True(open >= 0, "expected a call");

        var depth = 0;
        for (var i = open; i < source.Length; i++)
        {
            if (source[i] == '(') { depth++; }
            else if (source[i] == ')')
            {
                depth--;
                if (depth == 0) { return source[(open + 1)..i]; }
            }
        }

        Assert.Fail("unbalanced parentheses while reading a refusal report call");
        return string.Empty;
    }

    private static string ReadHostSource(string fileName, [CallerFilePath] string thisFile = "")
    {
        var relative = Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "Mcp", fileName);
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, relative)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.False(dir is null, $"could not locate {relative} from the test source path");
        return File.ReadAllText(Path.Combine(dir!, relative));
    }
}
