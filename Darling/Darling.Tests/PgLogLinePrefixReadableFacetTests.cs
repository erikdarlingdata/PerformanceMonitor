// #4735 item 3: a log_line_prefix with an unbracketed %p ('%m %p', '%t %p:', '%t [%p-%l]') cannot be read by the
// stderr log readers, and the plan-capture readiness read looked only at %Q, so such a target read as quiet. The
// readiness read now reports whether the prefix can be read.
using System;
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

public sealed class PgLogLinePrefixReadableFacetTests
{
    private static string Sql()
    {
        var context = new CollectorContext
        {
            ServerId = 1,
            ServerName = "target-a",
            CollectionTime = new DateTime(2026, 9, 29, 1, 0, 0, DateTimeKind.Unspecified),
            Deltas = new CollectorDeltaCalculator(),
            Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql },
        };

        return PgPlanCaptureReadinessCollector.Instance.BuildQuery(context).Text;
    }

    /// <summary>The facet's predicate, pulled out of the shipped statement so the test judges the text that runs. The
    /// pattern is spelled in the subset PostgreSQL's regex and .NET's share.</summary>
    private static Regex Predicate()
    {
        var match = Regex.Match(Sql(), @"coalesce\(s\.line_prefix, ''\)\s*~\s*'(?<pattern>[^']*)'\s+AS prefix_readable");
        Assert.True(match.Success, "the probe no longer derives prefix_readable from log_line_prefix");
        return new Regex(match.Groups["pattern"].Value, RegexOptions.CultureInvariant);
    }

    [Fact]
    public void TheFacetIsOneMoreSeparateRow()
    {
        var sql = Sql();

        Assert.Contains("'log_line_prefix_readable'::text", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("%m %p ")]
    [InlineData("%t %p:")]
    [InlineData("%t [%p-%l]")]
    [InlineData("%t[%p] ")]
    [InlineData("%m[%p] %Q ")]
    public void APrefixWithAnUnbracketedPidIsReportedAsUnreadable(string prefix)
        => Assert.False(Predicate().IsMatch(prefix), prefix);

    /// <summary>A literal bracket between the timestamp and <c>[%p]</c> (#7182 round 4). The readers' gap before the pid
    /// bracket passes the exact text <c>[local]</c> and no other bracket, and <c>%r</c> and <c>%h</c>, the only escapes
    /// that render it, carry no <c>[</c> in the format string. So <c>[%l]</c>, <c>[%a]</c> and <c>[%c]</c> before the pid
    /// bracket drop every line (or, for <c>[%l]</c>, read the line number as the pid) and the facet must say so.</summary>
    [Theory]
    [InlineData("%m [%l] [%p] ")]
    [InlineData("%m [%a] [%p] ")]
    [InlineData("%m [%c] [%p] ")]
    [InlineData("%m [%l] [%p] %Q ")]
    [InlineData("%m [%a] [%p] %Q ")]
    [InlineData("%m [%c] [%p] %Q ")]
    [InlineData("%t:[%a]:[%p]:")]
    public void ALiteralBracketBeforeThePidBracketIsReportedAsUnreadable(string prefix)
        => Assert.False(Predicate().IsMatch(prefix), prefix);

    /// <summary>The prefixes every stderr reader (the plan, deadlock and log-event readers) understands: PostgreSQL's
    /// own default, the Debian one, pgBadger's, the managed family (RDS), the fields-before-the-pid one, and the two
    /// that put <c>%r</c> before the pid, which write <c>[local]</c> there for a Unix-socket session. The two
    /// <c>[local]</c> renderings are read by all three readers since #7182 round 4
    /// (<see cref="PgLocalSocketPrefixReadersTests"/> and <see cref="PgDeadlockCandidatePatternLiveTests"/>).</summary>
    [Theory]
    [InlineData("%m [%p] ")]
    [InlineData("%m [%p] %q%u@%d ")]
    [InlineData("%t [%p]: user=%u,db=%d,app=%a,client=%h ")]
    [InlineData("%t:%r:%u@%d:[%p]:")]
    [InlineData("%t:%r:%u@%d:[%p]:%Q ")]
    [InlineData("%m %u@%d [%p] ")]
    [InlineData("%m %r [%p] ")]
    [InlineData("%m %r [%p] %Q ")]
    public void APrefixEveryStderrReaderUnderstandsIsReportedAsReadable(string prefix)
        => Assert.True(Predicate().IsMatch(prefix), prefix);

    [Theory]
    [InlineData("")]
    [InlineData("[%p] %m ")]
    [InlineData("%u@%d ")]
    public void APrefixWithoutALeadingTimestampOrABracketedPidIsUnreadable(string prefix)
        => Assert.False(Predicate().IsMatch(prefix), prefix);

    [Fact]
    public void TheFacetJudgesOnTheProbeColumnAndNamesTheRemedy()
    {
        var sql = Sql();
        var branch = sql[sql.IndexOf("'log_line_prefix_readable'::text", StringComparison.Ordinal)..];

        Assert.Contains("p.prefix_readable", branch, StringComparison.Ordinal);
        Assert.Contains("[%p]", branch, StringComparison.Ordinal);
    }
}
