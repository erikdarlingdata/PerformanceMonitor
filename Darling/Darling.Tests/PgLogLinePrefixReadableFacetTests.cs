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
    public void APrefixWithAnUnbracketedPidIsReportedAsUnreadable(string prefix)
        => Assert.False(Predicate().IsMatch(prefix), prefix);

    /// <summary>The prefixes the assembler already reads: PostgreSQL's own default, the Debian one, pgBadger's, the
    /// managed family (RDS), and the fields-before-the-pid one.</summary>
    [Theory]
    [InlineData("%m [%p] ")]
    [InlineData("%m [%p] %q%u@%d ")]
    [InlineData("%t [%p]: user=%u,db=%d,app=%a,client=%h ")]
    [InlineData("%t:%r:%u@%d:[%p]:")]
    [InlineData("%m %u@%d [%p] ")]
    public void APrefixTheReadersAlreadyUnderstandIsReportedAsReadable(string prefix)
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
