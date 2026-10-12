using System;
using System.Collections.Generic;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The rotation live tests' failure description has to name each hash, pid, time and message it was given; a
/// description that dropped them would turn a duplicate back into "expected 1, actual 2".
/// </summary>
public sealed class PgLogRotationEvidenceTests
{
    private static PgLogEvent Row(string hash, int pid, DateTime at, string message) => new(
        OccurredAtUtc: at, Family: PgLogFamilies.Error, Severity: "LOG", SqlState: null, DatabaseName: null, UserName: null,
        ApplicationName: null, Pid: pid, Message: message, Detail: "Process holding the lock: 99.", Context: "while updating tuple",
        StatementFingerprint: null, RawLineHash: hash, Metrics: default);

    [Fact]
    public void Describe_NamesEveryHashPidTimeAndMessageOfTheMatchedRows()
    {
        var floor = new DateTime(2026, 9, 30, 12, 0, 0, DateTimeKind.Utc);
        var rows = new List<PgLogEvent>
        {
            Row("hash-aaa", 4242, floor.AddSeconds(1), "process 4242 still waiting for ShareLock after 100.123 ms"),
            Row("hash-bbb", 4242, floor.AddSeconds(2), "process 4242 still waiting for ShareLock after 900.456 ms"),
            Row("hash-ccc", 7, floor.AddSeconds(3), "unrelated entry"),
            Row("hash-aaa", 4242, floor.AddSeconds(1), "process 4242 still waiting for ShareLock after 100.123 ms"),
        };

        var text = PgLogRotationEvidence.Describe(
            "after rotation", rows, 4242, floor, r => r.Pid == 4242 && r.OccurredAtUtc >= floor,
            new Dictionary<string, string> { ["log_tail_json"] = "100|postgresql-a.json" },
            new Dictionary<string, string> { ["log_tail_json"] = "200|postgresql-b.json" },
            "  postgresql-a.json  100  2026-09-30 12:00:00+00");

        foreach (var expected in new[]
        {
            "pid=4242", floor.ToString("O", System.Globalization.CultureInfo.InvariantCulture), "hash=hash-aaa", "hash=hash-bbb",
            "after 100.123 ms", "after 900.456 ms", "severity=LOG", "family=" + PgLogFamilies.Error, "detail=\"Process holding the lock: 99.\"",
            "context=\"while updating tuple\"", "hash-aaa x2", "log_tail_json=100|postgresql-a.json", "log_tail_json=200|postgresql-b.json",
            "postgresql-a.json  100",
        })
        {
            Assert.Contains(expected, text, StringComparison.Ordinal);
        }

        Assert.DoesNotContain("hash=hash-ccc", text, StringComparison.Ordinal);
        Assert.Contains("matching the wait: 3", text, StringComparison.Ordinal);
    }

    [Fact]
    public void Describe_ClipsLongTextAndToleratesMissingParts()
    {
        var floor = DateTime.UtcNow;
        var text = PgLogRotationEvidence.Describe(
            "x", [Row("h", 1, floor, new string('m', 500))], 1, floor, _ => true, null, null, string.Empty);

        Assert.Contains("...(500 chars)", text, StringComparison.Ordinal);
        Assert.DoesNotContain(new string('m', 250), text, StringComparison.Ordinal);
        Assert.Contains("carried state: (none)", text, StringComparison.Ordinal);
        Assert.Contains("(not read)", text, StringComparison.Ordinal);
    }
}
