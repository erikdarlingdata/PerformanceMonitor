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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8665: the text a PostgreSQL collector sends to a monitored server is stored in that server's statement statistics
/// and shown in the Viewer's Query Shapes as the monitor's own statement, so it carries no development notes. The
/// reasoning for a statement lives in a C# comment beside it. Only the short own-statement marker (PgOwnStatement.Marker)
/// may appear as a block comment in what is sent.
/// </summary>
public class PgSentStatementCommentTests
{
    private const int LongestAllowedBlockComment = 80;

    private sealed class NoDeltas : ICollectorDeltaCalculator
    {
        public long CalculateDelta(int serverId, string key, string metric, long current, DateTime? at = null, int i = 0) => 0;

        public long CalculateDeltaWithInterval(int serverId, string key, string metric, long current, out int seconds, DateTime? at = null, int i = 0)
        {
            seconds = 60;
            return 0;
        }
    }

    private static CollectorContext Context(int major, bool aurora) => new()
    {
        ServerId = 1,
        ServerName = "pg",
        CollectionTime = DateTime.UtcNow,
        Deltas = new NoDeltas(),
        Target = new CollectorTargetInfo
        {
            Engine = CollectorTargetEngine.PostgreSql,
            PostgresMajorVersion = major,
            PostgresVersionNum = major * 10000,
            IsAurora = aurora,
        },
        LogHashKey = TestLogHashKeys.Fixed,
    };

    private static IEnumerable<(string Name, string Sql)> SentStatements()
    {
        foreach (var (major, aurora) in new[] { (10, false), (13, false), (17, false), (16, true) })
        {
            var context = Context(major, aurora);
            foreach (var collector in CollectorCatalog.All.Where(c => c.TargetEngine == CollectorTargetEngine.PostgreSql))
            {
                CollectorQuery? query;
                try
                {
                    query = collector.BuildQueryUntyped(context);
                }
                catch (NotSupportedException)
                {
                    // A collector with no SQL route (pg_cpu_utilization reads through the RDS ingestor) says so this way.
                    continue;
                }

                if (query is not null)
                {
                    yield return ($"{collector.GetType().Name} (major {major}, aurora {aurora})", query.Text);
                }
            }

            yield return ($"PgStatementText.FetchSqlFor (major {major}, aurora {aurora})", PgStatementText.FetchSqlFor(aurora, major, context));
        }
    }

    private static List<string> LongBlockComments(string sql) =>
        Regex.Matches(sql, @"/\*.*?\*/", RegexOptions.Singleline)
            .Select(m => m.Value)
            .Where(c => c.Length > LongestAllowedBlockComment)
            .ToList();

    [Fact]
    public void NoStatementSentToAMonitoredPostgresServer_CarriesALongBlockComment()
    {
        var all = SentStatements().ToList();
        Assert.True(all.Count > 40, $"expected the PostgreSQL collectors' statements, found {all.Count}");

        var offenders = all
            .Select(s => (s.Name, Comments: LongBlockComments(PgOwnStatement.Mark(s.Sql))))
            .Where(s => s.Comments.Count > 0)
            .Select(s => $"{s.Name}: {s.Comments.Count} block comment(s), first starts \"{s.Comments[0][..Math.Min(60, s.Comments[0].Length)]}\"")
            .Distinct()
            .ToList();

        Assert.True(offenders.Count == 0, "a statement sent to a monitored server carries its explanation in the SQL:\n" + string.Join("\n", offenders));
    }

    [Fact]
    public void TheStatementTextFetchConstants_CarryNoBlockCommentButTheOwnStatementMarker()
    {
        foreach (var sql in new[] { PgStatementText.AuroraFetchSql, PgStatementText.VanillaFetchSql })
        {
            Assert.DoesNotContain("/*", sql.Replace(PgOwnStatement.Marker, string.Empty, StringComparison.Ordinal), StringComparison.Ordinal);
            Assert.Empty(LongBlockComments(PgOwnStatement.Mark(sql)));
            Assert.EndsWith(PgOwnStatement.Marker, PgOwnStatement.Mark(sql).TrimEnd(';'), StringComparison.Ordinal);
        }
    }
}
