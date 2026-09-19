/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Alerting;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Pins the Long-Running Query opt-out knob's ONE rule (#3653 A5, ruling Q5) in both of its spellings — the C#
/// matcher the engine's fakes and any in-memory host apply, and the <c>ILIKE … ESCAPE</c> predicate both SKUs'
/// reads splice ahead of their row cap — and that the two agree: a pattern that excludes a session in C# must
/// produce an operand that excludes the same session in SQL, or Lite and Darling drift in what a setting means,
/// which is the #1839/#1911 class of bug.
/// </summary>
public sealed class LongRunningQueryExclusionsTests
{
    [Theory]
    [InlineData("HammerDB", "HammerDB", true)]
    [InlineData("HammerDB", "hammerdb", true)]                        /* case-insensitive */
    [InlineData("HammerDB", "HammerDB 4.0", false)]                   /* whole-value: no implicit prefix */
    [InlineData("HammerDB", " HammerDB", false)]                      /* whole-value: no trimming of the session's value */
    [InlineData("SQLAgent - TSQL JobStep*", "SQLAgent - TSQL JobStep (Job 0x01 : Step 3)", true)]
    [InlineData("SQLAgent - TSQL JobStep*", "sqlagent - tsql jobstep (Job 0x01 : Step 3)", true)]
    [InlineData("SQLAgent - TSQL JobStep*", "SQLAgent - Job Manager", false)]
    [InlineData("svc_*", "svc_replication", true)]
    [InlineData("svc_*", "svc_", true)]                               /* the empty suffix is a suffix */
    [InlineData("svc_*", "SVCX", false)]                              /* _ is literal, not LIKE's single char */
    [InlineData("*Reader", "Replication Reader", false)]              /* no leading wildcard: literal '*' */
    [InlineData("Rep*Reader", "Replication Reader", false)]           /* no embedded wildcard */
    [InlineData("HammerDB", "", false)]                               /* an unnamed session matches nothing */
    [InlineData("HammerDB", null, false)]
    public void Matches_IsCaseInsensitive_WholeValue_WithATrailingStarOnly(string pattern, string? value, bool expected)
    {
        Assert.Equal(expected, LongRunningQueryExclusions.Matches(value, new[] { pattern }));
    }

    [Fact]
    public void Excludes_WhenEitherListMatches()
    {
        var knob = LongRunningQueryExclusions.From(new[] { "QueueWorker" }, new[] { "svc_*" });

        Assert.True(knob.Excludes("QueueWorker", "erik"));
        Assert.True(knob.Excludes("SSMS", "svc_etl"));
        Assert.False(knob.Excludes("SSMS", "erik"));
        Assert.False(knob.Excludes(null, null));
        Assert.False(knob.IsEmpty);
        Assert.True(LongRunningQueryExclusions.None.IsEmpty);
        Assert.False(LongRunningQueryExclusions.None.Excludes("anything", "anyone"));
    }

    [Fact]
    public void Normalize_TrimsDropsBlanksDedupesCaseInsensitively_AndRefusesABareStar()
    {
        /* The same treatment excludedDatabases gets, so a Settings-window string "HammerDB, hammerdb , " and an
           MCP array mean one thing. A bare * is the alert's enable switch in disguise and is refused rather than
           honoured. First-seen order and first-seen spelling are kept: the card lists what the operator typed. */
        var normalised = LongRunningQueryExclusions.Normalize(new[] { " HammerDB ", "hammerdb", "", "  ", "*", "svc_*", "HAMMERDB" });

        Assert.Equal(new[] { "HammerDB", "svc_*" }, normalised);
        Assert.Empty(LongRunningQueryExclusions.Normalize(null));
        Assert.Empty(LongRunningQueryExclusions.Normalize(new[] { "*" }));
    }

    [Theory]
    [InlineData("HammerDB", "HammerDB")]
    [InlineData("svc_*", "svc\\_%")]                                  /* _ escaped, wildcard becomes % */
    [InlineData("100%*", "100\\%%")]                                  /* a literal % survives, then the wildcard */
    [InlineData("a\\b", "a\\\\b")]                                    /* backslash escaped first */
    [InlineData("Rep*Reader", "Rep*Reader")]                          /* an embedded * is literal to LIKE too */
    public void ToLikeOperand_EscapesLikesMetacharacters_AndOnlyATrailingStarBecomesPercent(string pattern, string expected)
    {
        Assert.Equal(expected, LongRunningQueryExclusions.ToLikeOperand(pattern));
    }

    [Fact]
    public void BuildSqlPredicate_IsOnePositiveMatch_WithOperandsNumberedFromTheGivenOrdinal()
    {
        /* Positive rather than negated so the read can both filter on NOT (…) and count on it; COALESCE so a
           NULL column is an empty string that matches no non-empty pattern; one term per pattern, programs then
           logins, operands in the same order as their $n. */
        var knob = LongRunningQueryExclusions.From(new[] { "HammerDB", "SQLAgent - TSQL JobStep*" }, new[] { "svc_*" });

        var (predicate, operands) = knob.BuildSqlPredicate("r.program_name", "r.login_name", firstParameterOrdinal: 5);

        Assert.Equal(
            "(COALESCE(r.program_name, '') ILIKE $5 ESCAPE '\\' OR COALESCE(r.program_name, '') ILIKE $6 ESCAPE '\\' OR COALESCE(r.login_name, '') ILIKE $7 ESCAPE '\\')",
            predicate);
        Assert.Equal(new[] { "HammerDB", "SQLAgent - TSQL JobStep%", "svc\\_%" }, operands);
    }

    [Fact]
    public void BuildSqlPredicate_IsEmptyForAnEmptyKnob_SoTheReadStaysByteIdentical()
    {
        var (predicate, operands) = LongRunningQueryExclusions.None.BuildSqlPredicate("p", "l", 5);
        Assert.Equal("", predicate);
        Assert.Empty(operands);
    }

    /// <summary>
    /// The two SKUs' reads splice the predicate at the same place with the same operand ordinal and the same
    /// FALSE fallback, and both project <c>login_name</c> and the excluded count — source-pinned because the
    /// DuckDB read is an interpolated string inside a WPF-hosted service this Mac cannot load, and the parity is
    /// the point: one knob, one meaning, on both stores.
    /// </summary>
    [Fact]
    public void BothReads_SpliceTheKnobAheadOfTheCap_AndProjectTheCount()
    {
        var lite = ReadRepoFile("Lite", "Services", "LocalDataService.WaitStats.cs");
        var darling = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingAlertReadAdapter.cs");

        foreach (var source in new[] { lite, darling })
        {
            Assert.Contains("BuildSqlPredicate(\"r.program_name\", \"r.login_name\", firstParameterOrdinal: 5)", source, StringComparison.Ordinal);
            Assert.Contains("AS excluded_by_knob", source, StringComparison.Ordinal);
            Assert.Contains("WHERE NOT r.excluded_by_knob", source, StringComparison.Ordinal);
            Assert.Contains("(SELECT COUNT(*) FROM candidates AS x WHERE x.excluded_by_knob)", source, StringComparison.Ordinal);
            Assert.Contains("LIMIT $3", source, StringComparison.Ordinal);
            Assert.Contains("LoginName = reader.IsDBNull(11)", source, StringComparison.Ordinal);
            Assert.Contains("excludedCount = reader.IsDBNull(12)", source, StringComparison.Ordinal);
        }

        /* The flag sits INSIDE the CTE, before the outer LIMIT — the cap is over kept rows only. */
        var template = DarlingAlertReadAdapterTemplate(darling);
        Assert.True(template.IndexOf("{1} AS excluded_by_knob", StringComparison.Ordinal) < template.IndexOf("LIMIT $3", StringComparison.Ordinal));
        Assert.True(template.IndexOf("WHERE NOT r.excluded_by_knob", StringComparison.Ordinal) < template.IndexOf("LIMIT $3", StringComparison.Ordinal));
    }

    private static string DarlingAlertReadAdapterTemplate(string source)
    {
        var start = source.IndexOf("public const string LongRunningQueriesSqlTemplate", StringComparison.Ordinal);
        Assert.True(start >= 0);
        var end = source.IndexOf("\";", start, StringComparison.Ordinal);
        return source[start..end];
    }
}
