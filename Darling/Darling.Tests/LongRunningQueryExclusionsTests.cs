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
/// Pins the Long-Running Query opt-out knob's ONE rule (#3653 A5, ruling Q5, as the production-read addendum
/// shaped it) in both of its spellings — the C# matcher the engine's fakes and any in-memory host apply, and the
/// two <c>ILIKE … ESCAPE</c> predicates both SKUs' reads splice ahead of their row cap — and that the two agree:
/// an entry that excludes a session in C# must produce an operand that excludes the same session in SQL, or
/// Lite and Darling drift in what a setting means, which is the #1839/#1911 class of bug. Also pins the SEEDED
/// DEFAULTS to the read's four classes, because a default nobody pins is a default that quietly changes.
/// </summary>
public sealed class LongRunningQueryExclusionsTests
{
    [Theory]
    [InlineData("SQLAgent - TSQL JobStep", "SQLAgent - TSQL JobStep (Job 0x01 : Step 3)", true)]   /* the seeded prefix, the real value shape */
    [InlineData("SQLAgent - TSQL JobStep", "sqlagent - tsql jobstep (Job 0x01 : Step 3)", true)]   /* case-insensitive */
    [InlineData("SQLAgent - TSQL JobStep", "SQLAgent - TSQL JobStep", true)]                       /* the whole value is a prefix of itself */
    [InlineData("SQLAgent - TSQL JobStep", "SQLAgent - Job Manager", false)]
    [InlineData("SQLAgent - TSQL JobStep", " SQLAgent - TSQL JobStep (Job 0x01)", false)]          /* no trimming of the session's value */
    [InlineData("HammerDB", "HammerDB 4.0", true)]                                                 /* prefix, not whole-value */
    [InlineData("svc_", "svc_replication", true)]
    [InlineData("svc_", "SVCX", false)]                                                            /* _ is literal, not LIKE's single char */
    [InlineData("SQLAgent - TSQL JobStep*", "SQLAgent - TSQL JobStep (Job 0x01)", false)]          /* no wildcard grammar: * is a character */
    [InlineData("HammerDB", "", false)]                                                            /* an unnamed session matches nothing */
    [InlineData("HammerDB", null, false)]
    public void MatchesPrefix_IsACaseInsensitivePrefix_WithNoWildcardGrammar(string prefix, string? value, bool expected)
    {
        Assert.Equal(expected, LongRunningQueryExclusions.MatchesPrefix(value, new[] { prefix }));
    }

    [Theory]
    [InlineData(@"NT AUTHORITY\SYSTEM", @"NT AUTHORITY\SYSTEM", true)]
    [InlineData(@"NT AUTHORITY\SYSTEM", @"nt authority\system", true)]                             /* case-insensitive */
    [InlineData(@"NT AUTHORITY\SYSTEM", @"NT AUTHORITY\SYSTEM ", false)]                           /* whole value: no trimming of the session's value */
    [InlineData("svc", "svc_owner", false)]                                                        /* EXACT: a login is a whole name, never a prefix */
    [InlineData("svc_", "svc_replication", false)]
    [InlineData("erik", "", false)]
    [InlineData("erik", null, false)]
    public void MatchesExact_IsACaseInsensitiveWholeValue(string login, string? value, bool expected)
    {
        Assert.Equal(expected, LongRunningQueryExclusions.MatchesExact(value, new[] { login }));
    }

    [Fact]
    public void Classify_NamesTheArm_AndABothArmsMatchIsTheProgramPrefix()
    {
        /* The counts-once rule lives here: a job step running as SYSTEM is ONE excluded session, under the
           prefix — the arm listed first and the one an operator reaches for first — so the read's two counts sum
           to the sessions removed. */
        var knob = LongRunningQueryExclusions.Defaults;

        Assert.Equal(LongRunningQueryExclusionArm.ProgramPrefix, knob.Classify("SQLAgent - TSQL JobStep (Job 0x01 : Step 1)", "app_admin"));
        Assert.Equal(LongRunningQueryExclusionArm.ProgramPrefix, knob.Classify("SQLAgent - TSQL JobStep (Job 0x01 : Step 1)", @"NT AUTHORITY\SYSTEM"));
        Assert.Equal(LongRunningQueryExclusionArm.Login, knob.Classify(".Net SqlClient Data Provider", @"NT AUTHORITY\NETWORK SERVICE"));
        Assert.Equal(LongRunningQueryExclusionArm.None, knob.Classify("Microsoft SQL Server Management Studio - Query", "app_admin"));
        Assert.Equal(LongRunningQueryExclusionArm.None, knob.Classify(null, null));

        Assert.True(knob.Excludes("SQLAgent - TSQL JobStep (Job 0x01 : Step 1)", "erik"));
        Assert.False(knob.Excludes("SSMS", "erik"));
        Assert.False(knob.IsEmpty);
        Assert.True(LongRunningQueryExclusions.None.IsEmpty);
        Assert.False(LongRunningQueryExclusions.None.Excludes("anything", "anyone"));
    }

    [Fact]
    public void TheSeededDefaults_AreTheProductionReadsTwoBackgroundClasses_AndNotItsAdminLogin()
    {
        /* Measured over 7 days on one large production store, the long-running population was four classes:
           (1) SQL Agent job steps — program prefix, ~460 sessions/week; (2) the NT AUTHORITY\SYSTEM and
           NT AUTHORITY\NETWORK SERVICE logins — the multi-day CDC-shaped background; (3) the application's admin
           login — NOT a default, because it also carries real ad-hoc long-runners; (4) named humans — never.
           The seeds are exactly (1) and (2), and the login list has TWO entries, not a prefix that would cover
           both (a login is exact — "NT AUTHORITY\" as a prefix would also swallow LOCAL SERVICE and anything
           else under that authority the read never measured). */
        Assert.Equal(new[] { "SQLAgent - TSQL JobStep" }, LongRunningQueryExclusions.DefaultProgramNamePrefixes);
        Assert.Equal(new[] { @"NT AUTHORITY\SYSTEM", @"NT AUTHORITY\NETWORK SERVICE" }, LongRunningQueryExclusions.DefaultLogins);
        Assert.Same(LongRunningQueryExclusions.DefaultProgramNamePrefixes, LongRunningQueryExclusions.Defaults.ProgramNamePrefixes);
        Assert.Same(LongRunningQueryExclusions.DefaultLogins, LongRunningQueryExclusions.Defaults.Logins);

        /* The seeds survive the normaliser unchanged — a default that Normalize would rewrite is two defaults. */
        var normalised = LongRunningQueryExclusions.From(LongRunningQueryExclusions.DefaultProgramNamePrefixes, LongRunningQueryExclusions.DefaultLogins);
        Assert.Equal(LongRunningQueryExclusions.DefaultProgramNamePrefixes, normalised.ProgramNamePrefixes);
        Assert.Equal(LongRunningQueryExclusions.DefaultLogins, normalised.Logins);

        /* Class 3 stays evaluated, in every casing an admin login might be spelled. */
        Assert.DoesNotContain(LongRunningQueryExclusions.DefaultLogins, l => l.Contains("admin", StringComparison.OrdinalIgnoreCase));
        Assert.DoesNotContain(LongRunningQueryExclusions.DefaultLogins, l => l.Equals("sa", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Normalize_TrimsDropsBlanksDedupesCaseInsensitively_AndKeepsAStarAsACharacter()
    {
        /* The same treatment excludedDatabases gets, so a Settings-window string "HammerDB, hammerdb , " and an
           MCP array mean one thing. A blank is dropped because an empty PREFIX would match every program — the
           alert's enable switch in disguise. First-seen order and first-seen spelling are kept: the card lists
           what the operator typed, including a "*" typed by someone expecting a wildcard grammar that does not
           exist — kept, listed, and matching nothing, so the misspelling is visible rather than silently fixed. */
        var normalised = LongRunningQueryExclusions.Normalize(new[] { " HammerDB ", "hammerdb", "", "  ", "*", "svc_", "HAMMERDB" });

        Assert.Equal(new[] { "HammerDB", "*", "svc_" }, normalised);
        Assert.Empty(LongRunningQueryExclusions.Normalize(null));
        Assert.Empty(LongRunningQueryExclusions.Normalize(new[] { "", " " }));
    }

    [Theory]
    [InlineData("SQLAgent - TSQL JobStep", "SQLAgent - TSQL JobStep%")]                 /* the % is the RULE's, appended by the helper */
    [InlineData("svc_", "svc\\_%")]                                                     /* _ escaped, then the prefix % */
    [InlineData("100%", "100\\%%")]                                                     /* a literal % survives, then the prefix % */
    [InlineData("a\\b", "a\\\\b%")]                                                     /* backslash escaped first */
    [InlineData("Rep*Reader", "Rep*Reader%")]                                           /* * is literal to LIKE too */
    public void ToPrefixLikeOperand_EscapesLikesMetacharacters_ThenAppendsThePrefixPercent(string prefix, string expected)
    {
        Assert.Equal(expected, LongRunningQueryExclusions.ToPrefixLikeOperand(prefix));
    }

    [Theory]
    [InlineData(@"NT AUTHORITY\SYSTEM", @"NT AUTHORITY\\SYSTEM")]                      /* the backslash every Windows login carries */
    [InlineData("svc_etl", "svc\\_etl")]                                                /* _ escaped, NO trailing % */
    [InlineData("100%", "100\\%")]
    public void ToExactLikeOperand_EscapesLikesMetacharacters_AndAppendsNothing(string login, string expected)
    {
        Assert.Equal(expected, LongRunningQueryExclusions.ToExactLikeOperand(login));
    }

    [Fact]
    public void BuildSqlPredicates_AreTwoPositiveMatches_WithOperandsNumberedFromTheGivenOrdinal_ProgramsFirst()
    {
        /* Positive rather than negated so the read can filter on NOT (a OR b) and count on each; COALESCE so a
           NULL column is an empty string that matches no non-empty operand; one term per entry, program prefixes
           then logins, operands in the same order as their $n. The seeded knob is the case that matters most. */
        var sql = LongRunningQueryExclusions.Defaults.BuildSqlPredicates("r.program_name", "r.login_name", firstParameterOrdinal: 5);

        Assert.Equal("(COALESCE(r.program_name, '') ILIKE $5 ESCAPE '\\')", sql.ProgramPrefixPredicate);
        Assert.Equal("(COALESCE(r.login_name, '') ILIKE $6 ESCAPE '\\' OR COALESCE(r.login_name, '') ILIKE $7 ESCAPE '\\')", sql.LoginPredicate);
        Assert.Equal(new[] { "SQLAgent - TSQL JobStep%", @"NT AUTHORITY\\SYSTEM", @"NT AUTHORITY\\NETWORK SERVICE" }, sql.Operands);
    }

    [Fact]
    public void BuildSqlPredicates_SpellAnArmWithNoEntriesAsFalse_SoTheReadSplicesUnconditionally()
    {
        /* An empty arm is the literal FALSE, not an empty string: the reads splice both expressions into their
           CTE without a per-arm branch, and with both empty the kept rows are exactly the pre-knob read's. */
        var none = LongRunningQueryExclusions.None.BuildSqlPredicates("p", "l", 5);
        Assert.Equal("FALSE", none.ProgramPrefixPredicate);
        Assert.Equal("FALSE", none.LoginPredicate);
        Assert.Empty(none.Operands);

        var loginsOnly = LongRunningQueryExclusions.From(null, new[] { "erik" }).BuildSqlPredicates("p", "l", 5);
        Assert.Equal("FALSE", loginsOnly.ProgramPrefixPredicate);
        Assert.Equal("(COALESCE(l, '') ILIKE $5 ESCAPE '\\')", loginsOnly.LoginPredicate);   /* the first operand is still $5 */
        Assert.Equal(new[] { "erik" }, loginsOnly.Operands);

        /* #3742: the three-argument overload has no database arm — the literal FALSE, no operands of its own — so a
           caller that never learned about the arm (the PostgreSQL-target read, until #3743) is byte-identical. */
        Assert.Equal("FALSE", none.DatabasePredicate);
        Assert.Equal("FALSE", loginsOnly.DatabasePredicate);
    }

    /// <summary>
    /// #3742: the shared <c>excludedDatabases</c> list is the builder's THIRD arm — the login arm's rule (exact,
    /// case-insensitive, one <c>ILIKE … ESCAPE</c> term per entry, no wildcard grammar) over <c>database_name</c>,
    /// with its operands bound AFTER the logins so a read that already binds the knob's operands from $5 gains the
    /// arm by appending and no ordinal it bound before this arm existed moves. The list is normalised on the way in:
    /// a blank entry must NOT become an <c>ILIKE ''</c> term, because <c>COALESCE(col, '')</c> would then match
    /// exactly the no-database rows the rule has always kept.
    /// </summary>
    [Fact]
    public void BuildSqlPredicates_WithExcludedDatabases_IsAThirdExactArm_BoundAfterTheLogins()
    {
        var sql = LongRunningQueryExclusions.Defaults.BuildSqlPredicates(
            "r.program_name", "r.login_name", "r.database_name", new[] { "ReportingDb", " Stack_Overflow ", "", "reportingdb" }, firstParameterOrdinal: 5);

        /* The knob's two arms are exactly what the three-argument overload returns — $5, $6, $7 — unchanged. */
        Assert.Equal("(COALESCE(r.program_name, '') ILIKE $5 ESCAPE '\\')", sql.ProgramPrefixPredicate);
        Assert.Equal("(COALESCE(r.login_name, '') ILIKE $6 ESCAPE '\\' OR COALESCE(r.login_name, '') ILIKE $7 ESCAPE '\\')", sql.LoginPredicate);
        /* The database arm follows at $8 onward: trimmed, the blank dropped, the case-duplicate dropped, `_` escaped
           (a literal underscore in a database name is not LIKE's single-character wildcard), NO trailing % — exact. */
        Assert.Equal("(COALESCE(r.database_name, '') ILIKE $8 ESCAPE '\\' OR COALESCE(r.database_name, '') ILIKE $9 ESCAPE '\\')", sql.DatabasePredicate);
        Assert.Equal(new[] { "SQLAgent - TSQL JobStep%", @"NT AUTHORITY\\SYSTEM", @"NT AUTHORITY\\NETWORK SERVICE", "ReportingDb", "Stack\\_Overflow" }, sql.Operands);

        /* An empty knob with a database list: the two knob arms FALSE, the database arm the FIRST operand at $5. */
        var databasesOnly = LongRunningQueryExclusions.None.BuildSqlPredicates("p", "l", "d", new[] { "HammerDB" }, 5);
        Assert.Equal("FALSE", databasesOnly.ProgramPrefixPredicate);
        Assert.Equal("FALSE", databasesOnly.LoginPredicate);
        Assert.Equal("(COALESCE(d, '') ILIKE $5 ESCAPE '\\')", databasesOnly.DatabasePredicate);
        Assert.Equal(new[] { "HammerDB" }, databasesOnly.Operands);

        /* Null, empty, and all-blank lists spell the arm FALSE with no operands — a store with no exclusions returns
           exactly the rows it did before the arm existed. */
        foreach (var empty in new[] { null, System.Array.Empty<string>(), new[] { "", "  " } })
        {
            var withoutDatabases = LongRunningQueryExclusions.Defaults.BuildSqlPredicates("p", "l", "d", empty, 5);
            Assert.Equal("FALSE", withoutDatabases.DatabasePredicate);
            Assert.Equal(3, withoutDatabases.Operands.Count);
        }

        /* A null column means "this read has no database arm": the list is ignored, not bound to nothing. */
        var noColumn = LongRunningQueryExclusions.None.BuildSqlPredicates("p", "l", databaseNameColumn: null, new[] { "HammerDB" }, 5);
        Assert.Equal("FALSE", noColumn.DatabasePredicate);
        Assert.Empty(noColumn.Operands);
    }

    [Fact]
    public void ReadResult_ExcludedCount_IsTheTwoKnobArmsSum_AndTheDatabaseCountIsItsOwn()
    {
        /* #3742: ExcludedCount stays the KNOB's receipt — the number its card item has always shown under a heading
           that says "by the opt-out knob" — and the database list's count is a separate field with a separate item,
           so neither heading lies about what its number contains. */
        var result = new LongRunningQueryReadResult(new System.Collections.Generic.List<LongRunningQueryInfo>(), 2, 3, 6);
        Assert.Equal(5, result.ExcludedCount);
        Assert.Equal(6, result.ExcludedByDatabase);
        Assert.Equal(0, LongRunningQueryReadResult.Empty.ExcludedCount);
        Assert.Equal(0, LongRunningQueryReadResult.Empty.ExcludedByDatabase);
        Assert.Empty(LongRunningQueryReadResult.Empty.Sessions);
    }

    /// <summary>
    /// The two SKUs' reads splice the three arms at the same place with the same operand ordinal, both project
    /// <c>login_name</c> and all three counts, and both count DISTINCT sessions with the login count taken AND NOT
    /// the program flag and the database count AND NOT both — source-pinned because the DuckDB read is an
    /// interpolated string inside a WPF-hosted service this Mac cannot load, and the parity is the point: one
    /// knob, one list, one meaning, on both stores.
    /// </summary>
    [Fact]
    public void BothReads_SpliceTheKnobAndTheDatabaseListAheadOfTheCap_AndProjectAllThreeCountsInSessions()
    {
        var lite = ReadRepoFile("Lite", "Services", "LocalDataService.WaitStats.cs");
        var darling = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingAlertReadAdapter.cs");

        foreach (var source in new[] { lite, darling })
        {
            Assert.Contains("BuildSqlPredicates(\"r.program_name\", \"r.login_name\", \"r.database_name\", excludedDatabases, firstParameterOrdinal: 5)", source, StringComparison.Ordinal);
            Assert.Contains("AS excluded_by_program_prefix,", source, StringComparison.Ordinal);
            Assert.Contains("AS excluded_by_login,", source, StringComparison.Ordinal);
            Assert.Contains("AS excluded_by_database", source, StringComparison.Ordinal);
            Assert.Contains("WHERE NOT (r.excluded_by_program_prefix OR r.excluded_by_login OR r.excluded_by_database)", source, StringComparison.Ordinal);
            Assert.Contains("(SELECT COUNT(DISTINCT x.session_id) FROM candidates AS x WHERE x.excluded_by_program_prefix)", source, StringComparison.Ordinal);
            Assert.Contains("(SELECT COUNT(DISTINCT x.session_id) FROM candidates AS x WHERE x.excluded_by_login AND NOT x.excluded_by_program_prefix)", source, StringComparison.Ordinal);
            Assert.Contains("(SELECT COUNT(DISTINCT x.session_id) FROM candidates AS x WHERE x.excluded_by_database AND NOT (x.excluded_by_program_prefix OR x.excluded_by_login))", source, StringComparison.Ordinal);
            Assert.Contains("LIMIT $3", source, StringComparison.Ordinal);
            Assert.Contains("LoginName = reader.IsDBNull(11)", source, StringComparison.Ordinal);
            Assert.Contains("excludedByProgramPrefix = reader.IsDBNull(12)", source, StringComparison.Ordinal);
            Assert.Contains("excludedByLogin = reader.IsDBNull(13)", source, StringComparison.Ordinal);
            Assert.Contains("excludedByDatabase = reader.IsDBNull(14)", source, StringComparison.Ordinal);
            /* No trace of the retired single-flag shape. */
            Assert.DoesNotContain("excluded_by_knob", source, StringComparison.Ordinal);
        }

        /* The flags sit INSIDE the CTE, before the outer LIMIT — the cap is over kept rows only. */
        var template = DarlingAlertReadAdapterTemplate(darling);
        Assert.True(template.IndexOf("{1} AS excluded_by_program_prefix", StringComparison.Ordinal) < template.IndexOf("LIMIT $3", StringComparison.Ordinal));
        Assert.True(template.IndexOf("{2} AS excluded_by_login", StringComparison.Ordinal) < template.IndexOf("LIMIT $3", StringComparison.Ordinal));
        Assert.True(template.IndexOf("{3} AS excluded_by_database", StringComparison.Ordinal) < template.IndexOf("LIMIT $3", StringComparison.Ordinal));
        Assert.True(template.IndexOf("WHERE NOT (r.excluded_by_program_prefix OR r.excluded_by_login OR r.excluded_by_database)", StringComparison.Ordinal) < template.IndexOf("LIMIT $3", StringComparison.Ordinal));
    }

    /// <summary>
    /// #3742's negative half: NEITHER SQL Server-family adapter filters <c>excludedDatabases</c> on the C# side of
    /// the read any more — the <c>.Where(q =&gt; string.IsNullOrEmpty(q.DatabaseName) || …)</c> block that consumed the
    /// page is gone from both. Source-pinned with a POSITIVE control on the same text (the predicate must be
    /// present in the read), so a matcher that quietly stopped matching cannot report a clean bill. The
    /// PostgreSQL-TARGET reader (<c>DarlingPgSessionStatesReader</c>) is not asserted here: its retired helper's
    /// absence and its three-flag shape are <c>DarlingPgSessionStatesReaderTests</c>' and
    /// <c>PgLongRunningQueryExclusionTests</c>' pins, which took the same shape through the same five-argument
    /// overload once #3772 had landed it.
    /// </summary>
    [Fact]
    public void NeitherSqlServerAdapter_FiltersExcludedDatabasesAfterTheRead()
    {
        var liteAdapter = ReadRepoFile("Lite", "Services", "LiteAlertReadAdapter.cs");
        var darlingAdapter = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingAlertReadAdapter.cs");

        foreach (var source in new[] { liteAdapter, darlingAdapter })
        {
            /* The retired block, in the exact spelling both adapters carried. */
            Assert.DoesNotContain(".Where(q => string.IsNullOrEmpty(q.DatabaseName)", source, StringComparison.Ordinal);
            Assert.DoesNotContain("excludedDatabases.Any(", source, StringComparison.Ordinal);
            /* Positive control: the list still reaches the read — as an argument, not as a filter. */
            Assert.Contains("excludedDatabases", source, StringComparison.Ordinal);
        }

        /* Lite's adapter forwards the list INTO the DuckDB read (the data service's new trailing parameter), and the
           data service is where the predicate lives. */
        Assert.Contains("excludeBackups, excludeMiscWaits, excludeCdc, exclusions, excludedDatabases), cancellationToken);", liteAdapter, StringComparison.Ordinal);
        var liteRead = ReadRepoFile("Lite", "Services", "LocalDataService.WaitStats.cs");
        Assert.Contains("IReadOnlyList<string>? excludedDatabases = null)", liteRead, StringComparison.Ordinal);
        Assert.Contains("{exclusionSql.DatabasePredicate} AS excluded_by_database", liteRead, StringComparison.Ordinal);

        /* Darling's template carries the third placeholder and the adapter splices it. */
        Assert.Contains(".Replace(\"{3}\", exclusionSql.DatabasePredicate)", darlingAdapter, StringComparison.Ordinal);
    }

    /// <summary>
    /// Both hosts SEED the knob and both tell an absent key from a present-and-empty one — source-pinned on the
    /// Lite loader (WPF-hosted) and on Darling's settings seam. Lite: the static initialisers are the two
    /// default lists and the settings.json reader replaces a list only when its key is present; the sample
    /// documents the seeds under the renamed keys. Darling (V135): <c>DarlingAlertSettings</c> forwards
    /// <c>AlertsConfig</c>'s lists by reference, and those initialise to the seeds, so both SKUs evaluate the same
    /// population from day one.
    /// </summary>
    [Fact]
    public void BothHosts_SeedTheDefaults_AndLiteTellsAbsentFromPresentAndEmpty()
    {
        var app = ReadRepoFile("Lite", "App.xaml.cs");
        Assert.Contains("AlertLongRunningQueryExcludedProgramNamePrefixes { get; set; } = LongRunningQueryExclusions.DefaultProgramNamePrefixes.ToList();", app, StringComparison.Ordinal);
        Assert.Contains("AlertLongRunningQueryExcludedLogins { get; set; } = LongRunningQueryExclusions.DefaultLogins.ToList();", app, StringComparison.Ordinal);
        Assert.Contains("TryGetProperty(\"alert_long_running_query_excluded_program_name_prefixes\", out v) && v.IsArray()", app, StringComparison.Ordinal);
        Assert.Contains("TryGetProperty(\"alert_long_running_query_excluded_logins\", out v) && v.IsArray()", app, StringComparison.Ordinal);
        Assert.DoesNotContain("alert_long_running_query_excluded_program_names\"", app, StringComparison.Ordinal);

        var sample = ReadRepoFile("Lite", "config", "settings.sample.json");
        Assert.Contains("\"alert_long_running_query_excluded_program_name_prefixes\": [\"SQLAgent - TSQL JobStep\"]", sample, StringComparison.Ordinal);
        Assert.Contains("\"alert_long_running_query_excluded_logins\": [\"NT AUTHORITY\\\\SYSTEM\", \"NT AUTHORITY\\\\NETWORK SERVICE\"]", sample, StringComparison.Ordinal);
        Assert.Contains("admin login", sample, StringComparison.Ordinal);

        /* The Settings window's "defaults" button restores the seeds rather than emptying the boxes. */
        var window = ReadRepoFile("Lite", "Windows", "SettingsWindow.xaml.cs");
        Assert.Contains("AlertLrqExcludedProgramNamePrefixesBox.Text = string.Join(\", \", LongRunningQueryExclusions.DefaultProgramNamePrefixes);", window, StringComparison.Ordinal);
        Assert.Contains("AlertLrqExcludedLoginsBox.Text = string.Join(\", \", LongRunningQueryExclusions.DefaultLogins);", window, StringComparison.Ordinal);
        Assert.Contains("root[\"alert_long_running_query_excluded_program_name_prefixes\"]", window, StringComparison.Ordinal);
        Assert.Contains("root[\"alert_long_running_query_excluded_logins\"]", window, StringComparison.Ordinal);

        /* Darling since V135: by reference through AlertsConfig, whose initialisers are the seeds (the rung's column
           DEFAULT is the same two lists — LongRunningQueryExclusionKnobRungTests pins the three equal). */
        var darling = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingAlertSettings.cs");
        Assert.Contains("LongRunningQueryExcludedProgramNamePrefixes => _config.Alerts.LongRunningQueryExcludedProgramNamePrefixes;", darling, StringComparison.Ordinal);
        Assert.Contains("LongRunningQueryExcludedLogins => _config.Alerts.LongRunningQueryExcludedLogins;", darling, StringComparison.Ordinal);
        var config = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingConfig.cs");
        Assert.Contains("LongRunningQueryExcludedProgramNamePrefixes { get; set; } = LongRunningQueryExclusions.DefaultProgramNamePrefixes.ToList();", config, StringComparison.Ordinal);
        Assert.Contains("LongRunningQueryExcludedLogins { get; set; } = LongRunningQueryExclusions.DefaultLogins.ToList();", config, StringComparison.Ordinal);
    }

    private static string DarlingAlertReadAdapterTemplate(string source)
    {
        var start = source.IndexOf("public const string LongRunningQueriesSqlTemplate", StringComparison.Ordinal);
        Assert.True(start >= 0);
        var end = source.IndexOf("\";", start, StringComparison.Ordinal);
        return source[start..end];
    }
}
