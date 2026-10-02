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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The PostgreSQL Long-Running Query twin honours the program/login opt-out knob (#3743, closing the gap
/// #3734 / #3736 stated): the knob that was visible, editable and read back on a PostgreSQL target — and did
/// nothing there — now rides INTO <see cref="DarlingPgSessionStatesReader.GetCurrentLongRunningSessionsAsync"/>
/// through the SHARED <see cref="LongRunningQueryExclusions.BuildSqlPredicates"/> translation, over
/// <c>application_name</c> (the <c>program_name</c> twin) and <c>username</c> (<c>pg_stat_activity.usename</c>,
/// the <c>login_name</c> twin), ahead of <c>LIMIT $4</c>, with the two removed-by-arm counts beside the rows.
///
/// <para><b>Source pins against the builder's OUTPUT, never a hand-typed predicate.</b> The whole point of
/// the shared builder is that an entry means one thing on both engines; a test that spelled the
/// <c>ILIKE … ESCAPE '\'</c> text by hand would pass while the reader drifted from the builder. So every
/// shape assertion here renders the reader's SQL through the same builder call the host makes and asserts
/// the builder's text is IN it, at the builder's ordinals.</para>
///
/// <para><b>#3742: the shared <c>excludedDatabases</c> list is the CTE's third flag.</b> Until #3742 this
/// reader applied the list in C# after <c>LIMIT $4</c> — the exact shape the knob refused, and the one #3772
/// fixed on the two SQL Server reads — so an excluded database's sessions consumed the page and the alert came
/// back short or empty while matches existed. The list now rides the SAME builder call's five-argument
/// overload over <c>s.database_name</c> (exact, case-insensitive, operands bound after the login arm's), the
/// outer <c>WHERE</c> drops it with the knob's two, and its count is the third scalar, <c>AND NOT</c> both knob
/// flags. Pinned here through the builder like the knob's arms, and live with the mutation the issue names.</para>
///
/// <para>The unit half runs everywhere; the live half is <c>DARLING_TEST_PG</c>-gated and plants a capture
/// with the three shapes the knob has to tell apart — an ETL worker matched by program prefix, a report
/// runner matched by login only, and a real long user query matched by neither — and a second capture where
/// an excluded database holds every one of the longest sessions.</para>
/// </summary>
public class PgLongRunningQueryExclusionTests
{
    /// <summary>The host's exact builder call (<c>DarlingWorker.EvaluatePgLongRunningQueryAsync</c>): the
    /// five-argument overload over the reader's three column constants, the database list last.</summary>
    private static LongRunningQueryExclusionSql Build(LongRunningQueryExclusions knob, IReadOnlyList<string>? excludedDatabases = null) =>
        knob.BuildSqlPredicates(
            DarlingPgSessionStatesReader.ExclusionProgramNameColumn,
            DarlingPgSessionStatesReader.ExclusionLoginNameColumn,
            DarlingPgSessionStatesReader.ExclusionDatabaseNameColumn,
            excludedDatabases,
            DarlingPgSessionStatesReader.ExclusionFirstParameterOrdinal);

    private static string Render(LongRunningQueryExclusions knob, bool excludeBackups = true, IReadOnlyList<string>? excludedDatabases = null)
    {
        var sql = Build(knob, excludedDatabases);
        return DarlingPgSessionStatesReader.BuildCurrentLongRunningSessionsSql(excludeBackups, sql.ProgramPrefixPredicate, sql.LoginPredicate, sql.DatabasePredicate);
    }

    /// <summary>
    /// The two arms reach the shipped SQL as the builder spells them, over this engine's two columns, with the
    /// operands numbered from the reader's first free ordinal — <c>$5</c>, after <c>$1</c> server, <c>$2</c>
    /// threshold, <c>$3</c> recency floor, <c>$4</c> limit, all four of which are still there.
    /// </summary>
    [Fact]
    public void TheKnobsTwoArms_ReachTheShippedSql_ThroughTheSharedBuilder_AtTheReadersOrdinals()
    {
        var knob = LongRunningQueryExclusions.From(new[] { "etl-", "Replicator " }, new[] { "svc_etl" });
        var built = Build(knob);
        var sql = Render(knob);

        Assert.Equal("s.application_name", DarlingPgSessionStatesReader.ExclusionProgramNameColumn);
        Assert.Equal("s.username", DarlingPgSessionStatesReader.ExclusionLoginNameColumn);
        Assert.Equal(5, DarlingPgSessionStatesReader.ExclusionFirstParameterOrdinal);

        Assert.Contains(built.ProgramPrefixPredicate + " AS excluded_by_program_prefix", sql, StringComparison.Ordinal);
        Assert.Contains(built.LoginPredicate + " AS excluded_by_login", sql, StringComparison.Ordinal);
        Assert.Contains("COALESCE(s.application_name, '') ILIKE $5 ESCAPE '\\'", sql, StringComparison.Ordinal);
        Assert.Contains("COALESCE(s.application_name, '') ILIKE $6 ESCAPE '\\'", sql, StringComparison.Ordinal);
        Assert.Contains("COALESCE(s.username, '') ILIKE $7 ESCAPE '\\'", sql, StringComparison.Ordinal);
        /* Normalise trims the entry first — "Replicator " is the prefix "Replicator", then the rule's %. */
        Assert.Equal(new[] { "etl-%", "Replicator%", @"svc\_etl" }, built.Operands);

        /* The four fixed parameters are untouched — the knob's operands FOLLOW them. */
        Assert.Contains("server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("query_duration_ms >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $3", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $4", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("{1}", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("{2}", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("{3}", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// #3742: the shared <c>excludedDatabases</c> list reaches the shipped SQL as the builder's
    /// <c>DatabasePredicate</c> over <c>s.database_name</c> — the login arm's rule (exact, case-insensitive,
    /// no trailing <c>%</c>), one <c>ILIKE</c> term per entry — spliced as <c>excluded_by_database</c>, with its
    /// operands numbered AFTER the login arm's so no ordinal that existed before this arm moves (the #3736 trap,
    /// walked once). The outer filter names all three flags ahead of <c>LIMIT $4</c>, and the third count
    /// scalar is taken <c>AND NOT</c> both knob flags so the three counts sum to the sessions removed.
    /// </summary>
    [Fact]
    public void TheDatabaseList_IsTheThirdFlag_ThroughTheSharedBuilder_BoundAfterTheLoginArm()
    {
        var knob = LongRunningQueryExclusions.From(new[] { "etl-" }, new[] { "svc_etl" });
        var databases = new[] { "Reporting", " staging_db " };
        var built = Build(knob, databases);
        var sql = Render(knob, excludedDatabases: databases);

        Assert.Equal("s.database_name", DarlingPgSessionStatesReader.ExclusionDatabaseNameColumn);
        Assert.Contains(built.DatabasePredicate + " AS excluded_by_database", sql, StringComparison.Ordinal);
        Assert.Contains("COALESCE(s.application_name, '') ILIKE $5 ESCAPE '\\'", sql, StringComparison.Ordinal);
        Assert.Contains("COALESCE(s.username, '') ILIKE $6 ESCAPE '\\'", sql, StringComparison.Ordinal);
        Assert.Contains("COALESCE(s.database_name, '') ILIKE $7 ESCAPE '\\'", sql, StringComparison.Ordinal);
        Assert.Contains("COALESCE(s.database_name, '') ILIKE $8 ESCAPE '\\'", sql, StringComparison.Ordinal);
        /* Exact, not prefix: the database operands carry no trailing %, and Normalize trimmed the entry. */
        Assert.Equal(new[] { "etl-%", @"svc\_etl", "Reporting", @"staging\_db" }, built.Operands);

        var filter = sql.IndexOf("WHERE NOT (s.excluded_by_program_prefix OR s.excluded_by_login OR s.excluded_by_database)", StringComparison.Ordinal);
        var limit = sql.IndexOf("LIMIT $4", StringComparison.Ordinal);
        Assert.True(filter >= 0, "the three-flag filter is missing");
        Assert.True(limit > filter, "the database list must be applied ahead of the row cap");
        Assert.Contains(
            "CAST((SELECT count(*) FROM candidates AS x WHERE x.excluded_by_database AND NOT (x.excluded_by_program_prefix OR x.excluded_by_login)) AS integer) AS excluded_by_database_count",
            sql, StringComparison.Ordinal);

        /* A list with no knob still binds from $5 — the arms are one ordinal sequence, not three. */
        var listOnly = Build(LongRunningQueryExclusions.None, new[] { "Reporting" });
        Assert.Contains("COALESCE(s.database_name, '') ILIKE $5 ESCAPE '\\'", listOnly.DatabasePredicate, StringComparison.Ordinal);
        Assert.Equal(new[] { "Reporting" }, listOnly.Operands);
    }

    /// <summary>
    /// <c>%</c> and <c>_</c> in an entry are characters, not wildcards: the shared helper escapes them before
    /// they reach <c>ILIKE</c>, and the operand the reader binds is the helper's output, so the PostgreSQL
    /// read cannot grow a wildcard grammar the SQL Server read does not have.
    /// </summary>
    [Fact]
    public void PercentAndUnderscore_AreEscapedByTheSharedHelper_NotInterpreted()
    {
        var knob = LongRunningQueryExclusions.From(new[] { "etl_%" }, new[] { "svc_etl" });
        var built = Build(knob);

        Assert.Equal(LongRunningQueryExclusions.ToPrefixLikeOperand("etl_%"), built.Operands[0]);
        Assert.Equal(@"etl\_\%%", built.Operands[0]);
        Assert.Equal(LongRunningQueryExclusions.ToExactLikeOperand("svc_etl"), built.Operands[1]);
        Assert.Equal(@"svc\_etl", built.Operands[1]);
        Assert.Contains("ESCAPE '\\'", Render(knob), StringComparison.Ordinal);
    }

    /// <summary>
    /// <c>[]</c> excludes nothing, and says so in the text: both arms render as the builder's <c>FALSE</c>, the
    /// two shipped field renderings (which the parse census checks) ARE that empty-knob text byte for byte,
    /// and the empty knob binds no operand. This is the shape a store whose operator cleared both lists sends.
    /// </summary>
    [Fact]
    public void AnEmptyKnob_RendersBothArmsFalse_AndIsTheShippedFieldText()
    {
        var built = Build(LongRunningQueryExclusions.None);

        Assert.Equal(DarlingPgSessionStatesReader.NoExclusionPredicate, built.ProgramPrefixPredicate);
        Assert.Equal(DarlingPgSessionStatesReader.NoExclusionPredicate, built.LoginPredicate);
        Assert.Equal(DarlingPgSessionStatesReader.NoExclusionPredicate, built.DatabasePredicate);
        Assert.Empty(built.Operands);
        /* #3742: an EMPTY database list (the shipped default) and a null one are the same FALSE arm, so a store
           with no exclusions sends byte-for-byte the field text. */
        Assert.Equal(DarlingPgSessionStatesReader.NoExclusionPredicate, Build(LongRunningQueryExclusions.None, Array.Empty<string>()).DatabasePredicate);
        Assert.Equal(DarlingPgSessionStatesReader.CurrentLongRunningSessionsSql, Render(LongRunningQueryExclusions.None, excludedDatabases: Array.Empty<string>()));

        Assert.Equal(DarlingPgSessionStatesReader.CurrentLongRunningSessionsSql, Render(LongRunningQueryExclusions.None));
        Assert.Equal(DarlingPgSessionStatesReader.CurrentLongRunningSessionsSqlBackupsIncluded, Render(LongRunningQueryExclusions.None, excludeBackups: false));
        Assert.Equal(DarlingPgSessionStatesReader.BuildCurrentLongRunningSessionsSql(excludeBackups: true), Render(LongRunningQueryExclusions.None));
        Assert.Contains("FALSE AS excluded_by_program_prefix", DarlingPgSessionStatesReader.CurrentLongRunningSessionsSql, StringComparison.Ordinal);
        Assert.Contains("FALSE AS excluded_by_login", DarlingPgSessionStatesReader.CurrentLongRunningSessionsSql, StringComparison.Ordinal);
        Assert.Contains("FALSE AS excluded_by_database", DarlingPgSessionStatesReader.CurrentLongRunningSessionsSql, StringComparison.Ordinal);
        Assert.DoesNotContain("ILIKE", DarlingPgSessionStatesReader.CurrentLongRunningSessionsSql, StringComparison.Ordinal);
        Assert.DoesNotContain("$5", DarlingPgSessionStatesReader.CurrentLongRunningSessionsSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The SQL Server read's shape, mirrored: a <c>candidates</c> CTE flags every over-threshold row once per
    /// arm; the outer query filters <c>WHERE NOT (program OR login)</c> AHEAD of <c>LIMIT $4</c> (a permanent
    /// ETL worker is the longest session by construction and would otherwise fill the cap); the two counts
    /// are scalars over the same CTE, the login one taken <c>AND NOT</c> the program flag so a session
    /// matching both arms counts once. <c>count(*)</c> rather than <c>COUNT(DISTINCT session_id)</c>: one row
    /// per backend per capture here, and <c>backend_id</c> / <c>pid</c> are nullable on a redacted target.
    /// </summary>
    [Fact]
    public void TheRead_FiltersAheadOfTheCap_AndCountsEachArmOnce()
    {
        var sql = DarlingPgSessionStatesReader.CurrentLongRunningSessionsSql;

        Assert.Contains("candidates AS (", sql, StringComparison.Ordinal);
        Assert.Contains("FROM candidates AS s", sql, StringComparison.Ordinal);
        var filter = sql.IndexOf("WHERE NOT (s.excluded_by_program_prefix OR s.excluded_by_login OR s.excluded_by_database)", StringComparison.Ordinal);
        var limit = sql.IndexOf("LIMIT $4", StringComparison.Ordinal);
        Assert.True(filter >= 0, "the knob's filter is missing");
        Assert.True(limit > filter, "the knob must be applied ahead of the row cap");

        Assert.Contains("CAST((SELECT count(*) FROM candidates AS x WHERE x.excluded_by_program_prefix) AS integer) AS excluded_by_program_prefix_count", sql, StringComparison.Ordinal);
        Assert.Contains("CAST((SELECT count(*) FROM candidates AS x WHERE x.excluded_by_login AND NOT x.excluded_by_program_prefix) AS integer) AS excluded_by_login_count", sql, StringComparison.Ordinal);
        Assert.Contains("CAST((SELECT count(*) FROM candidates AS x WHERE x.excluded_by_database AND NOT (x.excluded_by_program_prefix OR x.excluded_by_login)) AS integer) AS excluded_by_database_count", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("COUNT(DISTINCT", sql, StringComparison.OrdinalIgnoreCase);

        /* The pre-knob filters all still apply — INSIDE the CTE, so a flagged row is an over-threshold client
           backend that would otherwise have been evaluated, and the counts are of sessions the alert would
           have seen. */
        var cte = sql[..sql.IndexOf("FROM candidates AS s", StringComparison.Ordinal)];
        Assert.Contains("s.is_idle_in_transaction = false", cte, StringComparison.Ordinal);
        Assert.Contains("coalesce(s.backend_type, 'client backend') = 'client backend'", cte, StringComparison.Ordinal);
        Assert.Contains("coalesce(s.command_tag, '') NOT IN ('VACUUM', 'ANALYZE', 'REINDEX', 'CLUSTER')", cte, StringComparison.Ordinal);
        Assert.Contains(DarlingPgSessionStatesReader.BackupUtilitiesFilter, cte, StringComparison.Ordinal);
    }

    /// <summary>
    /// A blank arm text is a programming error, not an empty knob — an empty arm is the builder's
    /// <c>FALSE</c>, and splicing <c>""</c> would ship <c> AS excluded_by_program_prefix</c> with no expression.
    /// </summary>
    [Fact]
    public void ABlankArm_IsRefused()
    {
        Assert.ThrowsAny<ArgumentException>(() => DarlingPgSessionStatesReader.BuildCurrentLongRunningSessionsSql(true, "", "FALSE", "FALSE"));
        Assert.ThrowsAny<ArgumentException>(() => DarlingPgSessionStatesReader.BuildCurrentLongRunningSessionsSql(true, "FALSE", " ", "FALSE"));
        Assert.ThrowsAny<ArgumentException>(() => DarlingPgSessionStatesReader.BuildCurrentLongRunningSessionsSql(true, "FALSE", "FALSE", ""));
    }

    /// <summary>
    /// The host threads the knob from the SAME two settings members the SQL Server arm reads (the V135
    /// columns through <c>DarlingAlertSettings</c>), normalises it with the same <c>From</c>, translates it
    /// with the same builder over the reader's three constants, and hands the reader the rendered text.
    /// Single-line anchors on the raw file, so no LF-reader roster entry is needed.
    /// </summary>
    [Fact]
    public void TheHost_ThreadsTheKnobFromTheSharedSettings_ThroughTheSharedBuilder()
    {
        var worker = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        var start = worker.IndexOf("private async Task EvaluatePgLongRunningQueryAsync(", StringComparison.Ordinal);
        var end = worker.IndexOf("internal static AlertIncident BuildPgLongRunningQueryIncident(", start, StringComparison.Ordinal);
        Assert.True(start >= 0 && end > start, "the PostgreSQL LRQ evaluator moved");
        var evaluator = worker[start..end];

        Assert.Contains("var exclusions = LongRunningQueryExclusions.From(", evaluator, StringComparison.Ordinal);
        Assert.Contains("alertSettings.LongRunningQueryExcludedProgramNamePrefixes, alertSettings.LongRunningQueryExcludedLogins);", evaluator, StringComparison.Ordinal);
        Assert.Contains("var exclusionSql = exclusions.BuildSqlPredicates(", evaluator, StringComparison.Ordinal);
        Assert.Contains("DarlingPgSessionStatesReader.ExclusionProgramNameColumn,", evaluator, StringComparison.Ordinal);
        Assert.Contains("DarlingPgSessionStatesReader.ExclusionLoginNameColumn,", evaluator, StringComparison.Ordinal);
        /* #3742: the shared database list goes through the SAME call — the five-argument overload, over the
           reader's database column, from the same first ordinal — not a helper of its own. */
        Assert.Contains("DarlingPgSessionStatesReader.ExclusionDatabaseNameColumn,", evaluator, StringComparison.Ordinal);
        Assert.Contains("alertSettings.ExcludedDatabases,", evaluator, StringComparison.Ordinal);
        Assert.Contains("DarlingPgSessionStatesReader.ExclusionFirstParameterOrdinal);", evaluator, StringComparison.Ordinal);
        Assert.Contains("programPrefixPredicate: exclusionSql.ProgramPrefixPredicate,", evaluator, StringComparison.Ordinal);
        Assert.Contains("loginPredicate: exclusionSql.LoginPredicate,", evaluator, StringComparison.Ordinal);
        Assert.Contains("databasePredicate: exclusionSql.DatabasePredicate,", evaluator, StringComparison.Ordinal);
        Assert.Contains("exclusionOperands: exclusionSql.Operands,", evaluator, StringComparison.Ordinal);
        Assert.Contains("alertSettings.ExcludedDatabases, read.ExcludedByDatabase),", evaluator, StringComparison.Ordinal);
        Assert.Contains("exclusions, read.ExcludedByProgramPrefix, read.ExcludedByLogin,", evaluator, StringComparison.Ordinal);
        /* The list is no longer a reader argument the reader filters on after the read. */
        Assert.DoesNotContain("excludedDatabases: alertSettings.ExcludedDatabases,", evaluator, StringComparison.Ordinal);
        /* The fire is decided on the rows the read returned — the receipt annotates, never suppresses. */
        Assert.Contains("var rows = read.Sessions;", evaluator, StringComparison.Ordinal);
        Assert.Contains("_activePgLongRunningQueryAlert[key] = rows.Count > 0;", evaluator, StringComparison.Ordinal);
    }

    /// <summary>
    /// The receipt on the PostgreSQL card is the SQL Server twin's item — same builder, same labels, same
    /// counts-once arithmetic — and is absent when the knob is empty, <c>AlertEngine</c>'s rule.
    /// </summary>
    [Fact]
    public void TheReceipt_IsTheSqlServerTwinsItem_AndAbsentForAClearedKnob()
    {
        var knob = LongRunningQueryExclusions.From(new[] { "etl-" }, new[] { "svc_etl" });
        var noDatabases = Array.Empty<string>();
        var details = DarlingWorker.BuildPgLongRunningQueryExclusionDetails(knob, excludedByProgramPrefix: 2, excludedByLogin: 1, noDatabases, excludedByDatabase: 0);

        var item = Assert.Single(details);
        var expected = AlertContextBuilders.BuildLongRunningQueryExclusionItem(knob, 2, 1);
        Assert.Equal(expected.Heading, item.Heading);
        Assert.Equal(expected.Fields, item.Fields);
        Assert.Contains(item.Fields, f => f.Label == AlertContextBuilders.LongRunningQueryExcludedCountLabel && f.Value == "3");
        Assert.Contains(item.Fields, f => f.Label == AlertContextBuilders.LongRunningQueryExcludedByProgramPrefixLabel && f.Value == "2");
        Assert.Contains(item.Fields, f => f.Label == AlertContextBuilders.LongRunningQueryExcludedByLoginLabel && f.Value == "1");
        /* The knob's count stays the knob's two arms — the database count never folds into it. */
        Assert.DoesNotContain(item.Fields, f => f.Label == AlertContextBuilders.LongRunningQueryExcludedByDatabaseLabel);

        Assert.Empty(DarlingWorker.BuildPgLongRunningQueryExclusionDetails(LongRunningQueryExclusions.None, 0, 0, noDatabases, 0));
    }

    /// <summary>
    /// #3742: the database list's receipt is the SQL Server twin's second item
    /// (<see cref="AlertContextBuilders.BuildLongRunningQueryExcludedDatabasesItem"/>), appended AFTER the
    /// knob's, only when the list is SET — and then even at 0, so an operator who named a database reads that
    /// the absence was nothing — and absent when the list is empty, which it is by default, so a fresh install's
    /// card is byte-identical. A set list with a cleared knob renders the database item alone.
    /// </summary>
    [Fact]
    public void TheDatabaseReceipt_IsTheSqlServerTwinsItem_AfterTheKnobs_OnlyWhenTheListIsSet()
    {
        var knob = LongRunningQueryExclusions.From(new[] { "etl-" }, null);
        var databases = new[] { "Reporting", "staging" };

        var both = DarlingWorker.BuildPgLongRunningQueryExclusionDetails(knob, 1, 0, databases, excludedByDatabase: 6);
        Assert.Equal(2, both.Count);
        Assert.Equal(AlertContextBuilders.BuildLongRunningQueryExclusionItem(knob, 1, 0).Heading, both[0].Heading);
        var expected = AlertContextBuilders.BuildLongRunningQueryExcludedDatabasesItem(databases, 6);
        Assert.Equal(expected.Heading, both[1].Heading);
        Assert.Equal(expected.Fields, both[1].Fields);
        Assert.Contains(both[1].Fields, f => f.Label == AlertContextBuilders.LongRunningQueryExcludedByDatabaseLabel && f.Value == "6");
        Assert.Contains(both[1].Fields, f => f.Label == "Excluded Databases" && f.Value == "Reporting, staging");

        var listOnly = DarlingWorker.BuildPgLongRunningQueryExclusionDetails(LongRunningQueryExclusions.None, 0, 0, databases, excludedByDatabase: 0);
        var item = Assert.Single(listOnly);
        Assert.Contains(item.Fields, f => f.Label == AlertContextBuilders.LongRunningQueryExcludedByDatabaseLabel && f.Value == "0");

        Assert.Throws<ArgumentNullException>(() => DarlingWorker.BuildPgLongRunningQueryExclusionDetails(knob, 0, 0, null!, 0));
    }

    /// <summary>The README no longer says the PostgreSQL twin ignores the knob, and says what it does instead.</summary>
    [Fact]
    public void TheReadme_SaysThePostgresTwinConsultsTheKnob()
    {
        var readme = RepoFile.ReadRepoFile("Darling", "README.md");

        Assert.DoesNotContain("The PostgreSQL twin does not consult the knob", readme, StringComparison.Ordinal);
        Assert.Contains("The PostgreSQL twin consults the same knob through the same shared predicate builder (#3743)", readme, StringComparison.Ordinal);
        Assert.Contains("`application_name` is the `program_name` twin and `usename` (stored as `username`) the `login_name` twin", readme, StringComparison.Ordinal);
        /* The engine-coverage table row names the knob on the PostgreSQL side too. */
        Assert.Contains("the #3653 opt-out knob over `application_name` / `usename` through the same shared predicate builder and ahead of the same row cap (#3743", readme, StringComparison.Ordinal);
        /* #3742: and no longer says the database list is applied after the read. */
        Assert.DoesNotContain("`excludedDatabases` after the read", readme, StringComparison.Ordinal);
        Assert.Contains("`excludedDatabases` through the same builder's database arm, ahead of the same row cap (#3742)", readme, StringComparison.Ordinal);
        Assert.Contains("the PostgreSQL twin applies all three exclusions in the read", readme, StringComparison.Ordinal);
    }
}

/// <summary>
/// The knob against a real store (<c>DARLING_TEST_PG</c>): one <c>pg_session_states</c> capture with a
/// permanent ETL worker (<c>application_name = 'etl-worker'</c>, <c>username = 'svc_etl'</c> — matches BOTH
/// arms, and must count ONCE under the prefix), a report runner under the service role
/// (<c>application_name = 'report-runner'</c>, <c>username = 'svc_etl'</c> — login arm only) and a real long
/// user query (<c>psql</c> / <c>analyst</c> — neither). With the knob set (prefix <c>etl-</c>, login
/// <c>svc_etl</c>) the read returns only the user query and counts 1 / 1; with <c>[]</c> it returns all three
/// and counts 0 / 0; with the knob set and a cap of ONE the user query is still what comes back, because the
/// exclusion runs ahead of the cap and the ETL worker (the longest) never competes for it.
/// </summary>
[Collection("live-postgres")]
public sealed class PgLongRunningQueryExclusionLiveTests
{
    private const string ServerName = "pg-lrq-exclusion-live-test";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    [Fact]
    public async Task TheKnob_RemovesTheBackground_AheadOfTheCap_AndCountsByArm()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a PostgreSQL connection string to run the live knob test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
            var captureAt = now.AddMinutes(-1);
            var collectionId = CollectionIdGenerator.Next();
            /* Longest first, the way the read orders: the ETL worker is the longest by construction. */
            await PlantAsync(connection, collectionId, captureAt, backendId: 1, pid: 5001, "etl-worker", "svc_etl", durationMs: 180 * 60_000L, ct);
            await PlantAsync(connection, collectionId, captureAt, backendId: 2, pid: 5002, "report-runner", "svc_etl", durationMs: 120 * 60_000L, ct);
            await PlantAsync(connection, collectionId, captureAt, backendId: 3, pid: 5003, "psql", "analyst", durationMs: 45 * 60_000L, ct);

            var set = LongRunningQueryExclusions.From(new[] { "etl-" }, new[] { "svc_etl" });
            var knobbed = await ReadAsync(postgres, now, set, limit: 5, ct);
            Assert.Equal(new long[] { 3 }, knobbed.Sessions.Select(s => s.BackendId).ToArray());
            Assert.Equal("analyst", knobbed.Sessions[0].Username);
            Assert.Equal(1, knobbed.ExcludedByProgramPrefix);
            Assert.Equal(1, knobbed.ExcludedByLogin);

            var open = await ReadAsync(postgres, now, LongRunningQueryExclusions.None, limit: 5, ct);
            Assert.Equal(new long[] { 1, 2, 3 }, open.Sessions.Select(s => s.BackendId).ToArray());
            Assert.Equal(0, open.ExcludedByProgramPrefix);
            Assert.Equal(0, open.ExcludedByLogin);

            /* Ahead of the cap: with room for ONE row the user query is what comes back, not the ETL worker. */
            var capped = await ReadAsync(postgres, now, set, limit: 1, ct);
            Assert.Equal(new long[] { 3 }, capped.Sessions.Select(s => s.BackendId).ToArray());
            Assert.Equal(1, capped.ExcludedByProgramPrefix);
            Assert.Equal(1, capped.ExcludedByLogin);

            /* Case-insensitive on both arms, and a prefix is a prefix: 'ETL-' and 'SVC_ETL' remove the same two. */
            var shouted = await ReadAsync(postgres, now, LongRunningQueryExclusions.From(new[] { "ETL-" }, new[] { "SVC_ETL" }), limit: 5, ct);
            Assert.Equal(new long[] { 3 }, shouted.Sessions.Select(s => s.BackendId).ToArray());

            /* No wildcard grammar: an underscore in an entry is a character. 'svc_etl' matches; 'svcXetl' shaped
               entries do not, and an entry 'svc%' matches nothing because % is escaped. */
            var literal = await ReadAsync(postgres, now, LongRunningQueryExclusions.From(null, new[] { "svc%" }), limit: 5, ct);
            Assert.Equal(3, literal.Sessions.Count);
            Assert.Equal(0, literal.ExcludedByLogin);

            /* The SQL Server seeds match nothing here, by construction. */
            var seeded = await ReadAsync(postgres, now, LongRunningQueryExclusions.Defaults, limit: 5, ct);
            Assert.Equal(3, seeded.Sessions.Count);
            Assert.Equal(0, seeded.ExcludedByProgramPrefix + seeded.ExcludedByLogin);

            /* #3742: with no database list set, the database count is 0 on every read above — the arm is FALSE. */
            Assert.Equal(0, knobbed.ExcludedByDatabase);
            Assert.Equal(0, open.ExcludedByDatabase);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// #3742's E2E, in #3772's shape: SIX over-threshold sessions in an excluded database (<c>ReportingDb</c>,
    /// 600–605 minutes) sit above every real one (five in <c>appdb</c> at 100–140 minutes, plus a shorter
    /// sixth so the cap has something to cut), plus an ETL worker in the excluded database that the knob's
    /// prefix arm also matches, and a row with NO database name. At the shipped cap of 5 with the list set the
    /// page is the five longest REAL sessions and <c>ExcludedByDatabase</c> is 6 — the ETL worker counts under
    /// the prefix, not the database (the arm is last); without the list the same cap holds five
    /// <c>ReportingDb</c> rows, the exact rows the retired post-read filter read and threw away, leaving a page
    /// that was empty while six real long-running sessions existed. The list is exact and case-insensitive
    /// (<c>REPORTINGDB</c> removes the same rows; <c>Reporting</c> removes none), and the no-database row is kept
    /// under a list that names every other database.
    ///
    /// <para><b>Mutation, executed here:</b> with the reader's <c>WHERE</c> restored to the two-flag shape and the
    /// C# <c>FilterExcludedDatabases</c> put back after the cap, the first assertion fails with the page holding
    /// nothing but <c>appdb</c>'s sixth row or nothing at all — the page-of-five went to the excluded rows and
    /// the filter emptied it. That is the issue's lie, and the assertion that catches it.</para>
    /// </summary>
    [Fact]
    public async Task TheDatabaseList_RemovesTheExcludedDatabase_AheadOfTheCap_AndCountsIt()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a PostgreSQL connection string to run the live excluded-databases test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
            var captureAt = now.AddMinutes(-1);
            var collectionId = CollectionIdGenerator.Next();
            /* Six excluded-database sessions, every one longer than every real one. */
            for (var i = 0; i < 6; i++)
            {
                await PlantAsync(connection, collectionId, captureAt, backendId: 100 + i, pid: 6100 + i, "report-runner", "reporting", durationMs: (600 + i) * 60_000L, ct, databaseName: "ReportingDb");
            }
            /* The ETL worker in the excluded database — the knob's prefix arm matches it too; it is the knob's. */
            await PlantAsync(connection, collectionId, captureAt, backendId: 200, pid: 6200, "etl-worker", "svc_etl", durationMs: 700 * 60_000L, ct, databaseName: "ReportingDb");
            /* Six real sessions: five long ones and a shorter sixth the cap cuts. */
            for (var i = 0; i < 5; i++)
            {
                await PlantAsync(connection, collectionId, captureAt, backendId: 300 + i, pid: 6300 + i, "psql", "analyst", durationMs: (100 + i * 10) * 60_000L, ct);
            }
            await PlantAsync(connection, collectionId, captureAt, backendId: 305, pid: 6305, "psql", "analyst", durationMs: 40 * 60_000L, ct);
            /* A session on no database at all, longer than the real ones — kept, whatever the list names. */
            await PlantAsync(connection, collectionId, captureAt, backendId: 400, pid: 6400, "psql", "analyst", durationMs: 150 * 60_000L, ct, databaseName: null);

            var list = new[] { "ReportingDb" };
            var knob = LongRunningQueryExclusions.From(new[] { "etl-" }, null);

            /* The shipped cap with the list set: the no-database row and the five longest appdb rows fill the
               page in duration order; the sixth appdb row falls to the cap, the seven ReportingDb rows never
               compete for it. */
            var withList = await ReadAsync(postgres, now, knob, limit: 5, ct, list);
            Assert.Equal(new long[] { 400, 304, 303, 302, 301 }, withList.Sessions.Select(s => s.BackendId).ToArray());
            Assert.All(withList.Sessions, s => Assert.NotEqual("ReportingDb", s.DatabaseName));
            Assert.Equal(6, withList.ExcludedByDatabase);
            Assert.Equal(1, withList.ExcludedByProgramPrefix);
            Assert.Equal(0, withList.ExcludedByLogin);

            /* Without the list: the same cap holds the excluded database's rows — the rows the retired shape
               read and then threw away. */
            var withoutList = await ReadAsync(postgres, now, knob, limit: 5, ct, excludedDatabases: null);
            Assert.Equal(new long[] { 105, 104, 103, 102, 101 }, withoutList.Sessions.Select(s => s.BackendId).ToArray());
            Assert.All(withoutList.Sessions, s => Assert.Equal("ReportingDb", s.DatabaseName));
            Assert.Equal(0, withoutList.ExcludedByDatabase);
            Assert.Equal(1, withoutList.ExcludedByProgramPrefix);

            /* A cleared knob: the ETL worker is now the database list's to count — seven. */
            var listOnly = await ReadAsync(postgres, now, LongRunningQueryExclusions.None, limit: 5, ct, list);
            Assert.Equal(new long[] { 400, 304, 303, 302, 301 }, listOnly.Sessions.Select(s => s.BackendId).ToArray());
            Assert.Equal(7, listOnly.ExcludedByDatabase);
            Assert.Equal(0, listOnly.ExcludedByProgramPrefix);

            /* Exact and case-insensitive: the shouted name removes the same rows; a prefix of the name removes none. */
            var shouted = await ReadAsync(postgres, now, LongRunningQueryExclusions.None, limit: 5, ct, new[] { "REPORTINGDB" });
            Assert.Equal(new long[] { 400, 304, 303, 302, 301 }, shouted.Sessions.Select(s => s.BackendId).ToArray());
            Assert.Equal(7, shouted.ExcludedByDatabase);
            var prefix = await ReadAsync(postgres, now, LongRunningQueryExclusions.None, limit: 5, ct, new[] { "Reporting" });
            Assert.Equal(0, prefix.ExcludedByDatabase);
            Assert.Equal(200, prefix.Sessions[0].BackendId);

            /* A list naming every database that exists keeps the row on none of them. */
            var everything = await ReadAsync(postgres, now, LongRunningQueryExclusions.None, limit: 5, ct, new[] { "ReportingDb", "appdb" });
            Assert.Equal(new long[] { 400 }, everything.Sessions.Select(s => s.BackendId).ToArray());
            Assert.Null(everything.Sessions[0].DatabaseName);
            Assert.Equal(13, everything.ExcludedByDatabase);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>The host's call, verbatim: the five-argument builder over the reader's three column
    /// constants, the database list last, the reader handed the three arm texts and the one operand list.</summary>
    private static Task<DarlingPgSessionStatesReader.LongRunningSessionsReadResult> ReadAsync(
        NpgsqlDataSource postgres, DateTime nowUtc, LongRunningQueryExclusions knob, int limit, CancellationToken ct,
        IReadOnlyList<string>? excludedDatabases = null)
    {
        var sql = knob.BuildSqlPredicates(
            DarlingPgSessionStatesReader.ExclusionProgramNameColumn,
            DarlingPgSessionStatesReader.ExclusionLoginNameColumn,
            DarlingPgSessionStatesReader.ExclusionDatabaseNameColumn,
            excludedDatabases,
            DarlingPgSessionStatesReader.ExclusionFirstParameterOrdinal);
        return DarlingPgSessionStatesReader.GetCurrentLongRunningSessionsAsync(
            postgres, ServerId, thresholdMs: 30 * 60_000L, nowUtc, recencyMinutes: 30, limit,
            excludeBackups: true,
            programPrefixPredicate: sql.ProgramPrefixPredicate, loginPredicate: sql.LoginPredicate,
            databasePredicate: sql.DatabasePredicate, exclusionOperands: sql.Operands,
            ct);
    }

    private static async Task PlantAsync(
        NpgsqlConnection connection, long collectionId, DateTime at, long backendId, int pid,
        string applicationName, string username, long durationMs, CancellationToken ct, string? databaseName = "appdb")
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_session_states
    (collection_id, collection_time, server_id, server_name, backend_id, pid, database_name, username, application_name, client_addr,
     backend_type, state, wait_event_type, wait_event, command_tag, query_id,
     state_duration_ms, xact_duration_ms, query_duration_ms, backend_duration_ms, xmin_age, xid_age, horizon_age,
     is_idle_in_transaction, is_horizon_holder, state_is_redacted,
     total_sessions, active_sessions, idle_in_transaction_sessions, reportable_sessions)
VALUES ($1, $2, $3, $4, $5, $6, $10, $7, $8, NULL,
        'client backend', 'active', NULL, NULL, 'SELECT', NULL,
        $9, $9, $9, $9, -1, -1, -1,
        false, false, false,
        3, 3, 0, 3)", connection);
        command.Parameters.AddWithValue(collectionId);
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(backendId);
        command.Parameters.AddWithValue(pid);
        command.Parameters.AddWithValue(username);
        command.Parameters.AddWithValue(applicationName);
        command.Parameters.AddWithValue(durationMs);
        command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Text, (object?)databaseName ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var command = new NpgsqlCommand($"DELETE FROM pg_session_states WHERE server_id = {ServerId}", connection);
        await command.ExecuteNonQueryAsync(ct);
    }
}
