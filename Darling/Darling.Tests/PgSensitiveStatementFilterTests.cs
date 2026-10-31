/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4348 census: <see cref="PgSensitiveStatementFilter"/> holds the ONE definition of the sensitive-statement
/// pattern and placeholder text, and every fetch/collection query that stores a monitored target's statement
/// text verbatim references it (not a copy). The corpus that judges what the pattern actually matches runs
/// against a real PostgreSQL (<c>PgSensitiveStatementFilterLiveTests</c>) — the pattern is POSIX ARE syntax
/// (<c>[[:&lt;:]]</c>/<c>[[:&gt;:]]</c>/<c>[[:space:]]</c>) .NET's <see cref="System.Text.RegularExpressions.Regex"/>
/// cannot parse (confirmed: constructing a <c>Regex</c> from the exact pattern text throws
/// <c>RegexParseException</c> on the POSIX class syntax), so this file only proves the census, not the match
/// behavior.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class PgSensitiveStatementFilterTests
{
    /// <summary>
    /// The stored-text scrub's frozen pattern is the exact text of the shipped version 1 pattern (the dev
    /// definition before the filter was widened), and it is not the live shared pattern: no stored row is
    /// rewritten by the wider filter, so the scrub must keep naming exactly what it named.
    /// </summary>
    [Fact]
    public void TheStoredTextScrubKeepsItsFrozenVersionOnePattern()
    {
        const string gap = "([[:space:]]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^[:cntrl:]]*)";
        const string shippedVersionOne =
            "[[:<:]](create|alter)" + gap + "+(role|user|group|subscription|server)[[:>:]]" +
            "|[[:<:]]password[[:>:]]" + gap + "*(=|to)?" + gap + "*(e?'|u&'|[$][^0-9])" +
            "|[[:<:]](pg)?password[[:space:]]*=[[:space:]]*[^$[:space:]]" +
            "|[a-z][a-z0-9+.-]*://[^[:space:]/@:]+:[^[:space:]/@]+@";

        Assert.Equal(shippedVersionOne, PerformanceMonitor.Darling.Service.PgStatementTextScrub.Version1Pattern);
        Assert.NotEqual(PerformanceMonitor.Common.SensitiveStatements.Pattern,
            PerformanceMonitor.Darling.Service.PgStatementTextScrub.Version1Pattern);
        Assert.Equal(1, PerformanceMonitor.Darling.Service.PgStatementTextScrub.ScrubVersion);
    }

    /// <summary>
    /// <see cref="StoreStatementStats.SensitiveStatementPattern"/> is an alias, not a second copy — same
    /// string reference, so the store and every collector reading a target's own statement text can never
    /// drift apart on what counts as sensitive.
    /// </summary>
    [Fact]
    public void StoreStatementStats_AliasesTheSharedPattern()
    {
        Assert.Same(PgSensitiveStatementFilter.SensitiveStatementPattern, StoreStatementStats.SensitiveStatementPattern);
    }

    /// <summary>
    /// <c>PgStatementText</c>'s fetch queries reference the shared filter's pattern and placeholder text —
    /// literally present in the built SQL, not a re-typed copy.
    /// </summary>
    [Fact]
    public void PgStatementText_FetchSql_ReferencesTheSharedFilter()
    {
        var predicate = PgSensitiveStatementFilter.SqlPredicate("query_text");

        Assert.Contains(predicate, PerformanceMonitor.Darling.Storage.PgStatementText.AuroraFetchSql, StringComparison.Ordinal);
        Assert.Contains(predicate, PerformanceMonitor.Darling.Storage.PgStatementText.VanillaFetchSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// <c>PgBlockingCollector</c>'s query references the shared filter's pattern and placeholder text for
    /// both the blocked and blocking sides.
    /// </summary>
    [Fact]
    public void PgBlockingCollector_QueryText_ReferencesTheSharedFilter()
    {
        var context = new CollectorContext
        {
            ServerId = 1,
            ServerName = "test",
            CollectionTime = DateTime.UtcNow,
            Deltas = null!,
        };
        var query = PgBlockingCollector.Instance.BuildQuery(context).Text;

        Assert.Contains(PgSensitiveStatementFilter.SqlPredicate("blocked.query"), query, StringComparison.Ordinal);
        Assert.Contains(PgSensitiveStatementFilter.SqlPredicate("blocker.query"), query, StringComparison.Ordinal);
        Assert.Equal(2, CountOccurrences(query, PgSensitiveStatementFilter.PlaceholderText));
    }

    /// <summary>
    /// A literal, hard-coded expected string — not built from <see cref="PgSensitiveStatementFilter.SqlLiteral"/>
    /// or <see cref="PgSensitiveStatementFilter.SqlPredicate"/> themselves — so a quoting bug in either shared
    /// builder (say, a dropped doubled quote) changes what this test compares against without changing the
    /// value under test, and fails loudly instead of passing silently the way the other tests' own
    /// <c>Contains(SqlPredicate(...))</c> pins would (they build their expected value with the same builder
    /// they are checking).
    /// </summary>
    [Fact]
    public void SqlPredicate_MatchesTheLiteralExpectedSql()
    {
        // The pattern is typed out by hand here (never built from the shared constants) so a change to the
        // shared pattern fails this test until the expected text is updated on purpose. A1-A4 are the original
        // alternatives, byte-identical; T1-T13 are the appended T-SQL and later alternatives (#4348, #5320).
        const string gap = "([[:space:]]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^[:cntrl:]]*)";
        const string expected =
            "CASE WHEN c ~* '[[:<:]](create|alter)" + gap + "+(role|user|group|subscription|server)[[:>:]]" +
            "|[[:<:]]password[[:>:]]" + gap + "*(=|to)?" + gap + "*(e?''|u&''|[$][^0-9])" +
            "|[[:<:]](pg)?password[[:space:]]*=[[:space:]]*[^$[:space:]]" +
            "|[a-z][a-z0-9+.-]*://[^[:space:]/@:]+:[^[:space:]/@]+@" +
            "|[[:<:]](create|alter)" + gap + "+(login|credential)[[:>:]]" +
            "|[[:<:]]scoped" + gap + "+credential[[:>:]]" +
            "|[[:<:]]([a-z0-9_]*(password|passwd|pwd|secret)|key_source)[[:>:]]" + gap + "*(=|to)?" +
            gap + "*(n?''|e''|u&''|0x|[$][^0-9])" +
            "|[[:<:]]pwd[[:space:]]*=[[:space:]]*[^$@[:space:]]" +
            "|[[:<:]](sp_addlogin|sp_password|sp_addlinkedsrvlogin|sp_addapprole|sp_approlepassword|sp_setapprole" +
            "|sp_change_users_login|sp_adddistributor|sp_changedistributor_password|sp_adddistpublisher" +
            "|sp_addsubscriber|sp_link_publication|sp_control_dbmasterkey_password|sp_xp_cmdshell_proxy_account)[[:>:]]" +
            "|[[:<:]](encryptbypassphrase|decryptbypassphrase|decryptbykeyautocert|decryptbykeyautoasymkey" +
            "|decryptbyasymkey|decryptbycert|signbycert|signbyasymkey|pwdencrypt|pwdcompare)[[:>:]]" +
            "|[[:<:]]opendatasource[[:>:]]" +
            "|[[:<:]]openrowset" + gap + "*[(]" + gap + "*n?''" +
            "|[[:<:]][a-z0-9_]*(password|passwd|pwd|secret)(]|\")" + gap + "*=" + gap + "*(n?''|e''|u&''|0x)" +
            "|[[:<:]]([a-z0-9_]*(password|passwd|pwd|secret)|key_source)" + gap + "+((as|constant)" + gap + "+)?" +
            "[[]?[a-z_][a-z0-9_.]*]?([[:space:]]*[(][[:space:]0-9a-z,]{0,20}[)])?" + gap +
            "*(:?=|default[[:>:]])" + gap + "*(n?''|e''|u&''|0x|[$][^0-9])" +
            "|[[:<:]]sslpassword[[:space:]]*=[[:space:]]*[^$[:space:]]" +
            "|[[:<:]](crypt|hmac|encrypt|decrypt|encrypt_iv|decrypt_iv|pgp_sym_encrypt|pgp_sym_encrypt_bytea" +
            "|pgp_sym_decrypt|pgp_sym_decrypt_bytea|pgp_pub_encrypt|pgp_pub_encrypt_bytea|pgp_pub_decrypt" +
            "|pgp_pub_decrypt_bytea)" + gap + "*[(]([^()]|[(][^()]*[)])*(''|[$][^0-9])" +
            "|[[:<:]](bcp|sqlcmd|osql|isql)[[:>:]].*[[:space:]]-p[[:space:]]*[^[:space:]-]' " +
            "THEN '-- statement text withheld (#4348)' ELSE c END";

        Assert.Equal(expected, PgSensitiveStatementFilter.SqlPredicate("c"));
    }

    private static int CountOccurrences(string haystack, string needle)
    {
        var count = 0;
        var index = 0;
        while ((index = haystack.IndexOf(needle, index, StringComparison.Ordinal)) >= 0)
        {
            count++;
            index += needle.Length;
        }

        return count;
    }
}
