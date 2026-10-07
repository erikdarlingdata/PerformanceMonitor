/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Parity of the two engines (#4348): PostgreSQL's <c>~*</c> against the shared pattern and
/// <see cref="SensitiveStatements.Names"/> must agree on every string of the corpus. The adversarial strings
/// are not part of the parity set: PostgreSQL's answer and time are recorded for them, without an equality
/// assertion.
///
/// <para><b>#1776 own-store</b> - mints its own scratch database (<see cref="ScratchPostgres"/>) rather than
/// sharing the live fixture, so it is deliberately NOT in the <c>live-postgres</c> collection.</para>
/// </summary>
public sealed class SensitiveStatementsParityLiveTests
{
    [Fact]
    public async Task PostgresAndTheDotNetEvaluationAgreeOnTheCorpus()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString),
            "Set DARLING_TEST_PG to a connection string to compare the #4348 pattern in both engines.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        var disagreements = new List<string>();
        var checkedCount = 0;
        var all = new List<(string Text, bool Expected)>();
        foreach (var text in SensitiveStatementCorpus.Named)
        {
            foreach (var variant in SensitiveStatementCorpus.Variants(text))
            {
                all.Add((variant, true));
            }
        }

        foreach (var text in SensitiveStatementCorpus.NotNamed)
        {
            all.Add((text, false));
        }

        foreach (var (text, expected) in all)
        {
            var pg = await IsNamedAsync(connection, text, ct);
            var dotnet = SensitiveStatements.Names(text);
            checkedCount++;
            if (pg != dotnet || pg != expected)
            {
                disagreements.Add($"pg={pg} dotnet={dotnet} expected={expected}: {text}");
            }
        }

        TestContext.Current.SendDiagnosticMessage(string.Create(CultureInfo.InvariantCulture,
            $"parity: {checkedCount} strings, {disagreements.Count} disagreements"));
        Assert.True(disagreements.Count == 0, string.Join(Environment.NewLine, disagreements));
    }

    [Fact]
    public async Task PostgresAnswersTheAdversarialStringsQuickly_RecordedNotCompared()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString),
            "Set DARLING_TEST_PG to a connection string to time the #4348 adversarial strings in PostgreSQL.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        foreach (var text in SensitiveStatementCorpus.Adversarial)
        {
            var watch = Stopwatch.StartNew();
            var named = await IsNamedAsync(connection, text, ct);
            watch.Stop();
            TestContext.Current.SendDiagnosticMessage(string.Create(CultureInfo.InvariantCulture,
                $"pg ~* on adversarial '{text.Substring(0, 8)}...': named={named}, {watch.ElapsedMilliseconds} ms"));
        }
    }

    /// <summary>The strings aimed at the typed-declaration alternative (#5320) carry no literal, so both engines
    /// answer Clean, and PostgreSQL answers them quickly.</summary>
    [Fact]
    public async Task PostgresAndTheDotNetEvaluationAgreeOnTheTypedDeclarationAdversarialStrings()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString),
            "Set DARLING_TEST_PG to a connection string to compare the #5320 typed-declaration strings in both engines.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        foreach (var text in SensitiveStatementCorpus.TypedDeclarationAdversarial)
        {
            var watch = Stopwatch.StartNew();
            var pg = await IsNamedAsync(connection, text, ct);
            watch.Stop();
            var dotnet = SensitiveStatements.Names(text);
            TestContext.Current.SendDiagnosticMessage(string.Create(CultureInfo.InvariantCulture,
                $"pg ~* on typed-declaration string '{text.Substring(0, 8)}...': named={pg}, {watch.ElapsedMilliseconds} ms"));

            Assert.False(pg, text.Substring(0, 24));
            Assert.Equal(dotnet, pg);
            Assert.True(watch.ElapsedMilliseconds < 2000, $"{watch.ElapsedMilliseconds} ms: {text.Substring(0, 24)}");
        }
    }

    /// <summary>The one known divergence (#5320 L2): a typed secret-named variable followed by a dash banner, in a
    /// value that also passes the pre-check. PostgreSQL answers Clean, quickly; the .NET judge runs out of its time
    /// on the backtracking comment run and names the value (TimedOut). The row pins both answers.</summary>
    [Fact]
    public async Task PostgresAnswersTheDashBannerRowCleanWhereTheDotNetJudgeNamesItByTimeout()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString),
            "Set DARLING_TEST_PG to a connection string to compare the #5320 dash-banner row in both engines.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        foreach (var text in SensitiveStatementCorpus.NamedByTimeoutInDotNetOnly)
        {
            var watch = Stopwatch.StartNew();
            var pg = await IsNamedAsync(connection, text, ct);
            watch.Stop();

            Assert.False(pg, text.Substring(0, 24));
            Assert.True(watch.ElapsedMilliseconds < 2000, $"{watch.ElapsedMilliseconds} ms: {text.Substring(0, 24)}");
            Assert.Equal(SensitiveStatements.Verdict.TimedOut, SensitiveStatements.Judge(text));
            Assert.True(SensitiveStatements.Names(text));
        }
    }

    /// <summary>A U+212A (Kelvin sign) next to a named keyword is not a word character under the C collation,
    /// and the .NET word class is case-sensitive so it is not one there either (#5320 L1). PostgreSQL's
    /// answer depends on the cluster's locale, so the match runs with <c>COLLATE "C"</c> and the fact runs
    /// on every cluster, whatever its character type (#5320 N2). Not part of the corpus: lower-casing the
    /// sign gives an ASCII k.</summary>
    [Fact]
    public async Task AKelvinSignNextToANamedKeywordGetsTheSameVerdictInBothEngines_UnderTheCCollation()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString),
            "Set DARLING_TEST_PG to a connection string to compare the #5320 Kelvin-sign case in both engines.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        const string text = "x \u212Asp_addlogin 'a'";
        var pg = await IsNamedAsync(connection, text, ct, cCollation: true);
        var dotnet = SensitiveStatements.Names(text);
        TestContext.Current.SendDiagnosticMessage(string.Create(CultureInfo.InvariantCulture,
            $"kelvin sign: pg={pg} dotnet={dotnet}"));

        Assert.True(dotnet);
        Assert.Equal(dotnet, pg);
    }

    private static async Task<bool> IsNamedAsync(NpgsqlConnection connection, string text, CancellationToken ct, bool cCollation = false)
    {
        await using var command = new NpgsqlCommand(cCollation ? "SELECT $1 COLLATE \"C\" ~* $2" : "SELECT $1 ~* $2", connection);
        command.Parameters.AddWithValue(text);
        command.Parameters.AddWithValue(SensitiveStatements.Pattern);
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }
}
