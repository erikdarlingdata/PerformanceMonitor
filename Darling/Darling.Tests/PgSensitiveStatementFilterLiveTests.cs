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
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The shared filter's (#4348) corpus against PostgreSQL's own regex engine, not
/// <c>System.Text.RegularExpressions</c>.
///
/// <para><b>Why live.</b> <see cref="PgSensitiveStatementFilter.SensitiveStatementPattern"/> is a POSIX ARE:
/// its <c>[[:&lt;:]]</c>/<c>[[:&gt;:]]</c> word boundaries and <c>[[:space:]]</c>/<c>[[:cntrl:]]</c> classes
/// are not rejected by .NET's <c>Regex</c> constructor, but they are not honored by it either — a construction
/// check confirms the pattern builds without throwing, then a match check against the same statement text
/// (<c>ALTER ROLE app PASSWORD 'x'</c>) comes back <c>false</c>, the opposite of what the <c>~*</c> operator
/// every caller actually runs returns. The corpus is therefore judged the same way
/// <c>StoreStatementStatsLiveTests</c> judges #3915's patterns: against a scratch PostgreSQL connection, with
/// no server bootstrap or extension needed.</para>
///
/// <para><b>#1776 own-store</b> — mints its own scratch database (<see cref="ScratchPostgres"/>) rather than
/// sharing the live fixture, so it is deliberately NOT in the <c>live-postgres</c> collection.</para>
/// </summary>
public sealed class PgSensitiveStatementFilterLiveTests
{
    /// <summary>
    /// The T-SQL corpus (#4348): every statement the shared pattern must name, each also lower-cased and with
    /// a leading block comment, and the statements it must leave alone, judged in PostgreSQL's own regex
    /// engine (the engine that decides every stored PostgreSQL statement). The adversarial strings the
    /// .NET evaluation bounds with a budget are NOT in this parity set: they are only timed here.
    /// </summary>
    [Fact]
    public async Task TheTSqlCorpusIsNamedAndTheNeighboursAreNot_InPostgresOwnRegexEngine()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString),
            "Set DARLING_TEST_PG to a connection string to judge the #4348 pattern in PostgreSQL's regex engine.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        var failures = new List<string>();
        foreach (var text in SensitiveStatementCorpus.Named)
        {
            foreach (var variant in SensitiveStatementCorpus.Variants(text))
            {
                if (!await IsNamedAsync(connection, variant, ct))
                {
                    failures.Add("should be named: " + variant);
                }
            }
        }

        foreach (var text in SensitiveStatementCorpus.NotNamed)
        {
            if (await IsNamedAsync(connection, text, ct))
            {
                failures.Add("should NOT be named: " + text);
            }
        }

        Assert.True(failures.Count == 0, string.Join(Environment.NewLine, failures));
    }

    /// <summary>
    /// A 1,000,000-character ordinary statement is not named and comes back quickly, and the two strings the
    /// .NET side bounds with a time budget are timed (not compared) in PostgreSQL's engine, which backtracks
    /// differently. Each answer and time is written to the test's diagnostics so a run records it.
    /// </summary>
    [Fact]
    public async Task ALargeOrdinaryStatementIsNotNamed_AndTheAdversarialStringsAreTimedInPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString),
            "Set DARLING_TEST_PG to a connection string to judge the #4348 pattern in PostgreSQL's regex engine.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        var unit = "SELECT col_a, col_b FROM dbo.t WHERE c = 1 AND d <> 2 ";
        var big = new StringBuilder(1_000_000 + unit.Length);
        while (big.Length < 1_000_000)
        {
            big.Append(unit);
        }

        var bigText = big.ToString(0, 1_000_000);
        var watch = Stopwatch.StartNew();
        var bigNamed = await IsNamedAsync(connection, bigText, ct);
        watch.Stop();
        TestContext.Current.SendDiagnosticMessage(string.Create(CultureInfo.InvariantCulture,
            $"pg ~* on 1,000,000 chars: named={bigNamed}, {watch.ElapsedMilliseconds} ms"));
        Assert.False(bigNamed);
        Assert.True(watch.ElapsedMilliseconds < 5000, $"1,000,000-char statement took {watch.ElapsedMilliseconds} ms");

        var adversarial = SensitiveStatementCorpus.Adversarial;
        foreach (var text in adversarial)
        {
            watch.Restart();
            var named = await IsNamedAsync(connection, text, ct);
            watch.Stop();
            TestContext.Current.SendDiagnosticMessage(string.Create(CultureInfo.InvariantCulture,
                $"pg ~* on adversarial '{text.Substring(0, 8)}...': named={named}, {watch.ElapsedMilliseconds} ms"));
        }
    }

    private static async Task<bool> IsNamedAsync(NpgsqlConnection connection, string text, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("SELECT $1 ~* $2", connection);
        command.Parameters.AddWithValue(text);
        command.Parameters.AddWithValue(PgSensitiveStatementFilter.SensitiveStatementPattern);
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }
}
