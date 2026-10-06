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

    private static async Task<bool> IsNamedAsync(NpgsqlConnection connection, string text, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("SELECT $1 ~* $2", connection);
        command.Parameters.AddWithValue(text);
        command.Parameters.AddWithValue(SensitiveStatements.Pattern);
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }
}
