/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7730: the closed list of the monitor's old statements (<see cref="SqlServerOwnStatement"/>). The match is the whole text with its
/// whitespace removed, so changed whitespace still matches and a text that differs by one token does not. The collector's drop and
/// the key trigger's test must agree on every one of these, so the Postgres test is run over the same texts.
/// </summary>
public sealed class SqlServerOwnStatementTests
{
    private static string Respace(string text) =>
        text.Replace("\r\n", "\n", StringComparison.Ordinal).Replace("\n", "\r\n\t  ", StringComparison.Ordinal).Replace(" ", "\t ", StringComparison.Ordinal);

    [Fact]
    public void EveryOldText_IsRecognized_WithItsWhitespaceChanged()
    {
        Assert.NotEmpty(SqlServerOwnStatement.OldTexts);
        foreach (var text in SqlServerOwnStatement.OldTexts)
        {
            Assert.False(text.Contains(QueryStoreCollector.SelfQueryMarker, StringComparison.Ordinal), "an old text carries no marker");
            Assert.True(SqlServerOwnStatement.IsOldStatement(text));
            Assert.True(SqlServerOwnStatement.IsOldStatement(Respace(text)));
            Assert.True(SqlServerOwnStatement.IsOldStatement(SqlServerOwnStatement.StripWhitespace(text)));
            Assert.True(SqlServerOwnStatement.IsOwnStatement(Respace(text)));
        }
    }

    [Fact]
    public void ATextOneTokenOff_OrWithSomethingAdded_IsNotRecognized()
    {
        foreach (var text in SqlServerOwnStatement.OldTexts)
        {
            var oneTokenOff = text.Contains("= 8", StringComparison.Ordinal) ? text.Replace("= 8", "= 0", StringComparison.Ordinal)
                : text.Contains("TOP (500)", StringComparison.Ordinal) ? text.Replace("TOP (500)", "TOP (501)", StringComparison.Ordinal)
                : text.Contains("= 0", StringComparison.Ordinal) ? text.Replace("= 0", "= 1", StringComparison.Ordinal)
                : text.Replace("JOIN", "LEFT JOIN", StringComparison.Ordinal);
            Assert.NotEqual(text, oneTokenOff);
            Assert.False(SqlServerOwnStatement.IsOldStatement(oneTokenOff));
            Assert.False(SqlServerOwnStatement.IsOldStatement(text + "\nOPTION(MAXDOP 1)"));
            Assert.False(SqlServerOwnStatement.IsOldStatement("SELECT 1 " + text));
        }

        Assert.False(SqlServerOwnStatement.IsOldStatement(null));
        Assert.False(SqlServerOwnStatement.IsOldStatement(""));
        Assert.False(SqlServerOwnStatement.IsOwnStatement("SELECT 1"));
        Assert.True(SqlServerOwnStatement.IsOwnStatement("SELECT /* " + QueryStoreCollector.SelfQueryMarker + " */ 1"));
    }

    [Fact]
    public async Task ThePostgresTest_AgreesWithTheCsharpOne_OnTheListAndOnNearMisses()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_PG")), "Set DARLING_TEST_PG to a Postgres connection string to run the #7730 live tests.");
        var ct = TestContext.Current.CancellationToken;
        var texts = SqlServerOwnStatement.OldTexts
            .SelectMany(t => new[] { t, Respace(t), t.Replace("1", "2", StringComparison.Ordinal), t + " ", t + "\nOPTION(MAXDOP 1)" })
            .Append("SELECT 1")
            .Append("SELECT /* " + QueryStoreCollector.SelfQueryMarker + " */ 1")
            .Append(new string(' ', SqlServerOwnStatement.MaxRawLength + 1) + SqlServerOwnStatement.OldTexts[0])
            .ToList();

        /* #1776 own-store: the database is made and dropped through ScratchPostgres. */
        await using var scratch = await ScratchPostgres.CreateAsync(Environment.GetEnvironmentVariable("DARLING_TEST_PG")!, ct, plainPostgres: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        var sql = "SELECT " + QueryStoreSelfStatement.SelfOrOldOfText("$1") + ", " + QueryStoreSelfStatement.SelfOrOldOfText("NULL::text");
        foreach (var text in texts)
        {
            await using var command = new NpgsqlCommand(sql, connection);
            command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = text });
            await using var reader = await command.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct));
            Assert.Equal(SqlServerOwnStatement.IsOwnStatement(text), reader.GetBoolean(0));
            Assert.False(reader.GetBoolean(1), "a NULL text is not the monitor's own");
        }

        Assert.Contains(texts, SqlServerOwnStatement.IsOwnStatement);
        Assert.Contains(texts, t => !SqlServerOwnStatement.IsOwnStatement(t));
    }
}
