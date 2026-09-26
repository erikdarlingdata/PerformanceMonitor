/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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
    /// Each of the five statement forms the pattern exists to catch, each also with a leading comment and in
    /// lower case, plus statements that must NOT match: normalized DML, a non-credential setting, a bare
    /// SELECT, and DDL merely naming a table "passwords".
    /// </summary>
    [Fact]
    public async Task ThePatternMatchesTheFiveCredentialFormsAndNothingElse()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString),
            "Set DARLING_TEST_PG to a connection string to judge the #4348 pattern in PostgreSQL's regex engine.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        var cases = new (string Text, bool Sensitive)[]
        {
            ("ALTER ROLE app PASSWORD 'secret-x'", true),
            ("/* c */ ALTER ROLE app PASSWORD 'secret-x'", true),
            ("alter role app password 'secret-x'", true),

            ("CREATE ROLE r LOGIN PASSWORD 'secret-x'", true),
            ("/* c */ CREATE ROLE r LOGIN PASSWORD 'secret-x'", true),
            ("create role r login password 'secret-x'", true),

            ("CREATE USER MAPPING FOR u SERVER s OPTIONS (user 'u', password 'secret-x')", true),
            ("/* c */ CREATE USER MAPPING FOR u SERVER s OPTIONS (user 'u', password 'secret-x')", true),
            ("create user mapping for u server s options (user 'u', password 'secret-x')", true),

            ("ALTER SYSTEM SET primary_conninfo = 'host=h password=secret-x'", true),
            ("/* c */ ALTER SYSTEM SET primary_conninfo = 'host=h password=secret-x'", true),
            ("alter system set primary_conninfo = 'host=h password=secret-x'", true),

            ("CREATE SUBSCRIPTION sub CONNECTION 'host=h password=secret-x' PUBLICATION p", true),
            ("/* c */ CREATE SUBSCRIPTION sub CONNECTION 'host=h password=secret-x' PUBLICATION p", true),
            ("create subscription sub connection 'host=h password=secret-x' publication p", true),

            // role/user/group/subscription/server DDL is withheld whole, whatever it sets
            ("ALTER ROLE app SET work_mem = '64MB'", true),

            ("CREATE USER MAPPING FOR u SERVER s OPTIONS (user 'u', secret_access_key 'secret-x')", true),
            ("ALTER SERVER s OPTIONS (ADD token 'secret-x')", true),
            ("CREATE SERVER s FOREIGN DATA WRAPPER w OPTIONS (api_key 'secret-x')", true),

            ("SELECT * FROM t WHERE password_changed_at > $1", false),
            ("SET work_mem = '64MB'", false),
            ("SELECT rolname FROM pg_roles", false),
            ("SELECT 1", false),
            ("CREATE TABLE passwords (id int)", false),
        };

        foreach (var (text, sensitive) in cases)
        {
            await using var command = new NpgsqlCommand("SELECT $1 ~* $2", connection);
            command.Parameters.AddWithValue(text);
            command.Parameters.AddWithValue(PgSensitiveStatementFilter.SensitiveStatementPattern);
            var actual = (bool)(await command.ExecuteScalarAsync(ct))!;
            Assert.True(sensitive == actual, $"sensitive({text}) should be {sensitive}, was {actual}");
        }
    }
}
