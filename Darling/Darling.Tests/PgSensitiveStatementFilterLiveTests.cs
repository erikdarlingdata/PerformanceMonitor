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

        var named = new[]
        {
            "CREATE LOGIN [app] WITH PASSWORD = N'S3cret'",
            "ALTER LOGIN [app] WITH PASSWORD = 0x0200AB HASHED",
            "ALTER LOGIN [app] DISABLE",
            "CREATE/* x */LOGIN [a] WITH PASSWORD = 'p'",
            "CREATE\r\nLOGIN [a] FROM WINDOWS",
            "EXEC sp_addlogin 'app', 'S3cret'",
            "EXEC master..sp_password NULL, 'S3cret', 'app'",
            "EXEC sp_addlinkedsrvlogin 'SRV', 'false', NULL, 'u', 'S3cret'",
            "EXEC sp_setapprole 'r', 'S3cret'",
            "OPEN SYMMETRIC KEY k DECRYPTION BY PASSWORD = 'S3cret'",
            "OPEN MASTER KEY DECRYPTION BY PASSWORD = N'S3cret'",
            "CREATE MASTER KEY ENCRYPTION BY PASSWORD = N'S3cret'",
            "CREATE CREDENTIAL c WITH IDENTITY = 'u', SECRET = 'S3cret'",
            "CREATE DATABASE SCOPED CREDENTIAL c WITH IDENTITY = 'SHARED ACCESS SIGNATURE', SECRET = 'sv=1&sig=x'",
            "BACKUP CERTIFICATE c TO FILE = 'f' WITH PRIVATE KEY (FILE = 'k', ENCRYPTION BY PASSWORD = 'S3cret')",
            "RESTORE DATABASE d FROM DISK = 'f' WITH MEDIAPASSWORD = 'S3cret'",
            "CREATE SYMMETRIC KEY k WITH KEY_SOURCE = 'phrase', ALGORITHM = AES_256 ENCRYPTION BY CERTIFICATE c",
            "SELECT * FROM OPENROWSET('MSOLEDBSQL', 'Server=h;UID=u;PWD=S3cret;', 'SELECT 1')",
            "SELECT * FROM OPENDATASOURCE('MSOLEDBSQL', 'Data Source=h;User ID=u;Pwd=S3cret').db.dbo.t",
            "EXEC sp_addlinkedserver @server = 'S', @provider = 'MSOLEDBSQL', @provstr = 'UID=u;PWD=S3cret'",
            "EXEC sp_addpushsubscription_agent @publication = N'p', @job_password = N'S3cret'",
            "ALTER SERVICE MASTER KEY WITH OLD_ACCOUNT = 'a', OLD_PASSWORD = 'S3cret'",
            "ALTER APPLICATION ROLE r WITH PASSWORD = N'S3cret'",
            "SELECT DECRYPTBYPASSPHRASE('phrase', @c)",
            "exec sp_executesql N'UPDATE dbo.t SET c = @pwd', N'@pwd nvarchar(50)', @pwd = N'S3cret'",
            "UPDATE dbo.Creds SET [password] = N'S3cret'",
            "UPDATE t SET \"password\" = 'S3cret'",
            "UPDATE dbo.Users SET pwd = N'S3cret' WHERE id = 7",
            "UPDATE dbo.Users SET password = @p",
            // the widening the version-2 re-scrub applies to stored PostgreSQL text
            "SELECT 1 WHERE client_secret = 'S3cret'",
        };

        var notNamed = new[]
        {
            "SELECT name FROM sys.sql_logins",
            "EXECUTE AS LOGIN = N'app'",
            "SELECT * FROM dbo.Users WHERE PasswordHash = @h",
            "UPDATE dbo.Users SET PasswordHash = HASHBYTES('SHA2_256', @p)",
            "SELECT * FROM OPENROWSET(BULK N'f.json', SINGLE_CLOB) AS j",
            "EXEC sp_helplogins",
            "SELECT secret_id FROM dbo.t WHERE secret_id = 5",
            "SELECT * FROM t WHERE pwd = @p",
            "SELECT * FROM t WHERE pwd = $1",
            "SELECT [password] FROM t",
            "SET password_encryption = 'scram-sha-256'",
            "ALTER INDEX ix ON dbo.t REBUILD",
            "BACKUP DATABASE d TO URL = 'https://storage.example/c/d.bak' WITH CREDENTIAL = 'cred'",
            "ALTER AVAILABILITY GROUP ag FAILOVER",
        };

        var failures = new List<string>();
        foreach (var text in named)
        {
            foreach (var variant in new[] { text, text.ToLowerInvariant(), "/* c */ " + text })
            {
                if (!await IsNamedAsync(connection, variant, ct))
                {
                    failures.Add("should be named: " + variant);
                }
            }
        }

        foreach (var text in notNamed)
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

        var adversarial = new[]
        {
            "create" + string.Concat(Enumerable.Repeat(" --", 40)) + "x",
            "password" + string.Concat(Enumerable.Repeat(" --", 40)) + "x",
        };
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
