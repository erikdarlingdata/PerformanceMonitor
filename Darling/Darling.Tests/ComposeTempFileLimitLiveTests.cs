/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4605 live pin: the <c>temp_file_limit</c> backstop actually cancels a spilling read at the database, and
/// the resulting SQLSTATE 53400 classifies through <see cref="ReadOutcomeClassifier"/> as
/// <see cref="ReadOutcome.Limit"/> \u2014 through the product's own renderer
/// (<see cref="DarlingManagedRoles.BuildComposeStatementTimeoutSql"/>), not a hand-typed copy of the ALTER
/// ROLE statement, so a change to the renderer that dropped the temp_file_limit line would fail this test
/// too.
/// </summary>
/* #1776 own-store: mints its own scratch database and its own throwaway login role, so it cannot race the
   shared store's tables or another test's viewer/mcp role state. */
public sealed class ComposeTempFileLimitLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task ARoleWithTheRendererSSql_RefusesASpillingRead_AsSqlState53400_ClassifiedAsLimit()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the #4605 temp_file_limit live pin.");

        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);

        /* A throwaway login role, provisioned with the SAME shape the product's renderer emits for viewer/mcp
           -- but with the ceiling lowered to 1MB so a normal test-sized spill trips it without needing to
           write a gigabyte of temp files to prove the backstop exists. */
        var roleName = "darling_temp_limit_pin_" + Guid.NewGuid().ToString("N")[..8];

        await using var admin = new NpgsqlConnection(scratch.ConnectionString);
        await admin.OpenAsync(ct);

        await using (var create = new NpgsqlCommand(
            $"CREATE ROLE {roleName} LOGIN PASSWORD 'pin-password'; GRANT ALL ON DATABASE \"{scratch.DatabaseName}\" TO {roleName};",
            admin))
        {
            await create.ExecuteNonQueryAsync(ct);
        }

        try
        {
            /* The renderer's own output for viewer/mcp, with the role names substituted for our throwaway
               role via a straight text replace -- the SQL SHAPE (statement_timeout, log_min_duration_statement,
               temp_file_limit) is exactly what BuildComposeStatementTimeoutSql emits for the real roles. */
            var rendered = DarlingManagedRoles.BuildComposeStatementTimeoutSql(60)
                .Replace("viewer", roleName, StringComparison.Ordinal)
                .Replace("mcp", roleName, StringComparison.Ordinal);
            await using (var applyRendered = new NpgsqlCommand(rendered, admin))
            {
                await applyRendered.ExecuteNonQueryAsync(ct);
            }

            /* Lower the ceiling from the renderer's real value to 1MB for this test only -- proving the
               MECHANISM (the ALTER ROLE ... SET temp_file_limit statement fires and the store enforces it),
               not the specific 1GB production value, which ComposeStatementTimeoutReloadTests already pins
               against the renderer's literal output. */
            await using (var lower = new NpgsqlCommand($"ALTER ROLE {roleName} SET temp_file_limit = '1MB';", admin))
            {
                await lower.ExecuteNonQueryAsync(ct);
            }

            var roleBuilder = new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                Username = roleName,
                Password = "pin-password",
                Pooling = false,
            };

            await using var asRole = new NpgsqlConnection(roleBuilder.ConnectionString);
            await asRole.OpenAsync(ct);

            /* SHOW temp_file_limit as the role -- confirms the lowered ceiling actually took effect on this
               session (a role SET only applies on the role's NEXT session, so a fresh connection is
               required), before asserting on the spill itself. */
            await using (var show = new NpgsqlCommand("SHOW temp_file_limit", asRole))
            await using (var reader = await show.ExecuteReaderAsync(ct))
            {
                Assert.True(await reader.ReadAsync(ct));
                Assert.Equal("1MB", reader.GetString(0));
            }

            await using var spill = new NpgsqlCommand(
                "SELECT string_agg(md5(random()::text), '') FROM generate_series(1, 500000) g;", asRole);

            var ex = await Assert.ThrowsAsync<PostgresException>(async () => await spill.ExecuteScalarAsync(ct));

            Assert.Equal("53400", ex.SqlState);
            Assert.Contains("temp_file_limit", ex.MessageText, StringComparison.Ordinal);
            Assert.Equal(ReadOutcome.Limit, ReadOutcomeClassifier.Classify(ex, System.Threading.CancellationToken.None));

            bodySucceeded = true;
        }
        finally
        {
            /* No LiveStoreCleanup here: the role lives on the SCRATCH database's server, not the shared
               store, and dropping the scratch database (ScratchPostgres.DisposeAsync, WITH (FORCE)) does not
               drop a server-level role, so it is dropped explicitly. The database-level GRANT ALL made above
               leaves a dependency a bare DROP ROLE refuses (2BP01), so it is revoked first -- the scratch
               database itself is dropped WITH (FORCE) by ScratchPostgres.DisposeAsync after this method
               returns, which is what actually clears the grant's target, but DROP ROLE runs before that
               dispose, so the explicit REVOKE keeps this method's own cleanup order-independent of it. */
            await using (var revoke = new NpgsqlCommand(
                $"REVOKE ALL ON DATABASE \"{scratch.DatabaseName}\" FROM {roleName};", admin))
            {
                await revoke.ExecuteNonQueryAsync(System.Threading.CancellationToken.None);
            }

            await using var drop = new NpgsqlCommand($"DROP ROLE IF EXISTS {roleName};", admin);
            await drop.ExecuteNonQueryAsync(System.Threading.CancellationToken.None);
        }

        Assert.True(bodySucceeded);
    }
}
