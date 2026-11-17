/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8737: <see cref="PersonalTokenStore"/> on a live store migrated to V272, on the owner connection: create under the cap, the name rule and the
/// advisory lock, the lists without the hash, revoke, the settings sweep, the last-use stamp, the journal, the retention purge and the missing table.
/// </summary>
[Collection("live-postgres")]
public sealed class PersonalTokenStoreLiveTests
{
    private const string Actor = "person:alice";

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the personal token store live tests.");
        return connectionString!;
    }

    private static async Task<string> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture)!;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task WithStoreAsync(Func<NpgsqlConnection, NpgsqlDataSource, CancellationToken, Task> body)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(RequireLivePostgres(), ct, emptyStore: true);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await using var owner = NpgsqlDataSource.Create(scratch.ConnectionString);
            await body(connection, owner, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>Creates a token through the store with a freshly minted id and hash; returns its id.</summary>
    private static async Task<string> MakeAsync(NpgsqlDataSource owner, string ownerKey, string name, string role, DateTimeOffset now, CancellationToken ct,
        int lifetimeDays = 30, int cap = 5, string actor = Actor)
    {
        var minted = PersonalTokenFormat.Mint();
        var outcome = await PersonalTokenStore.CreateAsync(owner, minted.TokenId, minted.SecretSha256, ownerKey, "subject-" + ownerKey, name, role, now,
            lifetimeDays, cap, actor, ct);
        Assert.Equal(PersonalTokenCreateOutcome.Created, outcome);
        return minted.TokenId;
    }

    [Fact]
    public Task Create_ThenList_ReturnsRowWithoutHash() => WithStoreAsync(async (connection, owner, ct) =>
    {
        var now = DateTimeOffset.UtcNow;
        var id = await MakeAsync(owner, "k1", "laptop", "reader", now, ct, lifetimeDays: 10);
        await MakeAsync(owner, "k2", "other", "operator", now, ct);

        var mine = await PersonalTokenStore.ListAsync(owner, "k1", ct);
        var row = Assert.Single(mine);
        Assert.Equal(id, row.TokenId);
        Assert.Equal("k1", row.OwnerKey);
        Assert.Equal("subject-k1", row.Subject);
        Assert.Equal("laptop", row.TokenName);
        Assert.Equal("reader", row.Role);
        Assert.True(Math.Abs((row.CreatedAt - now).TotalSeconds) < 2);
        Assert.Equal(row.CreatedAt.AddDays(10), row.ExpiresAt);
        Assert.Null(row.LastUsedAt);
        Assert.Null(row.RevokedAt);
        Assert.Null(row.RevokedBy);
        Assert.Null(row.RevokeReason);

        Assert.Equal(2, (await PersonalTokenStore.ListAsync(owner, null, ct)).Count);
    });

    [Fact]
    public Task LoadAll_CarriesTheHash() => WithStoreAsync(async (connection, owner, ct) =>
    {
        var minted = PersonalTokenFormat.Mint();
        await PersonalTokenStore.CreateAsync(owner, minted.TokenId, minted.SecretSha256, "k1", "s", "laptop", "reader", DateTimeOffset.UtcNow, 30, 5, Actor, ct);
        var loaded = Assert.Single(await PersonalTokenStore.LoadAllAsync(owner, ct));
        Assert.Equal(minted.TokenId, loaded.Row.TokenId);
        Assert.Equal(minted.SecretSha256, loaded.SecretSha256);
    });

    [Fact]
    public Task Create_AtCap_ReturnsAtCap() => WithStoreAsync(async (connection, owner, ct) =>
    {
        var now = DateTimeOffset.UtcNow;
        await MakeAsync(owner, "k1", "a", "reader", now, ct, cap: 2);
        await MakeAsync(owner, "k1", "b", "reader", now, ct, cap: 2);
        var minted = PersonalTokenFormat.Mint();
        Assert.Equal(PersonalTokenCreateOutcome.AtCap,
            await PersonalTokenStore.CreateAsync(owner, minted.TokenId, minted.SecretSha256, "k1", "s", "c", "reader", now, 30, 2, Actor, ct));
        Assert.Equal("2", await ScalarAsync(connection, "SELECT count(*) FROM config.mcp_personal_token", ct));
        Assert.Equal(PersonalTokenCreateOutcome.Created,
            await PersonalTokenStore.CreateAsync(owner, minted.TokenId, minted.SecretSha256, "k2", "s", "c", "reader", now, 30, 2, Actor, ct));
    });

    [Fact]
    public Task Create_DuplicateLiveName_ReturnsNameTaken() => WithStoreAsync(async (connection, owner, ct) =>
    {
        var now = DateTimeOffset.UtcNow;
        await MakeAsync(owner, "k1", "laptop", "reader", now, ct);
        var minted = PersonalTokenFormat.Mint();
        Assert.Equal(PersonalTokenCreateOutcome.NameTaken,
            await PersonalTokenStore.CreateAsync(owner, minted.TokenId, minted.SecretSha256, "k1", "s", "laptop", "reader", now, 30, 5, Actor, ct));
        Assert.Equal(PersonalTokenCreateOutcome.Created,
            await PersonalTokenStore.CreateAsync(owner, minted.TokenId, minted.SecretSha256, "k2", "s", "laptop", "reader", now, 30, 5, Actor, ct));
    });

    [Fact]
    public Task Create_NameOfRevokedToken_IsFree() => WithStoreAsync(async (connection, owner, ct) =>
    {
        var now = DateTimeOffset.UtcNow;
        var id = await MakeAsync(owner, "k1", "laptop", "reader", now, ct);
        Assert.True(await PersonalTokenStore.RevokeAsync(owner, id, "k1", "k1", "user", Actor, now, ct));
        await MakeAsync(owner, "k1", "laptop", "reader", now, ct);
    });

    /// <summary>Review Focus 3: two creates at the last slot of one owner key, run together, make exactly one token.</summary>
    [Fact]
    public Task Create_TwoConcurrentAtLastSlot_OneCreatedOneAtCap() => WithStoreAsync(async (connection, owner, ct) =>
    {
        var now = DateTimeOffset.UtcNow;
        var first = PersonalTokenFormat.Mint();
        var second = PersonalTokenFormat.Mint();
        var outcomes = await Task.WhenAll(
            PersonalTokenStore.CreateAsync(owner, first.TokenId, first.SecretSha256, "k1", "s", "a", "reader", now, 30, 1, Actor, ct),
            PersonalTokenStore.CreateAsync(owner, second.TokenId, second.SecretSha256, "k1", "s", "b", "reader", now, 30, 1, Actor, ct));
        Assert.Equal(1, outcomes.Count(o => o == PersonalTokenCreateOutcome.Created));
        Assert.Equal(1, outcomes.Count(o => o == PersonalTokenCreateOutcome.AtCap));
        Assert.Equal("1", await ScalarAsync(connection, "SELECT count(*) FROM config.mcp_personal_token", ct));
    });

    [Fact]
    public Task Create_WritesJournalRowWithoutValue() => WithStoreAsync(async (connection, owner, ct) =>
    {
        var minted = PersonalTokenFormat.Mint();
        await PersonalTokenStore.CreateAsync(owner, minted.TokenId, minted.SecretSha256, "k1", "s", "laptop", "operator", DateTimeOffset.UtcNow, 30, 5, Actor, ct);
        Assert.Equal("1", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'mcp_personal_token_created'", ct));
        Assert.Equal(Actor, await ScalarAsync(connection, "SELECT actor FROM config.investigation_journal WHERE event = 'mcp_personal_token_created'", ct));
        var detailText = await ScalarAsync(connection, "SELECT detail::text FROM config.investigation_journal WHERE event = 'mcp_personal_token_created'", ct);
        var detail = JsonNode.Parse(detailText)!.AsObject();
        Assert.Equal(["reason", "role", "token_id", "token_name"], detail.Select(p => p.Key).Order(StringComparer.Ordinal).ToArray());
        Assert.Equal(minted.TokenId, (string?)detail["token_id"]);
        Assert.Equal("laptop", (string?)detail["token_name"]);
        Assert.Equal("operator", (string?)detail["role"]);
        Assert.DoesNotContain(minted.Value, detailText, StringComparison.Ordinal);
        Assert.DoesNotContain(Convert.ToHexString(minted.SecretSha256), detailText, StringComparison.OrdinalIgnoreCase);
    });

    [Fact]
    public Task Revoke_Own_Succeeds() => WithStoreAsync(async (connection, owner, ct) =>
    {
        var now = DateTimeOffset.UtcNow;
        var id = await MakeAsync(owner, "k1", "laptop", "reader", now, ct);
        Assert.True(await PersonalTokenStore.RevokeAsync(owner, id, "k1", "person:alice", "user_revoked", Actor, now, ct));
        var row = Assert.Single(await PersonalTokenStore.ListAsync(owner, "k1", ct));
        Assert.NotNull(row.RevokedAt);
        Assert.Equal("person:alice", row.RevokedBy);
        Assert.Equal("user_revoked", row.RevokeReason);
        Assert.Equal("1", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'mcp_personal_token_revoked'", ct));
    });

    [Fact]
    public Task Revoke_OtherOwner_ReturnsFalseAndLeavesRow() => WithStoreAsync(async (connection, owner, ct) =>
    {
        var now = DateTimeOffset.UtcNow;
        var id = await MakeAsync(owner, "k1", "laptop", "reader", now, ct);
        Assert.False(await PersonalTokenStore.RevokeAsync(owner, id, "k2", "k2", "user_revoked", "person:eve", now, ct));
        Assert.Null(Assert.Single(await PersonalTokenStore.ListAsync(owner, null, ct)).RevokedAt);
        Assert.Equal("0", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'mcp_personal_token_revoked'", ct));
    });

    [Fact]
    public Task Revoke_Admin_NullOwnerKey_Succeeds() => WithStoreAsync(async (connection, owner, ct) =>
    {
        var now = DateTimeOffset.UtcNow;
        var id = await MakeAsync(owner, "k1", "laptop", "reader", now, ct);
        Assert.True(await PersonalTokenStore.RevokeAsync(owner, id, null, "person:admin", "admin_revoked", "person:admin", now, ct));
        Assert.NotNull(Assert.Single(await PersonalTokenStore.ListAsync(owner, null, ct)).RevokedAt);
    });

    [Fact]
    public Task Revoke_AlreadyRevoked_ReturnsFalse() => WithStoreAsync(async (connection, owner, ct) =>
    {
        var now = DateTimeOffset.UtcNow;
        var id = await MakeAsync(owner, "k1", "laptop", "reader", now, ct);
        Assert.True(await PersonalTokenStore.RevokeAsync(owner, id, "k1", "k1", "user_revoked", Actor, now, ct));
        Assert.False(await PersonalTokenStore.RevokeAsync(owner, id, "k1", "k1", "user_revoked", Actor, now, ct));
        Assert.Equal("1", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'mcp_personal_token_revoked'", ct));
    });

    [Fact]
    public Task RevokeForOwner_OperatorOnly_LeavesReader() => WithStoreAsync(async (connection, owner, ct) =>
    {
        var now = DateTimeOffset.UtcNow;
        await MakeAsync(owner, "k1", "op", "operator", now, ct);
        var reader = await MakeAsync(owner, "k1", "rd", "reader", now, ct);
        await MakeAsync(owner, "k2", "other-op", "operator", now, ct);

        Assert.Equal(1, await PersonalTokenStore.RevokeForOwnerAsync(owner, "k1", operatorOnly: true, "sign-in", "operator_grant_lost", now, ct));
        var rows = await PersonalTokenStore.ListAsync(owner, null, ct);
        Assert.Null(rows.Single(r => r.TokenId == reader).RevokedAt);
        Assert.Equal(["k1:op"], rows.Where(r => r.RevokedAt is not null).Select(r => r.OwnerKey + ":" + r.TokenName).ToArray());
        Assert.Equal("sign-in", await ScalarAsync(connection, "SELECT actor FROM config.investigation_journal WHERE event = 'mcp_personal_token_revoked'", ct));
    });

    [Fact]
    public Task RevokeForOwner_All() => WithStoreAsync(async (connection, owner, ct) =>
    {
        var now = DateTimeOffset.UtcNow;
        await MakeAsync(owner, "k1", "op", "operator", now, ct);
        await MakeAsync(owner, "k1", "rd", "reader", now, ct);
        await MakeAsync(owner, "k2", "other", "reader", now, ct);

        Assert.Equal(2, await PersonalTokenStore.RevokeForOwnerAsync(owner, "k1", operatorOnly: false, "sign-in", "person_removed", now, ct));
        Assert.Equal(0, await PersonalTokenStore.RevokeForOwnerAsync(owner, "k1", operatorOnly: false, "sign-in", "person_removed", now, ct));
        var rows = await PersonalTokenStore.ListAsync(owner, null, ct);
        Assert.All(rows.Where(r => r.OwnerKey == "k1"), r => Assert.NotNull(r.RevokedAt));
        Assert.Null(rows.Single(r => r.OwnerKey == "k2").RevokedAt);
        Assert.Equal("2", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'mcp_personal_token_revoked'", ct));
    });

    [Fact]
    public Task ApplySettings_ReaderOnly_RevokesOperator() => WithStoreAsync(async (connection, owner, ct) =>
    {
        var now = DateTimeOffset.UtcNow;
        await MakeAsync(owner, "k1", "op", "operator", now, ct);
        await MakeAsync(owner, "k2", "op", "operator", now, ct);
        var reader = await MakeAsync(owner, "k1", "rd", "reader", now, ct);

        Assert.Equal((2, 0), await PersonalTokenStore.ApplySettingsAsync(owner, 90, operatorAllowed: false, now, ct));
        var rows = await PersonalTokenStore.ListAsync(owner, null, ct);
        Assert.All(rows.Where(r => r.Role == "operator"), r =>
        {
            Assert.NotNull(r.RevokedAt);
            Assert.Equal("config", r.RevokedBy);
            Assert.Equal(PersonalTokenStore.OperatorsDisabledReason, r.RevokeReason);
        });
        Assert.Null(rows.Single(r => r.TokenId == reader).RevokedAt);
        Assert.Equal("2", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'mcp_personal_token_revoked' AND actor = 'config'", ct));
        Assert.Equal((0, 0), await PersonalTokenStore.ApplySettingsAsync(owner, 90, operatorAllowed: false, now, ct));
    });

    [Fact]
    public Task ApplySettings_LowerLifetime_CutsExpiryAndJournalsCut() => WithStoreAsync(async (connection, owner, ct) =>
    {
        var now = DateTimeOffset.UtcNow;
        var long1 = await MakeAsync(owner, "k1", "long", "reader", now, ct, lifetimeDays: 90);
        var short1 = await MakeAsync(owner, "k1", "short", "reader", now, ct, lifetimeDays: 5);

        Assert.Equal((0, 1), await PersonalTokenStore.ApplySettingsAsync(owner, 30, operatorAllowed: true, now, ct));
        var rows = await PersonalTokenStore.ListAsync(owner, null, ct);
        var cut = rows.Single(r => r.TokenId == long1);
        Assert.Equal(cut.CreatedAt.AddDays(30), cut.ExpiresAt);
        Assert.Null(cut.RevokedAt);
        var untouched = rows.Single(r => r.TokenId == short1);
        Assert.Equal(untouched.CreatedAt.AddDays(5), untouched.ExpiresAt);
        Assert.Equal("1", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'mcp_personal_token_expiry_cut' AND actor = 'config'", ct));
        Assert.Equal((0, 0), await PersonalTokenStore.ApplySettingsAsync(owner, 30, operatorAllowed: true, now, ct));
    });

    [Fact]
    public Task TouchLastUsed_NeverMovesBackwards() => WithStoreAsync(async (connection, owner, ct) =>
    {
        var now = DateTimeOffset.UtcNow;
        var a = await MakeAsync(owner, "k1", "a", "reader", now, ct);
        var b = await MakeAsync(owner, "k1", "b", "reader", now, ct);
        var later = now.AddMinutes(10).ToUniversalTime();
        var earlier = now.AddMinutes(5).ToUniversalTime();

        await PersonalTokenStore.TouchLastUsedAsync(owner, new Dictionary<string, DateTimeOffset> { [a] = later, [b] = later }, ct);
        var rows = await PersonalTokenStore.ListAsync(owner, null, ct);
        Assert.Equal(later, rows.Single(r => r.TokenId == a).LastUsedAt!.Value.ToUniversalTime(), TimeSpan.FromMilliseconds(1));

        await PersonalTokenStore.TouchLastUsedAsync(owner, new Dictionary<string, DateTimeOffset> { [a] = earlier, [b] = later.AddMinutes(1) }, ct);
        rows = await PersonalTokenStore.ListAsync(owner, null, ct);
        Assert.Equal(later, rows.Single(r => r.TokenId == a).LastUsedAt!.Value.ToUniversalTime(), TimeSpan.FromMilliseconds(1));
        Assert.Equal(later.AddMinutes(1), rows.Single(r => r.TokenId == b).LastUsedAt!.Value.ToUniversalTime(), TimeSpan.FromMilliseconds(1));

        await PersonalTokenStore.TouchLastUsedAsync(owner, new Dictionary<string, DateTimeOffset>(), ct);
    });

    [Fact]
    public Task Retention_DeletesRowsDeadOver30Days_KeepsRecent() => WithStoreAsync(async (connection, owner, ct) =>
    {
        const string insert = "INSERT INTO config.mcp_personal_token (token_id, secret_sha256, owner_key, subject, token_name, role, created_at, expires_at, revoked_at) "
            + "VALUES ('{0}', decode(repeat('ab', 32), 'hex'), 'k', 's', '{0}', 'reader', now() - interval '100 days', now() + interval '{1}', {2})";
        await ExecAsync(connection, string.Format(CultureInfo.InvariantCulture, insert, "00000000000000a1", "-40 days", "NULL"), ct);
        await ExecAsync(connection, string.Format(CultureInfo.InvariantCulture, insert, "00000000000000a2", "20 days", "now() - interval '40 days'"), ct);
        await ExecAsync(connection, string.Format(CultureInfo.InvariantCulture, insert, "00000000000000b1", "-10 days", "NULL"), ct);
        await ExecAsync(connection, string.Format(CultureInfo.InvariantCulture, insert, "00000000000000b2", "20 days", "now() - interval '10 days'"), ct);
        await ExecAsync(connection, string.Format(CultureInfo.InvariantCulture, insert, "00000000000000c1", "20 days", "NULL"), ct);

        await DarlingRetention.PurgeAsync(owner, timescaleAvailable: false, null, ct);

        Assert.Equal("00000000000000b1,00000000000000b2,00000000000000c1",
            await ScalarAsync(connection, "SELECT string_agg(token_id, ',' ORDER BY token_id) FROM config.mcp_personal_token", ct));
    });

    /// <summary>E1: below V272 the table is absent, and the store says so with the one code callers branch on.</summary>
    [Fact]
    public Task MissingTable_SurfacesAsTypedError() => WithStoreAsync(async (connection, owner, ct) =>
    {
        /* The store as V271 leaves it: the V272 table and its trigger function are gone. */
        await ExecAsync(connection, "DROP TABLE config.mcp_personal_token; DROP FUNCTION config.mcp_personal_token_owner_only()", ct);

        var list = await Assert.ThrowsAsync<PostgresException>(() => PersonalTokenStore.ListAsync(owner, null, ct));
        Assert.Equal("42P01", list.SqlState);
        Assert.True(PersonalTokenStore.IsMissingTable(list));
        var load = await Assert.ThrowsAsync<PostgresException>(() => PersonalTokenStore.LoadAllAsync(owner, ct));
        Assert.Equal("42P01", load.SqlState);
        Assert.False(PersonalTokenStore.IsMissingTable(new InvalidOperationException("x")));
    });
}
