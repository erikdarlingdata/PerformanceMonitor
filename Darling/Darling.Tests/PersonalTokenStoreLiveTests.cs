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
/// #8737: <see cref="PersonalTokenStore"/> on a live store migrated to V275, on the owner connection: create under the cap, the name rule and the
/// advisory lock, the lists without the hash, revoke, the settings sweep, the last-use stamp, the journal, the retention purge and the missing table.
/// </summary>
[Collection("live-postgres")]
public sealed class PersonalTokenStoreLiveTests : IClassFixture<PersonalTokenStoreLiveTests.SharedStore>
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

    /// <summary>
    /// #9266: one migrated scratch database for the whole class. Minting a database per test cost about two seconds of the
    /// 53-second class (the clone, the scheduler start and the drop), and a test only touches the two tables this resets.
    /// The class's tests run one at a time, and each starts from the two tables emptied, so none sees another's rows.
    /// The one test that needs the table gone moves it aside and puts it back, so no test needs a database of its own.
    /// </summary>
    public sealed class SharedStore : IAsyncLifetime
    {
        private ScratchPostgres? _scratch;

        public string ConnectionString => _scratch?.ConnectionString ?? throw new InvalidOperationException("The shared store was not created.");

        public async ValueTask InitializeAsync()
        {
            var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
            if (string.IsNullOrEmpty(connectionString))
            {
                return;
            }

            var ct = CancellationToken.None;
            _scratch = await ScratchPostgres.CreateAsync(connectionString, ct);
            await using var connection = new NpgsqlConnection(_scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await ExecAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        }

        public async ValueTask DisposeAsync()
        {
            if (_scratch is not null)
            {
                await _scratch.DisposeAsync();
            }
        }
    }

    private readonly SharedStore _store;

    public PersonalTokenStoreLiveTests(SharedStore store) => _store = store;

    private const string ResetSql = "TRUNCATE config.mcp_personal_token, config.investigation_journal";

    private async Task WithStoreAsync(Func<NpgsqlConnection, NpgsqlDataSource, CancellationToken, Task> body)
    {
        var ct = TestContext.Current.CancellationToken;
        RequireLivePostgres();
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(_store.ConnectionString);
            await connection.OpenAsync(ct);
            await ExecAsync(connection, ResetSql, ct);
            await using var owner = NpgsqlDataSource.Create(_store.ConnectionString);
            await body(connection, owner, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(_store.ConnectionString, bodySucceeded, (connection, _) => ExecAsync(connection, ResetSql, CancellationToken.None));
        }
    }

    /// <summary>Creates a token through the store with a freshly minted id and hash; returns its id.</summary>
    private static async Task<string> MakeAsync(NpgsqlDataSource owner, string ownerKey, string name, string role, DateTimeOffset now, CancellationToken ct,
        int lifetimeDays = 30, int cap = 5, string actor = Actor)
    {
        var minted = PersonalTokenFormat.Mint();
        var outcome = await PersonalTokenStore.InsertAsync(owner, minted.TokenId, minted.SecretSha256, ownerKey, "subject-" + ownerKey, name, role, now,
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

        var mine = (await PersonalTokenStore.ListAsync(owner, "k1", DateTimeOffset.UtcNow, ct)).Rows;
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

        Assert.Equal(2, ((await PersonalTokenStore.ListAsync(owner, null, DateTimeOffset.UtcNow, ct)).Rows).Count);
    });

    [Fact]
    public Task LoadAll_CarriesTheHash() => WithStoreAsync(async (connection, owner, ct) =>
    {
        var minted = PersonalTokenFormat.Mint();
        await PersonalTokenStore.InsertAsync(owner, minted.TokenId, minted.SecretSha256, "k1", "s", "laptop", "reader", DateTimeOffset.UtcNow, 30, 5, Actor, ct);
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
            await PersonalTokenStore.InsertAsync(owner, minted.TokenId, minted.SecretSha256, "k1", "s", "c", "reader", now, 30, 2, Actor, ct));
        Assert.Equal("2", await ScalarAsync(connection, "SELECT count(*) FROM config.mcp_personal_token", ct));
        Assert.Equal(PersonalTokenCreateOutcome.Created,
            await PersonalTokenStore.InsertAsync(owner, minted.TokenId, minted.SecretSha256, "k2", "s", "c", "reader", now, 30, 2, Actor, ct));
    });

    [Fact]
    public Task Create_DuplicateLiveName_ReturnsNameTaken() => WithStoreAsync(async (connection, owner, ct) =>
    {
        var now = DateTimeOffset.UtcNow;
        await MakeAsync(owner, "k1", "laptop", "reader", now, ct);
        var minted = PersonalTokenFormat.Mint();
        Assert.Equal(PersonalTokenCreateOutcome.NameTaken,
            await PersonalTokenStore.InsertAsync(owner, minted.TokenId, minted.SecretSha256, "k1", "s", "laptop", "reader", now, 30, 5, Actor, ct));
        Assert.Equal(PersonalTokenCreateOutcome.Created,
            await PersonalTokenStore.InsertAsync(owner, minted.TokenId, minted.SecretSha256, "k2", "s", "laptop", "reader", now, 30, 5, Actor, ct));
    });

    [Fact]
    public Task Create_NameOfRevokedToken_IsFree() => WithStoreAsync(async (connection, owner, ct) =>
    {
        var now = DateTimeOffset.UtcNow;
        var id = await MakeAsync(owner, "k1", "laptop", "reader", now, ct);
        Assert.True(await PersonalTokenStore.RevokeAsync(owner, id, "k1", "k1", Actor, "admin:alice", Actor, now, ct));
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
            PersonalTokenStore.InsertAsync(owner, first.TokenId, first.SecretSha256, "k1", "s", "a", "reader", now, 30, 1, Actor, ct),
            PersonalTokenStore.InsertAsync(owner, second.TokenId, second.SecretSha256, "k1", "s", "b", "reader", now, 30, 1, Actor, ct));
        Assert.Equal(1, outcomes.Count(o => o == PersonalTokenCreateOutcome.Created));
        Assert.Equal(1, outcomes.Count(o => o == PersonalTokenCreateOutcome.AtCap));
        Assert.Equal("1", await ScalarAsync(connection, "SELECT count(*) FROM config.mcp_personal_token", ct));
    });

    /// <summary>#9643: a second owner key cannot take a subject another owner key's token carries, in any case, even when that token is revoked.</summary>
    [Fact]
    public Task Create_ForASubjectAnotherOwnerHolds_ReturnsSubjectTaken_InAnyCase_AndTheSameOwnerMayRepeatIt() => WithStoreAsync(async (connection, owner, ct) =>
    {
        var now = DateTimeOffset.UtcNow;
        var first = PersonalTokenFormat.Mint();
        Assert.Equal(PersonalTokenCreateOutcome.Created,
            await PersonalTokenStore.InsertAsync(owner, first.TokenId, first.SecretSha256, "k1", "Pat Person", "a", "reader", now, 30, 5, Actor, ct));

        var other = PersonalTokenFormat.Mint();
        Assert.Equal(PersonalTokenCreateOutcome.SubjectTaken,
            await PersonalTokenStore.InsertAsync(owner, other.TokenId, other.SecretSha256, "k2", "Pat Person", "a", "reader", now, 30, 5, Actor, ct));
        Assert.Equal(PersonalTokenCreateOutcome.SubjectTaken,
            await PersonalTokenStore.InsertAsync(owner, other.TokenId, other.SecretSha256, "k2", "pat person", "a", "reader", now, 30, 5, Actor, ct));
        Assert.Equal("1", await ScalarAsync(connection, "SELECT count(*) FROM config.mcp_personal_token", ct));

        Assert.True(await PersonalTokenStore.RevokeAsync(owner, first.TokenId, "k1", "k1", Actor, "admin:alice", Actor, now, ct));
        Assert.Equal(PersonalTokenCreateOutcome.SubjectTaken,
            await PersonalTokenStore.InsertAsync(owner, other.TokenId, other.SecretSha256, "k2", "PAT PERSON", "b", "reader", now, 30, 5, Actor, ct));

        var again = PersonalTokenFormat.Mint();
        Assert.Equal(PersonalTokenCreateOutcome.Created,
            await PersonalTokenStore.InsertAsync(owner, again.TokenId, again.SecretSha256, "k1", "PAT PERSON", "c", "reader", now, 30, 5, Actor, ct));
    });

    /// <summary>#9643: two owner keys minting the same subject at once, in different case, make exactly one token.</summary>
    [Fact]
    public Task Create_TwoOwnersAtOnceForOneSubject_OneCreatedOneSubjectTaken() => WithStoreAsync(async (connection, owner, ct) =>
    {
        var now = DateTimeOffset.UtcNow;
        var first = PersonalTokenFormat.Mint();
        var second = PersonalTokenFormat.Mint();
        var outcomes = await Task.WhenAll(
            PersonalTokenStore.InsertAsync(owner, first.TokenId, first.SecretSha256, "k1", "Race Person", "a", "reader", now, 30, 5, Actor, ct),
            PersonalTokenStore.InsertAsync(owner, second.TokenId, second.SecretSha256, "k2", "race person", "a", "reader", now, 30, 5, Actor, ct));
        Assert.Equal(1, outcomes.Count(o => o == PersonalTokenCreateOutcome.Created));
        Assert.Equal(1, outcomes.Count(o => o == PersonalTokenCreateOutcome.SubjectTaken));
        Assert.Equal("1", await ScalarAsync(connection, "SELECT count(*) FROM config.mcp_personal_token", ct));
    });

    /// <summary>I2: the live cap frees a slot on every revoke, so the creates of the last 24 hours are capped too, revoked ones included.</summary>
    [Fact]
    public Task Create_RecentCeiling_RefusesTheNextCreate_EvenWithEveryEarlierTokenRevoked() => WithStoreAsync(async (connection, owner, ct) =>
    {
        const int cap = 2;
        var now = DateTimeOffset.UtcNow;
        var ceiling = cap * PersonalTokenStore.RecentCreatesPerSlot;
        for (var i = 0; i < ceiling; i++)
        {
            var id = await MakeAsync(owner, "k1", "t" + i.ToString(CultureInfo.InvariantCulture), "reader", now, ct, cap: cap);
            Assert.True(await PersonalTokenStore.RevokeAsync(owner, id, "k1", "k1", Actor, "admin:alice", Actor, now, ct));
        }

        var minted = PersonalTokenFormat.Mint();
        Assert.Equal(PersonalTokenCreateOutcome.TooManyRecent,
            await PersonalTokenStore.InsertAsync(owner, minted.TokenId, minted.SecretSha256, "k1", "s", "one-more", "reader", now, 30, cap, Actor, ct));
        Assert.Equal(ceiling.ToString(CultureInfo.InvariantCulture), await ScalarAsync(connection, "SELECT count(*) FROM config.mcp_personal_token", ct));
        /* Another person is not held by this one's creates. */
        Assert.Equal(PersonalTokenCreateOutcome.Created,
            await PersonalTokenStore.InsertAsync(owner, minted.TokenId, minted.SecretSha256, "k2", "s", "one-more", "reader", now, 30, cap, Actor, ct));
    });

    [Fact]
    public Task Create_RecentCeiling_DoesNotCountARowOlderThan24Hours() => WithStoreAsync(async (connection, owner, ct) =>
    {
        const int cap = 1;
        var now = DateTimeOffset.UtcNow;
        var ceiling = cap * PersonalTokenStore.RecentCreatesPerSlot;
        for (var i = 0; i < ceiling; i++)
        {
            var id = await MakeAsync(owner, "k1", "t" + i.ToString(CultureInfo.InvariantCulture), "reader", now, ct, cap: cap);
            Assert.True(await PersonalTokenStore.RevokeAsync(owner, id, "k1", "k1", Actor, "admin:alice", Actor, now, ct));
        }

        /* One of them falls out of the window: one slot comes back. */
        await ExecAsync(connection,
            "UPDATE config.mcp_personal_token SET created_at = created_at - interval '25 hours' WHERE token_id = (SELECT min(token_id) FROM config.mcp_personal_token)", ct);

        var minted = PersonalTokenFormat.Mint();
        Assert.Equal(PersonalTokenCreateOutcome.Created,
            await PersonalTokenStore.InsertAsync(owner, minted.TokenId, minted.SecretSha256, "k1", "s", "again", "reader", now, 30, cap, Actor, ct));
    });

    [Fact]
    public Task List_PastTheLimit_KeepsTheNewest_AndSaysTruncated_AtTheLimitItDoesNot() => WithStoreAsync(async (connection, owner, ct) =>
    {
        var limit = PersonalTokenStore.ListLimit;
        await ExecAsync(connection, string.Format(CultureInfo.InvariantCulture,
            "INSERT INTO config.mcp_personal_token (token_id, secret_sha256, owner_key, subject, token_name, role, created_at, expires_at) "
            + "SELECT lpad(to_hex(i), 16, '0'), sha256(i::text::bytea), 'bulk', 's', 'n' || i, 'reader', now() - make_interval(secs => {0} - i), now() + interval '1 day' "
            + "FROM generate_series(0, {0}) AS i", limit), ct);

        var all = await PersonalTokenStore.ListAsync(owner, null, DateTimeOffset.UtcNow, ct);
        var own = await PersonalTokenStore.ListAsync(owner, "bulk", DateTimeOffset.UtcNow, ct);

        foreach (var page in new[] { all, own })
        {
            Assert.True(page.Truncated);
            Assert.Equal(limit, page.Rows.Count);
            /* The oldest row (i = 0) is the one left out. */
            Assert.DoesNotContain(page.Rows, r => r.TokenName == "n0");
            Assert.Contains(page.Rows, r => r.TokenName == "n" + limit.ToString(CultureInfo.InvariantCulture));
        }

        await ExecAsync(connection, "DELETE FROM config.mcp_personal_token WHERE token_name = 'n0'", ct);
        var exact = await PersonalTokenStore.ListAsync(owner, null, DateTimeOffset.UtcNow, ct);
        Assert.False(exact.Truncated);
        Assert.Equal(limit, exact.Rows.Count);
    });

    [Fact]
    public Task List_PastTheLimit_LiveRowsComeFirst_AnOlderLiveRowIsKept_AndSaysTruncated() => WithStoreAsync(async (connection, owner, ct) =>
    {
        var limit = PersonalTokenStore.ListLimit;
        /* One live row, the oldest of all, and limit + 5 newer dead ones (revoked, and expired): the oldest-first cut would drop the live row. */
        await ExecAsync(connection,
            "INSERT INTO config.mcp_personal_token (token_id, secret_sha256, owner_key, subject, token_name, role, created_at, expires_at) "
            + "VALUES ('0000000000000000', sha256('live'::bytea), 'bulk', 's', 'live-one', 'reader', now() - interval '10 days', now() + interval '1 day')", ct);
        await ExecAsync(connection, string.Format(CultureInfo.InvariantCulture,
            "INSERT INTO config.mcp_personal_token (token_id, secret_sha256, owner_key, subject, token_name, role, created_at, expires_at, revoked_at, revoked_by, revoke_reason) "
            + "SELECT lpad(to_hex(i), 16, '0'), sha256(i::text::bytea), 'bulk', 's', 'dead' || i, 'reader', now() - make_interval(secs => {0} - i), now() + interval '1 day', now(), 'test', 'revoked_by_owner' "
            + "FROM generate_series(1, {0}) AS i", limit + 5), ct);

        var all = await PersonalTokenStore.ListAsync(owner, null, DateTimeOffset.UtcNow, ct);
        var own = await PersonalTokenStore.ListAsync(owner, "bulk", DateTimeOffset.UtcNow, ct);

        foreach (var page in new[] { all, own })
        {
            Assert.True(page.Truncated);
            Assert.Equal(limit, page.Rows.Count);
            Assert.Contains(page.Rows, r => r.TokenName == "live-one");
            /* Still handed back oldest first, the live row (10 days old) at the head. */
            Assert.Equal("live-one", page.Rows[0].TokenName);
            Assert.Equal(page.Rows.OrderBy(r => r.CreatedAt).ThenBy(r => r.TokenId, StringComparer.Ordinal).Select(r => r.TokenId), page.Rows.Select(r => r.TokenId));
            /* The dead rows left out are the oldest of them. */
            Assert.DoesNotContain(page.Rows, r => r.TokenName == "dead1");
        }
    });

    [Fact]
    public Task Create_WritesJournalRowWithoutValue() => WithStoreAsync(async (connection, owner, ct) =>
    {
        var minted = PersonalTokenFormat.Mint();
        await PersonalTokenStore.InsertAsync(owner, minted.TokenId, minted.SecretSha256, "k1", "s", "laptop", "operator", DateTimeOffset.UtcNow, 30, 5, Actor, ct);
        Assert.Equal("1", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'mcp_personal_token_created'", ct));
        Assert.Equal(Actor, await ScalarAsync(connection, "SELECT actor FROM config.investigation_journal WHERE event = 'mcp_personal_token_created'", ct));
        var detailText = await ScalarAsync(connection, "SELECT detail::text FROM config.investigation_journal WHERE event = 'mcp_personal_token_created'", ct);
        var detail = JsonNode.Parse(detailText)!.AsObject();
        Assert.Equal(["owner_key", "reason", "role", "subject", "token_id", "token_name"], detail.Select(p => p.Key).Order(StringComparer.Ordinal).ToArray());
        Assert.Equal(minted.TokenId, (string?)detail["token_id"]);
        Assert.Equal("laptop", (string?)detail["token_name"]);
        Assert.Equal("operator", (string?)detail["role"]);
        /* #9643: the row names whose token it is. */
        Assert.Equal("s", (string?)detail["subject"]);
        Assert.Equal("k1", (string?)detail["owner_key"]);
        Assert.DoesNotContain(minted.Value, detailText, StringComparison.Ordinal);
        Assert.DoesNotContain(Convert.ToHexString(minted.SecretSha256), detailText, StringComparison.OrdinalIgnoreCase);
    });

    [Fact]
    public Task Revoke_JournalRowNamesWhoseTokenItWas() => WithStoreAsync(async (connection, owner, ct) =>
    {
        var now = DateTimeOffset.UtcNow;
        var minted = PersonalTokenFormat.Mint();
        await PersonalTokenStore.InsertAsync(owner, minted.TokenId, minted.SecretSha256, "k9", "Jo Person", "laptop", "reader", now, 30, 5, Actor, ct);
        Assert.True(await PersonalTokenStore.RevokeAsync(owner, minted.TokenId, null, null, "owner", "admin", Actor, now.AddMinutes(1), ct));
        var detail = JsonNode.Parse(await ScalarAsync(connection,
            "SELECT detail::text FROM config.investigation_journal WHERE event = 'mcp_personal_token_revoked'", ct))!.AsObject();
        Assert.Equal("Jo Person", (string?)detail["subject"]);
        Assert.Equal("k9", (string?)detail["owner_key"]);
        Assert.Equal(minted.TokenId, (string?)detail["token_id"]);
    });

    [Fact]
    public Task Revoke_Own_Succeeds() => WithStoreAsync(async (connection, owner, ct) =>
    {
        var now = DateTimeOffset.UtcNow;
        var id = await MakeAsync(owner, "k1", "laptop", "reader", now, ct);
        Assert.True(await PersonalTokenStore.RevokeAsync(owner, id, "k1", "k1", "person:alice", "admin:alice", Actor, now, ct));
        var row = Assert.Single((await PersonalTokenStore.ListAsync(owner, "k1", DateTimeOffset.UtcNow, ct)).Rows);
        Assert.NotNull(row.RevokedAt);
        Assert.Equal("person:alice", row.RevokedBy);
        Assert.Equal(PersonalTokenStore.RevokedByOwnerReason, row.RevokeReason);
        Assert.Equal("1", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'mcp_personal_token_revoked'", ct));
        Assert.Equal(PersonalTokenStore.RevokedByOwnerReason, await ScalarAsync(connection,
            "SELECT detail->>'reason' FROM config.investigation_journal WHERE event = 'mcp_personal_token_revoked'", ct));
    });

    [Fact]
    public Task Revoke_OtherOwner_ReturnsFalseAndLeavesRow() => WithStoreAsync(async (connection, owner, ct) =>
    {
        var now = DateTimeOffset.UtcNow;
        var id = await MakeAsync(owner, "k1", "laptop", "reader", now, ct);
        Assert.False(await PersonalTokenStore.RevokeAsync(owner, id, "k2", "k2", "person:eve", "admin:eve", "person:eve", now, ct));
        Assert.Null(Assert.Single((await PersonalTokenStore.ListAsync(owner, null, DateTimeOffset.UtcNow, ct)).Rows).RevokedAt);
        Assert.Equal("0", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'mcp_personal_token_revoked'", ct));
    });

    [Fact]
    public Task Revoke_Admin_NullOwnerKey_Succeeds() => WithStoreAsync(async (connection, owner, ct) =>
    {
        var now = DateTimeOffset.UtcNow;
        var id = await MakeAsync(owner, "k1", "laptop", "reader", now, ct);
        Assert.True(await PersonalTokenStore.RevokeAsync(owner, id, null, "k-admin", "person:admin", "admin:admin", "person:admin", now, ct));
        var row = Assert.Single((await PersonalTokenStore.ListAsync(owner, null, DateTimeOffset.UtcNow, ct)).Rows);
        Assert.NotNull(row.RevokedAt);
        Assert.Equal("admin:admin", row.RevokedBy);
        Assert.Equal(PersonalTokenStore.RevokedByAdminReason, row.RevokeReason);
        Assert.Equal(PersonalTokenStore.RevokedByAdminReason, await ScalarAsync(connection,
            "SELECT detail->>'reason' FROM config.investigation_journal WHERE event = 'mcp_personal_token_revoked'", ct));
    });

    /// <summary>M2: whose row it is decides the reason, not the seat: an admin revoking their own token is its owner revoking it.</summary>
    [Fact]
    public Task Revoke_AdminsOwnToken_IsRevokedByOwner() => WithStoreAsync(async (connection, owner, ct) =>
    {
        var now = DateTimeOffset.UtcNow;
        var id = await MakeAsync(owner, "k-admin", "laptop", "operator", now, ct);
        Assert.True(await PersonalTokenStore.RevokeAsync(owner, id, null, "k-admin", "person:admin", "admin:admin", "person:admin", now, ct));
        var row = Assert.Single((await PersonalTokenStore.ListAsync(owner, null, DateTimeOffset.UtcNow, ct)).Rows);
        Assert.Equal("person:admin", row.RevokedBy);
        Assert.Equal(PersonalTokenStore.RevokedByOwnerReason, row.RevokeReason);
    });

    [Fact]
    public Task Revoke_AlreadyRevoked_ReturnsFalse() => WithStoreAsync(async (connection, owner, ct) =>
    {
        var now = DateTimeOffset.UtcNow;
        var id = await MakeAsync(owner, "k1", "laptop", "reader", now, ct);
        Assert.True(await PersonalTokenStore.RevokeAsync(owner, id, "k1", "k1", Actor, "admin:alice", Actor, now, ct));
        Assert.False(await PersonalTokenStore.RevokeAsync(owner, id, "k1", "k1", Actor, "admin:alice", Actor, now, ct));
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
        var rows = (await PersonalTokenStore.ListAsync(owner, null, DateTimeOffset.UtcNow, ct)).Rows;
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
        var rows = (await PersonalTokenStore.ListAsync(owner, null, DateTimeOffset.UtcNow, ct)).Rows;
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
        var rows = (await PersonalTokenStore.ListAsync(owner, null, DateTimeOffset.UtcNow, ct)).Rows;
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
        var rows = (await PersonalTokenStore.ListAsync(owner, null, DateTimeOffset.UtcNow, ct)).Rows;
        var cut = rows.Single(r => r.TokenId == long1);
        Assert.Equal(cut.CreatedAt.AddDays(30), cut.ExpiresAt);
        Assert.Null(cut.RevokedAt);
        var untouched = rows.Single(r => r.TokenId == short1);
        Assert.Equal(untouched.CreatedAt.AddDays(5), untouched.ExpiresAt);
        Assert.Equal("1", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'mcp_personal_token_expiry_cut' AND actor = 'config'", ct));
        Assert.Equal((0, 0), await PersonalTokenStore.ApplySettingsAsync(owner, 30, operatorAllowed: true, now, ct));
    });

    [Fact]
    public Task ApplySettings_FullLifetimeAcrossADaylightSavingChange_IsNotCut() => WithStoreAsync(async (connection, owner, ct) =>
    {
        /* A session time zone with daylight saving: the 30 days from 1 March 2026 12:00 UTC to 31 March run across New York's change on 8 March, so
           created_at + '30 days' (wall clock) is an hour short of the 30 x 24 hours the insert stored. The cut compares the two, so it must use hours. */
        await using var newYork = NpgsqlDataSource.Create(owner.ConnectionString + ";Timezone=America/New_York");
        await using (var show = newYork.CreateCommand("SHOW TimeZone"))
        {
            Assert.Equal("America/New_York", (string?)await show.ExecuteScalarAsync(ct));
        }

        var created = new DateTimeOffset(2026, 3, 1, 12, 0, 0, TimeSpan.Zero);
        var id = await MakeAsync(newYork, "k1", "full", "reader", created, ct, lifetimeDays: 30);

        Assert.Equal((0, 0), await PersonalTokenStore.ApplySettingsAsync(newYork, 30, operatorAllowed: true, created.AddDays(1), ct));
        var row = (await PersonalTokenStore.ListAsync(newYork, null, created.AddDays(1), ct)).Rows.Single(r => r.TokenId == id);
        Assert.Equal(created.AddDays(30), row.ExpiresAt);
        Assert.Equal("0", await ScalarAsync(connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'mcp_personal_token_expiry_cut'", ct));

        /* A real cut across the same change still lands on exactly N x 24 hours. */
        var longId = await MakeAsync(newYork, "k1", "long", "reader", created, ct, lifetimeDays: 90);
        Assert.Equal((0, 1), await PersonalTokenStore.ApplySettingsAsync(newYork, 30, operatorAllowed: true, created.AddDays(1), ct));
        var cut = (await PersonalTokenStore.ListAsync(newYork, null, created.AddDays(1), ct)).Rows.Single(r => r.TokenId == longId);
        Assert.Equal(created.AddDays(30), cut.ExpiresAt);
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
        var rows = (await PersonalTokenStore.ListAsync(owner, null, DateTimeOffset.UtcNow, ct)).Rows;
        Assert.Equal(later, rows.Single(r => r.TokenId == a).LastUsedAt!.Value.ToUniversalTime(), TimeSpan.FromMilliseconds(1));

        await PersonalTokenStore.TouchLastUsedAsync(owner, new Dictionary<string, DateTimeOffset> { [a] = earlier, [b] = later.AddMinutes(1) }, ct);
        rows = (await PersonalTokenStore.ListAsync(owner, null, DateTimeOffset.UtcNow, ct)).Rows;
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

    /// <summary>E1: below V275 the table is absent, and the store says so with the one code callers branch on.</summary>
    [Fact]
    public Task MissingTable_SurfacesAsTypedError() => WithStoreAsync(async (connection, owner, ct) =>
    {
        /* The store as V274 leaves it: the V275 table and its trigger function are absent. They are renamed away here and renamed back in
           the finally, not dropped (#9266), because this test shares its store with the others. A lookup by name finds neither, as it
           would on a store without them. */
        await ExecAsync(connection, "ALTER TABLE config.mcp_personal_token RENAME TO mcp_personal_token_aside; ALTER FUNCTION config.mcp_personal_token_owner_only() RENAME TO mcp_personal_token_owner_only_aside", ct);
        try
        {
            var list = await Assert.ThrowsAsync<PostgresException>(() => PersonalTokenStore.ListAsync(owner, null, DateTimeOffset.UtcNow, ct));
            Assert.Equal("42P01", list.SqlState);
            Assert.True(PersonalTokenStore.IsMissingTable(list));
            Assert.False(PersonalTokenStore.IsMissingTable(new InvalidOperationException("x")));

            /* M7: the snapshot's load and the start sweep run every few seconds, so they look the table up first and never send a statement
               that would raise 42P01 in the PostgreSQL log: the server raised nothing, and the callers' one branch still holds. */
            var load = await Assert.ThrowsAnyAsync<Exception>(() => PersonalTokenStore.LoadAllAsync(owner, ct));
            Assert.IsNotType<PostgresException>(load);
            Assert.True(PersonalTokenStore.IsMissingTable(load));
            var sweep = await Assert.ThrowsAnyAsync<Exception>(() => PersonalTokenStore.ApplySettingsAsync(owner, 30, operatorAllowed: false, DateTimeOffset.UtcNow, ct));
            Assert.IsNotType<PostgresException>(sweep);
            Assert.True(PersonalTokenStore.IsMissingTable(sweep));
        }
        finally
        {
            await ExecAsync(connection, "ALTER TABLE config.mcp_personal_token_aside RENAME TO mcp_personal_token; ALTER FUNCTION config.mcp_personal_token_owner_only_aside() RENAME TO mcp_personal_token_owner_only", CancellationToken.None);
        }
    });
}
