/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the test mints its own scratch database through ScratchPostgres. The managed roles it provisions carry fixed,
   cluster-wide names (admin, viewer, mcp), so the class runs in the pg-cluster-roles collection with the other provisioning tests. */

/// <summary>
/// #6400: <c>write_investigation_note</c>, <c>approve_investigation_note</c>, <c>reject_investigation_note</c> and
/// <c>close_investigation_request</c> as the provisioned, NOSUPERUSER <c>mcp</c> role. The grants are the shipped
/// <c>DarlingManagedRoles.BuildProvisioningSql</c> batch run whole, so a missing grant fails here and not on an owner connection.
/// Proves each write lands what the web route's store call lands (an approved note by its author, a decision stamped and journaled,
/// a close with its reason and journal row), that each refusal the route has writes nothing, that the call is recorded as the MCP
/// caller, and that the role holds exactly the columns the four statements set.
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class InvestigationAdminMcpLiveTests
{
    private const int ServerId = 7_640_001;
    private const string ServerName = "LcInvAdminSrv";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static JsonNode Parse(string json) => JsonNode.Parse(json)!;

    private static async Task<string?> ScalarAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static async Task ExecAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> PendingNoteAsync(NpgsqlDataSource owner, string text, CancellationToken ct) =>
        long.Parse((await ScalarAsync(owner,
            $"INSERT INTO config.investigation_notes (server_id, note_text, author) VALUES ({ServerId}, '{text}', 'agent:planted') RETURNING note_id", ct))!,
            CultureInfo.InvariantCulture);

    [Fact]
    public async Task TheProvisionedMcpRole_WritesApprovesRejectsAndCloses_RefusesWhatTheWebRefuses_AndHoldsNothingWider()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the investigation admin tool test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using (var owner = new NpgsqlConnection(scratch.ConnectionString))
        {
            await owner.OpenAsync(ct);
            await PgMigrations.MigrateAsync(owner, ct);
            await DarlingMcpTestData.RegisterServerAsync(owner, ServerId, ServerName, ct);
            /* The managed roles carry fixed names and a role is cluster-wide: never adopt or drop a set that belongs to something else. */
            await using var probe = new NpgsqlCommand("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'mcp'))", owner);
            Assert.SkipWhen((bool)(await probe.ExecuteScalarAsync(ct))!, "A cluster-wide admin/viewer/mcp role already exists on this rig; the managed provisioning batch would adopt it.");
        }

        var bodySucceeded = false;
        try
        {
            await using (var owner = new NpgsqlConnection(scratch.ConnectionString))
            {
                await owner.OpenAsync(ct);
                var ownerRole = new NpgsqlConnectionStringBuilder(owner.ConnectionString).Username ?? "darling";
                var batch = DarlingManagedRoles.BuildProvisioningSql(
                    ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
                    15, PasswordReassert.All, ProvisioningTarget.ComposeStore(ownerRole, scratch.DatabaseName));
                await using var provision = new NpgsqlCommand(batch, owner);
                await provision.ExecuteNonQueryAsync(ct);
            }

            await using var ownerSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            var mcpString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                Username = "mcp",
                Password = ProvisioningTestSecrets.McpPassword,
                SearchPath = PgSchemaGenerator.SearchPath,
                Pooling = false,
            }.ConnectionString;
            await using var mcp = NpgsqlDataSource.Create(mcpString);
            ReadCaller.SetAmbientMcpToken("named token ops");
            const string expectedActor = "person:mcp named token ops";

            /* Write a note: approved as written, by its author, journaled note_written; the same shape the web route's store call writes. */
            var fleet = Parse(await DarlingMcpInvestigationAdminTools.WriteInvestigationNote(mcp, "  Nightly ETL runs at 02:00.  ", days: 30, cancellationToken: ct));
            Assert.Equal("written", (string)fleet["status"]!);
            Assert.Equal(expectedActor, (string)fleet["author"]!);
            Assert.Null(fleet["note"]!["before"]);
            Assert.Equal("approved", (string)fleet["note"]!["after"]!["state"]!);
            Assert.Null(fleet["undo"]);
            var fleetId = (long)fleet["note_id"]!;
            Assert.Equal("approved|Nightly ETL runs at 02:00.|" + expectedActor + "|" + expectedActor + "|",
                await ScalarAsync(ownerSource, $"SELECT state || '|' || note_text || '|' || author || '|' || decided_by || '|' || coalesce(server_id::text, '') FROM config.investigation_notes WHERE note_id = {fleetId}", ct));
            Assert.Equal("30", await ScalarAsync(ownerSource, $"SELECT round(extract(epoch FROM expires_at - created_at) / 86400)::int FROM config.investigation_notes WHERE note_id = {fleetId}", ct));
            Assert.Equal(expectedActor, await ScalarAsync(ownerSource, $"SELECT actor FROM config.investigation_journal WHERE event = 'note_written' AND reason = 'note {fleetId}'", ct));

            var scoped = Parse(await DarlingMcpInvestigationAdminTools.WriteInvestigationNote(mcp, "Index rebuilds finish by 03:00.", ServerName, "cpu", cancellationToken: ct));
            var scopedId = (long)scoped["note_id"]!;
            Assert.Equal(ServerName, (string)scoped["server"]!);
            var viaStore = await new InvestigationRequestStore(ownerSource, 30).WriteNoteAsync(ServerId, "cpu", "Index rebuilds finish by 03:00.", expectedActor, cancellationToken: ct);
            Assert.Equal(
                await ScalarAsync(ownerSource, $"SELECT state || '|' || author || '|' || decided_by || '|' || server_id || '|' || metric_name || '|' || round(extract(epoch FROM expires_at - created_at) / 86400)::int FROM config.investigation_notes WHERE note_id = {viaStore}", ct),
                await ScalarAsync(ownerSource, $"SELECT state || '|' || author || '|' || decided_by || '|' || server_id || '|' || metric_name || '|' || round(extract(epoch FROM expires_at - created_at) / 86400)::int FROM config.investigation_notes WHERE note_id = {scopedId}", ct));

            /* Refusals the route has write nothing. */
            var notesBefore = await ScalarAsync(ownerSource, "SELECT count(*) FROM config.investigation_notes", ct);
            foreach (var refused in new[]
            {
                await DarlingMcpInvestigationAdminTools.WriteInvestigationNote(mcp, "   ", cancellationToken: ct),
                await DarlingMcpInvestigationAdminTools.WriteInvestigationNote(mcp, new string('x', 2001), cancellationToken: ct),
                await DarlingMcpInvestigationAdminTools.WriteInvestigationNote(mcp, "ok", days: 0, cancellationToken: ct),
                await DarlingMcpInvestigationAdminTools.WriteInvestigationNote(mcp, "ok", days: 366, cancellationToken: ct),
                await DarlingMcpInvestigationAdminTools.WriteInvestigationNote(mcp, "ok", metric_name: new string('m', 129), cancellationToken: ct),
                await DarlingMcpInvestigationAdminTools.WriteInvestigationNote(mcp, "ok", "no-such-server-6400", cancellationToken: ct),
            })
            {
                Assert.NotEqual("written", (string)Parse(refused)["status"]!);
            }

            Assert.Equal(notesBefore, await ScalarAsync(ownerSource, "SELECT count(*) FROM config.investigation_notes", ct));

            /* Approve a pending agent note: before pending, after approved, stamped and journaled as the MCP caller. */
            var pendingA = await PendingNoteAsync(ownerSource, "agent proposal A", ct);
            var approved = Parse(await DarlingMcpInvestigationAdminTools.ApproveInvestigationNote(mcp, pendingA, cancellationToken: ct));
            Assert.Equal("approved", (string)approved["status"]!);
            Assert.Equal("pending", (string)approved["note"]!["before"]!["state"]!);
            Assert.Equal("approved", (string)approved["note"]!["after"]!["state"]!);
            Assert.Equal(expectedActor, (string)approved["note"]!["after"]!["decided_by"]!);
            Assert.Null(approved["undo"]);
            Assert.Equal("approved|" + expectedActor, await ScalarAsync(ownerSource, $"SELECT state || '|' || decided_by FROM config.investigation_notes WHERE note_id = {pendingA}", ct));
            Assert.Equal(expectedActor, await ScalarAsync(ownerSource, $"SELECT actor FROM config.investigation_journal WHERE event = 'note_approved' AND reason = 'note {pendingA}'", ct));

            /* Reject another; a decided note, a missing one and a second decision are refused and change nothing. */
            var pendingB = await PendingNoteAsync(ownerSource, "agent proposal B", ct);
            var rejected = Parse(await DarlingMcpInvestigationAdminTools.RejectInvestigationNote(mcp, pendingB, cancellationToken: ct));
            Assert.Equal("rejected", (string)rejected["status"]!);
            Assert.Equal("rejected|" + expectedActor, await ScalarAsync(ownerSource, $"SELECT state || '|' || decided_by FROM config.investigation_notes WHERE note_id = {pendingB}", ct));
            Assert.Equal(expectedActor, await ScalarAsync(ownerSource, $"SELECT actor FROM config.investigation_journal WHERE event = 'note_rejected' AND reason = 'note {pendingB}'", ct));
            var journalBefore = await ScalarAsync(ownerSource, "SELECT count(*) FROM config.investigation_journal", ct);
            Assert.Equal("invalid", (string)Parse(await DarlingMcpInvestigationAdminTools.ApproveInvestigationNote(mcp, pendingA, cancellationToken: ct))["status"]!);
            Assert.Equal("invalid", (string)Parse(await DarlingMcpInvestigationAdminTools.RejectInvestigationNote(mcp, pendingA, cancellationToken: ct))["status"]!);
            Assert.Equal("invalid", (string)Parse(await DarlingMcpInvestigationAdminTools.ApproveInvestigationNote(mcp, pendingB, cancellationToken: ct))["status"]!);
            Assert.Equal("invalid", (string)Parse(await DarlingMcpInvestigationAdminTools.RejectInvestigationNote(mcp, 987_654_321, cancellationToken: ct))["status"]!);
            Assert.Equal("approved", await ScalarAsync(ownerSource, $"SELECT state FROM config.investigation_notes WHERE note_id = {pendingA}", ct));
            Assert.Equal("rejected", await ScalarAsync(ownerSource, $"SELECT state FROM config.investigation_notes WHERE note_id = {pendingB}", ct));
            Assert.Equal(journalBefore, await ScalarAsync(ownerSource, "SELECT count(*) FROM config.investigation_journal", ct));

            /* Close a request: open to closed with the reason and the MCP caller, journaled; a second close and bad input write nothing. */
            var store = new InvestigationRequestStore(ownerSource, 30, () => DateTime.UtcNow);
            var requestId = (await store.OpenOrJoinAsync(
                new InvestigationAlert(1, ServerId, ServerName, "blocking", "Blocking", DateTime.UtcNow.AddHours(-1), new[] { ServerId + ":Blocking" }, null,
                    20, 100, LeaseMinutes: 15, ExpiryMinutes: 240), ct)).RequestId!.Value;
            var closed = Parse(await DarlingMcpInvestigationAdminTools.CloseInvestigationRequest(mcp, requestId.ToString(), "  duplicate of the nightly batch  ", cancellationToken: ct));
            Assert.Equal("closed", (string)closed["status"]!);
            Assert.Equal("open", (string)closed["request"]!["before"]!["state"]!);
            Assert.Equal("closed", (string)closed["request"]!["after"]!["state"]!);
            Assert.Equal("duplicate of the nightly batch", (string)closed["request"]!["after"]!["closed_reason"]!);
            Assert.Null(closed["undo"]);
            Assert.Equal("closed|" + expectedActor + "|duplicate of the nightly batch",
                await ScalarAsync(ownerSource, $"SELECT state || '|' || closed_by || '|' || closed_reason FROM config.investigation_requests WHERE request_id = '{requestId}'", ct));
            Assert.Equal(expectedActor, await ScalarAsync(ownerSource, $"SELECT actor FROM config.investigation_journal WHERE event = 'closed' AND request_id = '{requestId}'", ct));
            journalBefore = await ScalarAsync(ownerSource, "SELECT count(*) FROM config.investigation_journal", ct);
            Assert.Equal("invalid", (string)Parse(await DarlingMcpInvestigationAdminTools.CloseInvestigationRequest(mcp, requestId.ToString(), "again", cancellationToken: ct))["status"]!);
            Assert.Equal("invalid", (string)Parse(await DarlingMcpInvestigationAdminTools.CloseInvestigationRequest(mcp, Guid.NewGuid().ToString(), "gone", cancellationToken: ct))["status"]!);
            Assert.Equal("invalid", (string)Parse(await DarlingMcpInvestigationAdminTools.CloseInvestigationRequest(mcp, "not-a-guid", "x", cancellationToken: ct))["status"]!);
            Assert.Equal("invalid", (string)Parse(await DarlingMcpInvestigationAdminTools.CloseInvestigationRequest(mcp, requestId.ToString(), "   ", cancellationToken: ct))["status"]!);
            Assert.Equal("invalid", (string)Parse(await DarlingMcpInvestigationAdminTools.CloseInvestigationRequest(mcp, requestId.ToString(), new string('r', 501), cancellationToken: ct))["status"]!);
            Assert.Equal(journalBefore, await ScalarAsync(ownerSource, "SELECT count(*) FROM config.investigation_journal", ct));

            /* The role holds the columns these statements set, and no statement outside them. */
            await using var check = await mcp.OpenConnectionAsync(ct);
            foreach (var (sql, expected) in new[]
            {
                ("has_table_privilege('config.investigation_notes', 'DELETE')", false),
                ("has_table_privilege('config.investigation_requests', 'INSERT')", false),
                ("has_table_privilege('config.investigation_requests', 'DELETE')", false),
                ("has_column_privilege('config.investigation_notes', 'note_text', 'UPDATE')", false),
                ("has_column_privilege('config.investigation_notes', 'decided_by', 'UPDATE')", true),
                ("has_column_privilege('config.investigation_requests', 'closed_reason', 'UPDATE')", true),
                ("has_column_privilege('config.investigation_requests', 'server_name', 'UPDATE')", false),
            })
            {
                await using var priv = new NpgsqlCommand("SELECT " + sql, check);
                Assert.Equal(expected, (bool)(await priv.ExecuteScalarAsync(ct))!);
            }

            foreach (var outside in new[]
            {
                "DELETE FROM config.investigation_notes",
                "UPDATE config.investigation_notes SET note_text = 'rewritten'",
                /* No mcp code path changes a note's expiry by UPDATE (the decision sets state, decided_by and decided_at; the expiry is set at INSERT). */
                "UPDATE config.investigation_notes SET expires_at = now() + interval '10 years'",
                "UPDATE config.investigation_requests SET server_name = 'x'",
                "UPDATE config.agent_environment_profiles SET scope_server_id = 1",
            })
            {
                var refused = await Assert.ThrowsAsync<PostgresException>(async () =>
                {
                    await using var command = new NpgsqlCommand(outside, check);
                    await command.ExecuteNonQueryAsync(ct);
                });
                Assert.Equal(PostgresErrorCodes.InsufficientPrivilege, refused.SqlState);
            }

            /* The endpoint's one legacy token: the store rows name it "legacy token", never the 8-hex fingerprint the service log carries. */
            ReadCaller.SetAmbientMcpToken(ReadCaller.OfMcpToken("a-legacy-bearer-token"));
            var legacy = Parse(await DarlingMcpInvestigationAdminTools.WriteInvestigationNote(mcp, "Legacy-token note.", days: 30, cancellationToken: ct));
            Assert.Equal("written", (string)legacy["status"]!);
            const string legacyActor = "person:mcp legacy token";
            Assert.Equal(legacyActor, (string)legacy["author"]!);
            var legacyId = (long)legacy["note_id"]!;
            Assert.Equal(legacyActor, await ScalarAsync(ownerSource, $"SELECT author FROM config.investigation_notes WHERE note_id = {legacyId}", ct));
            var legacyJournal = await ScalarAsync(ownerSource, $"SELECT actor FROM config.investigation_journal WHERE event = 'note_written' AND reason = 'note {legacyId}'", ct);
            Assert.Equal(legacyActor, legacyJournal);
            Assert.DoesNotMatch("[0-9a-f]{8}", legacyJournal);

            bodySucceeded = true;
        }
        finally
        {
            ReadCaller.SetAmbientMcpToken(null);
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var drop = new NpgsqlCommand("DROP OWNED BY admin, viewer, mcp; DROP ROLE IF EXISTS admin; DROP ROLE IF EXISTS viewer; DROP ROLE IF EXISTS mcp", cleanup);
                await drop.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }
}
