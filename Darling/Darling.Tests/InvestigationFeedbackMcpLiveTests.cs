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
/// #6400 part 2: <c>start_practice_request</c>, <c>save_environment_profile</c> and <c>record_investigation_feedback</c> as the provisioned,
/// NOSUPERUSER <c>mcp</c> role (the shipped <c>DarlingManagedRoles.BuildProvisioningSql</c> batch run whole). Each tool writes what the
/// store call the web route and the Viewer make writes, each refusal writes nothing, the caller is the token, the role holds no more than
/// the three calls use, and a store whose <c>mcp</c> role lost the grants answers <c>precondition</c>.
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class InvestigationFeedbackMcpLiveTests
{
    private const int ServerId = 7_640_011;
    private const string ServerName = "LcInvFeedbackSrv";

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

    private static async Task<Guid> ReportedCaseAsync(NpgsqlDataSource owner, string family, string metric, CancellationToken ct)
    {
        var store = new InvestigationRequestStore(owner, 30, () => DateTime.UtcNow);
        var id = (await store.OpenOrJoinAsync(
            new InvestigationAlert(1, ServerId, ServerName, family, metric, DateTime.UtcNow.AddHours(-1), new[] { ServerId + ":" + metric }, null,
                20, 100, LeaseMinutes: 15, ExpiryMinutes: 240), ct)).RequestId!.Value;
        await ExecAsync(owner,
            "INSERT INTO config.investigation_reports (request_id, report_format, agent_id, claim_id, verdict, root_cause, evidence, server_id) "
            + $"VALUES ('{id}', 1, 'agent-1', gen_random_uuid(), 'real', 'a cause', '[{{\"k\":1}}]'::jsonb, {ServerId})", ct);
        return id;
    }

    private static string PracticeShape(string id) =>
        "SELECT route_id || '|' || server_id || '|' || server_name || '|' || lease_minutes || '|' || autonomy_level || '|' "
        + $"|| alert_count || '|' || (practice_of IS NOT NULL) || '|' || (SELECT count(*) FROM config.investigation_request_alerts a WHERE a.request_id = r.request_id) "
        + $"FROM config.investigation_requests r WHERE request_id = '{id}'";

    [Fact]
    public async Task TheProvisionedMcpRole_StartsPracticeSavesProfilesAndRecordsFeedback_LikeTheWebStoreCalls_AndHoldsNothingWider()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the investigation feedback tool test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using (var owner = new NpgsqlConnection(scratch.ConnectionString))
        {
            await owner.OpenAsync(ct);
            await PgMigrations.MigrateAsync(owner, ct);
            await DarlingMcpTestData.RegisterServerAsync(owner, ServerId, ServerName, ct);
            /* #6400: a profile may name a server the operator defined (config_monitored_servers), not merely one the worker observed (collect.servers). */
            await DarlingMcpTestData.ExecAsync(owner, ct, "INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES ($1, $2, 'host-a')", ServerId, ServerName);
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
            var expectedAuthor = InvestigationFeedbackRules.AuthorFromIdentity("mcp named token ops")!;

            /* Feedback: the row the web route's store call writes for the same input, under the token's name. */
            var caseA = await ReportedCaseAsync(ownerSource, "blocking", "Blocking", ct);
            var caseB = await ReportedCaseAsync(ownerSource, "resources", "Waits", ct);
            var noReport = (await new InvestigationRequestStore(ownerSource, 30, () => DateTime.UtcNow).OpenOrJoinAsync(
                new InvestigationAlert(1, ServerId, ServerName, "jobs", "Cpu", DateTime.UtcNow.AddHours(-1), new[] { ServerId + ":Cpu" }, null,
                    20, 100, LeaseMinutes: 15, ExpiryMinutes: 240), ct)).RequestId!.Value;
            var recorded = Parse(await DarlingMcpInvestigationFeedbackTools.RecordInvestigationFeedback(
                mcp, caseA.ToString(), "wrong", "false_positive", "  checked the lock chain  ", "a comment", cancellationToken: ct));
            Assert.Equal("recorded", (string)recorded["status"]!);
            Assert.Equal(expectedAuthor, (string)recorded["author"]!);
            Assert.Null(recorded["feedback"]!["before"]);
            Assert.Equal("wrong", (string)recorded["feedback"]!["after"]!["verdict"]!);
            Assert.Null(recorded["undo"]);
            var viaStore = await new InvestigationFeedbackStore(ownerSource, 30).RecordAsync(
                new InvestigationFeedbackSubmission(caseA, "wrong", "false_positive", "  checked the lock chain  ", "a comment", "web-person"), ct);
            Assert.Equal(InvestigationFeedbackOutcome.Recorded, viaStore.Outcome);
            var shape = "verdict || '|' || coalesce(corrected_verdict, '') || '|' || coalesce(missed_step, '') || '|' || coalesce(comment, '')";
            Assert.Equal(
                await ScalarAsync(ownerSource, $"SELECT {shape} FROM config.investigation_feedback WHERE request_id = '{caseA}' AND author = 'web-person'", ct),
                await ScalarAsync(ownerSource, $"SELECT {shape} FROM config.investigation_feedback WHERE request_id = '{caseA}' AND author = '{expectedAuthor}'", ct));
            var replaced = Parse(await DarlingMcpInvestigationFeedbackTools.RecordInvestigationFeedback(mcp, caseA.ToString(), "partly", cancellationToken: ct));
            Assert.Equal("replaced", (string)replaced["status"]!);
            Assert.Equal("wrong", (string)replaced["feedback"]!["before"]!["verdict"]!);
            Assert.Equal("partly", (string)replaced["feedback"]!["after"]!["verdict"]!);
            Assert.Equal("1", await ScalarAsync(ownerSource, $"SELECT count(*) FROM config.investigation_feedback WHERE request_id = '{caseA}' AND author = '{expectedAuthor}'", ct));

            var feedbackBefore = await ScalarAsync(ownerSource, "SELECT count(*) FROM config.investigation_feedback", ct);
            foreach (var refused in new[]
            {
                await DarlingMcpInvestigationFeedbackTools.RecordInvestigationFeedback(mcp, caseB.ToString(), "bogus", cancellationToken: ct),
                await DarlingMcpInvestigationFeedbackTools.RecordInvestigationFeedback(mcp, caseB.ToString(), "", cancellationToken: ct),
                await DarlingMcpInvestigationFeedbackTools.RecordInvestigationFeedback(mcp, caseB.ToString(), "right", "maybe", cancellationToken: ct),
                await DarlingMcpInvestigationFeedbackTools.RecordInvestigationFeedback(mcp, caseB.ToString(), "right", comment: new string('c', 4001), cancellationToken: ct),
                await DarlingMcpInvestigationFeedbackTools.RecordInvestigationFeedback(mcp, caseB.ToString(), "right", missed_step: new string('m', 4001), cancellationToken: ct),
                await DarlingMcpInvestigationFeedbackTools.RecordInvestigationFeedback(mcp, "not-a-guid", "right", cancellationToken: ct),
                await DarlingMcpInvestigationFeedbackTools.RecordInvestigationFeedback(mcp, noReport.ToString(), "right", cancellationToken: ct),
                await DarlingMcpInvestigationFeedbackTools.RecordInvestigationFeedback(mcp, Guid.NewGuid().ToString(), "right", cancellationToken: ct),
            })
            {
                Assert.Equal("invalid", (string)Parse(refused)["status"]!);
            }

            Assert.Equal(feedbackBefore, await ScalarAsync(ownerSource, "SELECT count(*) FROM config.investigation_feedback", ct));

            /* Practice: the request row, its alert copy and the opened journal row the store call writes; refusals write nothing. */
            var requestsBefore = await ScalarAsync(ownerSource, "SELECT count(*) FROM config.investigation_requests", ct);
            var journalBefore = await ScalarAsync(ownerSource, "SELECT count(*) FROM config.investigation_journal", ct);
            Assert.Equal("invalid", (string)Parse(await DarlingMcpInvestigationFeedbackTools.StartPracticeRequest(mcp, caseB.ToString(), cancellationToken: ct))["status"]!);
            Assert.Equal("invalid", (string)Parse(await DarlingMcpInvestigationFeedbackTools.StartPracticeRequest(mcp, noReport.ToString(), cancellationToken: ct))["status"]!);
            Assert.Equal("invalid", (string)Parse(await DarlingMcpInvestigationFeedbackTools.StartPracticeRequest(mcp, "not-a-guid", cancellationToken: ct))["status"]!);
            Assert.Equal("invalid", (string)Parse(await DarlingMcpInvestigationFeedbackTools.StartPracticeRequest(mcp, Guid.NewGuid().ToString(), cancellationToken: ct))["status"]!);
            Assert.Equal(requestsBefore, await ScalarAsync(ownerSource, "SELECT count(*) FROM config.investigation_requests", ct));
            Assert.Equal(journalBefore, await ScalarAsync(ownerSource, "SELECT count(*) FROM config.investigation_journal", ct));

            var opened = Parse(await DarlingMcpInvestigationFeedbackTools.StartPracticeRequest(mcp, caseA.ToString(), cancellationToken: ct));
            Assert.Equal("opened", (string)opened["status"]!);
            Assert.Equal(expectedActor, (string)opened["opened_by"]!);
            Assert.Null(opened["undo"]);
            var practiceId = (string)opened["request_id"]!;
            Assert.Equal(caseA.ToString(), (string)opened["source_request_id"]!);
            Assert.Equal(caseA.ToString(), await ScalarAsync(ownerSource, $"SELECT practice_of FROM config.investigation_requests WHERE request_id = '{practiceId}'", ct));
            Assert.Equal(expectedActor, await ScalarAsync(ownerSource, $"SELECT actor FROM config.investigation_journal WHERE event = 'opened' AND request_id = '{practiceId}'", ct));
            await new InvestigationFeedbackStore(ownerSource, 30).RecordAsync(new InvestigationFeedbackSubmission(caseB, "right", null, null, null, "web-person"), ct);
            var viaPractice = await new InvestigationPracticeStore(ownerSource, 30).OpenAsync(caseB, "person:web-person", ct);
            Assert.Equal(InvestigationPracticeOutcome.Opened, viaPractice.Outcome);
            Assert.Equal(
                await ScalarAsync(ownerSource, PracticeShape(viaPractice.RequestId!.Value.ToString()), ct),
                (await ScalarAsync(ownerSource, PracticeShape(practiceId), ct))!);
            /* A second start of a live case, and a start of a practice request, are refused and write nothing. */
            requestsBefore = await ScalarAsync(ownerSource, "SELECT count(*) FROM config.investigation_requests", ct);
            Assert.Equal("invalid", (string)Parse(await DarlingMcpInvestigationFeedbackTools.StartPracticeRequest(mcp, caseA.ToString(), cancellationToken: ct))["status"]!);
            Assert.Equal("invalid", (string)Parse(await DarlingMcpInvestigationFeedbackTools.StartPracticeRequest(mcp, practiceId, cancellationToken: ct))["status"]!);
            Assert.Equal(requestsBefore, await ScalarAsync(ownerSource, "SELECT count(*) FROM config.investigation_requests", ct));

            /* Profile: the row and the journal row SaveProfileAsync writes, the stale-edit guard, and the blank save that removes. */
            var fleet = Parse(await DarlingMcpInvestigationFeedbackTools.SaveEnvironmentProfile(mcp, "  Nightly ETL at 02:00.  ", cancellationToken: ct));
            Assert.Equal("saved", (string)fleet["status"]!);
            Assert.Null(fleet["profile"]!["before"]);
            Assert.Equal("Nightly ETL at 02:00.", (string)fleet["profile"]!["after"]!["profile_text"]!);
            Assert.Equal(expectedActor, (string)fleet["profile"]!["after"]!["modified_by"]!);
            Assert.Equal("profile_saved|" + expectedActor + "|fleet profile",
                await ScalarAsync(ownerSource, "SELECT event || '|' || actor || '|' || reason FROM config.investigation_journal WHERE event = 'profile_saved' ORDER BY journal_id DESC LIMIT 1", ct));
            var server = Parse(await DarlingMcpInvestigationFeedbackTools.SaveEnvironmentProfile(mcp, "Reporting server.", ServerName, cancellationToken: ct));
            Assert.Equal("saved", (string)server["status"]!);
            var profilesBefore = await ScalarAsync(ownerSource, "SELECT count(*) FROM config.agent_environment_profiles", ct);
            var profileJournalBefore = await ScalarAsync(ownerSource, "SELECT count(*) FROM config.investigation_journal", ct);
            var stale = Parse(await DarlingMcpInvestigationFeedbackTools.SaveEnvironmentProfile(mcp, "Lost update.", cancellationToken: ct));
            Assert.Equal("invalid", (string)stale["status"]!);
            Assert.Contains("modified_at", (string)stale["message"]!, StringComparison.Ordinal);
            foreach (var refused in new[]
            {
                await DarlingMcpInvestigationFeedbackTools.SaveEnvironmentProfile(mcp, new string('p', 4001), ServerName, cancellationToken: ct),
                await DarlingMcpInvestigationFeedbackTools.SaveEnvironmentProfile(mcp, "both scopes", ServerName, 1, cancellationToken: ct),
                await DarlingMcpInvestigationFeedbackTools.SaveEnvironmentProfile(mcp, "no such server", "no-such-server-6400", cancellationToken: ct),
                await DarlingMcpInvestigationFeedbackTools.SaveEnvironmentProfile(mcp, "bad stamp", expected_modified_at: "yesterday-ish", cancellationToken: ct),
            })
            {
                Assert.True((string)Parse(refused)["status"]! != "saved", refused);
            }

            Assert.Equal(profilesBefore, await ScalarAsync(ownerSource, "SELECT count(*) FROM config.agent_environment_profiles", ct));
            Assert.Equal(profileJournalBefore, await ScalarAsync(ownerSource, "SELECT count(*) FROM config.investigation_journal", ct));
            var edited = Parse(await DarlingMcpInvestigationFeedbackTools.SaveEnvironmentProfile(
                mcp, "Nightly ETL at 03:00.", expected_modified_at: (string)fleet["profile"]!["after"]!["modified_at"]!, cancellationToken: ct));
            Assert.Equal("saved", (string)edited["status"]!);
            Assert.Equal("Nightly ETL at 02:00.", (string)edited["profile"]!["before"]!["profile_text"]!);
            var removed = Parse(await DarlingMcpInvestigationFeedbackTools.SaveEnvironmentProfile(
                mcp, "  ", expected_modified_at: (string)edited["profile"]!["after"]!["modified_at"]!, cancellationToken: ct));
            Assert.Equal("removed", (string)removed["status"]!);
            Assert.Null(removed["profile"]!["after"]);
            Assert.Equal("0", await ScalarAsync(ownerSource, "SELECT count(*) FROM config.agent_environment_profiles WHERE scope_server_id IS NULL AND scope_tag_id IS NULL", ct));
            Assert.Equal("profile_removed", await ScalarAsync(ownerSource, "SELECT event FROM config.investigation_journal WHERE event LIKE 'profile_%' ORDER BY journal_id DESC LIMIT 1", ct));

            /* #6400: a server that was removed keeps its collect.servers row (the registry is never purged), so its name still resolves; with no
               config_monitored_servers row it is not registered, and the save is refused with the store's sentence and writes nothing. */
            await ExecAsync(ownerSource, $"INSERT INTO collect.servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date) VALUES ({ServerId + 1}, 'LcFeedbackRemoved', 'LcFeedbackRemoved', TRUE, 15, now(), now())", ct);
            var removedRows = await ScalarAsync(ownerSource, "SELECT count(*) FROM config.agent_environment_profiles", ct);
            var removedJournal = await ScalarAsync(ownerSource, "SELECT count(*) FROM config.investigation_journal", ct);
            var removedServer = Parse(await DarlingMcpInvestigationFeedbackTools.SaveEnvironmentProfile(mcp, "Removed server.", "LcFeedbackRemoved", cancellationToken: ct));
            Assert.Equal("invalid", (string)removedServer["status"]!);
            Assert.Contains($"Server {ServerId + 1} is not a registered server, so no profile was saved.", (string)removedServer["message"]!, StringComparison.Ordinal);
            Assert.Equal(removedRows, await ScalarAsync(ownerSource, "SELECT count(*) FROM config.agent_environment_profiles", ct));
            Assert.Equal(removedJournal, await ScalarAsync(ownerSource, "SELECT count(*) FROM config.investigation_journal", ct));

            /* #6400: tag 0, a negative tag and a tag that names no tag are invalid and write nothing (the profile table has no foreign key to
               the tags); a fleet save afterwards still works, and an existing tag saves. The mcp role reads the tag with its config SELECT. */
            var tagId = (await ScalarAsync(ownerSource, "INSERT INTO config.server_tags (name) VALUES ('oltp-6400') RETURNING id", ct))!;
            var noTag = (await ScalarAsync(ownerSource, "SELECT max(id) + 1 FROM config.server_tags", ct))!;
            var tagJournalBefore = await ScalarAsync(ownerSource, "SELECT count(*) FROM config.investigation_journal", ct);
            var profilesAtStart = int.Parse((await ScalarAsync(ownerSource, "SELECT count(*) FROM config.agent_environment_profiles", ct))!, CultureInfo.InvariantCulture);
            foreach (var badTag in new[] { "0", "-1", noTag })
            {
                var invalid = Parse(await DarlingMcpInvestigationFeedbackTools.SaveEnvironmentProfile(
                    mcp, "for no tag", tag_id: int.Parse(badTag, CultureInfo.InvariantCulture), cancellationToken: ct));
                Assert.Equal("invalid", (string)invalid["status"]!);
                Assert.Contains(badTag, (string)invalid["message"]!, StringComparison.Ordinal);
                Assert.Contains("Nothing was changed", (string)invalid["message"]!, StringComparison.Ordinal);
            }

            Assert.Equal(profilesAtStart.ToString(CultureInfo.InvariantCulture), await ScalarAsync(ownerSource, "SELECT count(*) FROM config.agent_environment_profiles", ct));
            Assert.Equal(tagJournalBefore, await ScalarAsync(ownerSource, "SELECT count(*) FROM config.investigation_journal", ct));
            Assert.Equal("saved", (string)Parse(await DarlingMcpInvestigationFeedbackTools.SaveEnvironmentProfile(mcp, "Fleet after the refusals.", cancellationToken: ct))["status"]!);
            Assert.Equal("saved", (string)Parse(await DarlingMcpInvestigationFeedbackTools.SaveEnvironmentProfile(
                mcp, "OLTP tag.", tag_id: int.Parse(tagId, CultureInfo.InvariantCulture), cancellationToken: ct))["status"]!);
            Assert.Equal((profilesAtStart + 2).ToString(CultureInfo.InvariantCulture), await ScalarAsync(ownerSource, "SELECT count(*) FROM config.agent_environment_profiles", ct));
            await ExecAsync(ownerSource, "DELETE FROM config.agent_environment_profiles WHERE scope_server_id IS NULL", ct);

            /* The role holds the columns these statements set, and no statement outside them. */
            await using var check = await mcp.OpenConnectionAsync(ct);
            foreach (var (sql, expected) in new[]
            {
                ("has_table_privilege('config.investigation_feedback', 'DELETE')", false),
                ("has_column_privilege('config.investigation_feedback', 'request_id', 'UPDATE')", false),
                ("has_column_privilege('config.investigation_feedback', 'verdict', 'UPDATE')", true),
                ("has_column_privilege('config.investigation_requests', 'practice_of', 'INSERT')", true),
                ("has_column_privilege('config.investigation_requests', 'claimed_by', 'INSERT')", false),
                ("has_table_privilege('config.investigation_request_alerts', 'UPDATE')", false),
                ("has_column_privilege('config.agent_environment_profiles', 'profile_id', 'INSERT')", false),
                ("has_column_privilege('config.agent_environment_profiles', 'scope_server_id', 'UPDATE')", false),
                ("has_column_privilege('config.agent_environment_profiles', 'profile_text', 'UPDATE')", true),
            })
            {
                await using var priv = new NpgsqlCommand("SELECT " + sql, check);
                Assert.Equal(expected, (bool)(await priv.ExecuteScalarAsync(ct))!);
            }

            foreach (var outside in new[]
            {
                "DELETE FROM config.investigation_feedback",
                "UPDATE config.investigation_feedback SET request_id = gen_random_uuid()",
                "UPDATE config.agent_environment_profiles SET scope_server_id = 1",
                "UPDATE config.investigation_request_alerts SET metric_name = 'x'",
                "DELETE FROM config.investigation_request_alerts",
            })
            {
                var refused = await Assert.ThrowsAsync<PostgresException>(async () =>
                {
                    await using var command = new NpgsqlCommand(outside, check);
                    await command.ExecuteNonQueryAsync(ct);
                });
                Assert.Equal(PostgresErrorCodes.InsufficientPrivilege, refused.SqlState);
            }

            /* A store whose mcp role was not re-provisioned answers precondition, and writes nothing. */
            await ExecAsync(ownerSource,
                "REVOKE INSERT, UPDATE ON config.investigation_feedback FROM mcp; REVOKE INSERT ON config.investigation_requests FROM mcp; "
                + "REVOKE INSERT, UPDATE, DELETE ON config.agent_environment_profiles FROM mcp", ct);
            Assert.Equal("precondition", (string)Parse(await DarlingMcpInvestigationFeedbackTools.RecordInvestigationFeedback(mcp, caseB.ToString(), "wrong", cancellationToken: ct))["status"]!);
            Assert.Equal("precondition", (string)Parse(await DarlingMcpInvestigationFeedbackTools.SaveEnvironmentProfile(mcp, "Needs grants.", cancellationToken: ct))["status"]!);
            var caseC = await ReportedCaseAsync(ownerSource, "availability", "Down", ct);
            await new InvestigationFeedbackStore(ownerSource, 30).RecordAsync(new InvestigationFeedbackSubmission(caseC, "right", null, null, null, "web-person"), ct);
            Assert.Equal("precondition", (string)Parse(await DarlingMcpInvestigationFeedbackTools.StartPracticeRequest(mcp, caseC.ToString(), cancellationToken: ct))["status"]!);
            Assert.Equal("0", await ScalarAsync(ownerSource, $"SELECT count(*) FROM config.investigation_requests WHERE practice_of = '{caseC}'", ct));
            Assert.Equal("0", await ScalarAsync(ownerSource, "SELECT count(*) FROM config.agent_environment_profiles WHERE profile_text = 'Needs grants.'", ct));

            bodySucceeded = true;
        }
        finally
        {
            ReadCaller.SetAmbientMcpToken(null);
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var drop = new NpgsqlCommand("DROP OWNED BY admin, viewer, web, mcp; DROP ROLE IF EXISTS admin; DROP ROLE IF EXISTS viewer; DROP ROLE IF EXISTS web; DROP ROLE IF EXISTS mcp", cleanup);
                await drop.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }
}
