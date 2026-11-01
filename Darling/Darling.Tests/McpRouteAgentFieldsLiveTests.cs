/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Security.Cryptography;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database and a role with a per-run name, so the class is deliberately NOT
   [Collection("live-postgres")]. */

/// <summary>
/// #6542 part 2: the route agent fields over MCP, AS the <c>mcp</c> role with the grants taken from
/// <see cref="DarlingManagedRoles.BuildProvisioningSql"/> and the functions and the journal trigger built by the owner, as in a
/// deployment. An operator-class token (a named operator, or the legacy token with writes) sets levels 0 to 3 and journals
/// <c>route_agent_changed</c> as <c>operator:</c> and its name or identity.
/// </summary>
public sealed class McpRouteAgentFieldsLiveTests
{
    private const string Operator = "ops-lead";
    private static readonly string RolePassword = Convert.ToHexString(RandomNumberGenerator.GetBytes(16));

    private static DarlingMcpServedPolicy OperatorPolicy() =>
        new(new DarlingMcpWritePolicy.Effective(true, false, false, Operator));

    /// <summary>The legacy single token with writes on: no token name, operator-class.</summary>
    private static DarlingMcpServedPolicy LegacyPolicy() => new(new DarlingMcpWritePolicy.Effective(true, false));

    private static string Status(string answer) => JsonDocument.Parse(answer).RootElement.GetProperty("status").GetString()!;

    private static int RouteIdOf(string answer) =>
        JsonDocument.Parse(answer).RootElement.GetProperty("route").GetProperty("route_id").GetInt32();

    private static async Task<string?> ScalarAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    [Fact]
    public async Task AnOperatorToken_CreatesARouteAtLevel1_AndTheJournalNamesTheToken()
    {
        await WithMcpRoleAsync(async (owner, asMcp, ct) =>
        {
            var created = await DarlingMcpAlertTools.CreateNotificationRoute(
                asMcp, "Blocking", agent_level: 1, agent_max_per_hour: 5, served: OperatorPolicy(), cancellationToken: ct);
            Assert.Equal("created", Status(created));
            var routeId = RouteIdOf(created);

            Assert.Equal("1|5", await ScalarAsync(owner,
                $"SELECT agent_level::text || '|' || agent_max_per_hour::text FROM config.config_notification_routes WHERE route_id = {routeId}", ct));
            Assert.Equal("1", await ScalarAsync(owner,
                $"SELECT count(*) FROM config.investigation_journal WHERE route_id = {routeId} AND event = '{NotificationRouteWriter.AgentChangedEvent}' " +
                $"AND actor = 'operator:{Operator}' AND (detail -> 'after' ->> 'agent_level') = '1' AND detail -> 'before' = 'null'::jsonb", ct));

            /* An edit of the limits journals again, with the values before and after. */
            var edited = await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, routeId, agent_level: 2, agent_lease_minutes: 30, served: OperatorPolicy(), cancellationToken: ct);
            Assert.Equal("updated", Status(edited));
            Assert.Equal("2|30", await ScalarAsync(owner,
                $"SELECT agent_level::text || '|' || agent_lease_minutes::text FROM config.config_notification_routes WHERE route_id = {routeId}", ct));
            Assert.Equal("1", await ScalarAsync(owner,
                $"SELECT count(*) FROM config.investigation_journal WHERE route_id = {routeId} AND event = 'route_agent_changed' " +
                $"AND actor = 'operator:{Operator}' AND reason = 'changed agent_level, agent_lease_minutes' " +
                "AND (detail -> 'before' ->> 'agent_level') = '1' AND (detail -> 'after' ->> 'agent_level') = '2'", ct));

            /* The Viewer's rules hold: a limit out of range is invalid and nothing changes. */
            var outOfRange = await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, routeId, agent_max_per_hour: 500, served: OperatorPolicy(), cancellationToken: ct);
            Assert.Equal("invalid", Status(outOfRange));
            Assert.Equal("5", await ScalarAsync(owner, $"SELECT agent_max_per_hour FROM config.config_notification_routes WHERE route_id = {routeId}", ct));
        });
    }

    /// <summary>Erik, 10-23: MCP is full admin, so an operator token sets level 3 and changes a route already at level 3, journaled each time.</summary>
    [Fact]
    public async Task AnOperatorToken_SetsLevel3_AndChangesARouteAtLevel3_EachJournaled()
    {
        await WithMcpRoleAsync(async (owner, asMcp, ct) =>
        {
            var created = await DarlingMcpAlertTools.CreateNotificationRoute(
                asMcp, "Deadlocks", agent_level: 3, served: OperatorPolicy(), cancellationToken: ct);
            Assert.Equal("created", Status(created));
            var routeId = RouteIdOf(created);
            Assert.Equal("3", await ScalarAsync(owner, $"SELECT agent_level FROM config.config_notification_routes WHERE route_id = {routeId}", ct));
            Assert.Equal("1", await ScalarAsync(owner,
                $"SELECT count(*) FROM config.investigation_journal WHERE route_id = {routeId} AND event = 'route_agent_changed' " +
                $"AND actor = 'operator:{Operator}' AND (detail -> 'after' ->> 'agent_level') = '3'", ct));

            /* A route at level 3: a limit changes, then the level is lowered, each with its own row. */
            Assert.Equal("updated", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, routeId, agent_max_per_day: 50, served: OperatorPolicy(), cancellationToken: ct)));
            Assert.Equal("updated", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, routeId, agent_level: 1, served: OperatorPolicy(), cancellationToken: ct)));
            Assert.Equal("1|50", await ScalarAsync(owner,
                $"SELECT agent_level::text || '|' || agent_max_per_day::text FROM config.config_notification_routes WHERE route_id = {routeId}", ct));
            Assert.Equal("1", await ScalarAsync(owner,
                $"SELECT count(*) FROM config.investigation_journal WHERE route_id = {routeId} AND actor = 'operator:{Operator}' " +
                "AND reason = 'changed agent_level' AND (detail -> 'before' ->> 'agent_level') = '3' AND (detail -> 'after' ->> 'agent_level') = '1'", ct));
            Assert.Equal("3", await ScalarAsync(owner,
                $"SELECT count(*) FROM config.investigation_journal WHERE route_id = {routeId} AND event = 'route_agent_changed'", ct));

            /* The store function refuses an actor that is not operator:<name>. Every token connects as the same mcp role, so the actor is a label
               the service passes: the real gate is the service's tool filter (NeverAgentTools, IsOperator). This pins the label's check. */
            var direct = await Assert.ThrowsAsync<PostgresException>(async () =>
            {
                await using var command = asMcp.CreateCommand(
                    $"SELECT config.set_notification_route_agent({routeId}, 'agent:{Operator}', ARRAY['agent_level'], 2, NULL, NULL, NULL, NULL, NULL, false, NULL)");
                await command.ExecuteNonQueryAsync(ct);
            });
            Assert.Equal("DL400", direct.SqlState);
        });
    }

    /// <summary>#6395 and M2 of the #6593 review: MCP is full admin, so an operator token changes send_resolutions and enabled on an agent route
    /// (each answers updated, and the enabled change is journaled as the Viewer's toggle journals it). The store's update function refuses the
    /// same change for any actor that is not operator:, so a caller the service does not pass as an operator gets DL403. The route is at level 2:
    /// a level 3 route with action approval required cannot be disabled over MCP (#7109, <see cref="ALevel3RouteWithApprovalRequired_CannotBeDisabledClearedOrReshaped_ButCanBeLowered"/>).</summary>
    [Fact]
    public async Task AnOperatorToken_ChangesSendResolutionsAndEnabled_OnAnAgentRoute_AndTheStoreRefusesAnyOtherActor()
    {
        await WithMcpRoleAsync(async (owner, asMcp, ct) =>
        {
            var routeId = RouteIdOf(await DarlingMcpAlertTools.CreateNotificationRoute(
                asMcp, "Deadlocks", agent_level: 2, served: OperatorPolicy(), cancellationToken: ct));

            Assert.Equal("updated", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, routeId, send_resolutions: "always", served: OperatorPolicy(), cancellationToken: ct)));
            Assert.Equal("updated", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, routeId, enabled: false, served: OperatorPolicy(), cancellationToken: ct)));
            Assert.Equal("true|false|2", await ScalarAsync(owner,
                $"SELECT send_resolutions::text || '|' || enabled::text || '|' || agent_level::text FROM config.config_notification_routes WHERE route_id = {routeId}", ct));
            Assert.Equal("1", await ScalarAsync(owner,
                $"SELECT count(*) FROM config.investigation_journal WHERE route_id = {routeId} AND event = 'route_agent_changed' " +
                $"AND actor = 'operator:{Operator}' AND reason = 'changed enabled' AND (detail -> 'before' ->> 'enabled') = 'true' " +
                "AND (detail -> 'after' ->> 'enabled') = 'false'", ct));

            /* The two single-field tools take the same path for an operator token. */
            Assert.Equal("updated", Status(await DarlingMcpAlertTools.SetNotificationRouteEnabled(
                asMcp, routeId, true, served: OperatorPolicy(), cancellationToken: ct)));
            Assert.Equal("true", await ScalarAsync(owner, $"SELECT enabled::text FROM config.config_notification_routes WHERE route_id = {routeId}", ct));

            /* Any other actor: NULL (a caller that is not operator-class) and an agent: label are refused in the store, and nothing changes. */
            foreach (var actor in new[] { "NULL", "'agent:triage-bot'" })
            {
                var refused = await Assert.ThrowsAsync<PostgresException>(async () =>
                {
                    await using var command = asMcp.CreateCommand(
                        $"SELECT config.update_notification_route({routeId}, {actor}, ARRAY['send_resolutions'], 'Deadlocks', true, NULL, NULL, false, NULL)");
                    await command.ExecuteNonQueryAsync(ct);
                });
                Assert.Equal("DL403", refused.SqlState);
            }

            Assert.Equal("true", await ScalarAsync(owner, $"SELECT send_resolutions::text FROM config.config_notification_routes WHERE route_id = {routeId}", ct));

            /* A caller that is not operator-class still gets refused by the tool, before the store. */
            var investigations = new DarlingMcpServedPolicy(new DarlingMcpWritePolicy.Effective(false, false, Investigations: true));
            Assert.Equal("refused", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, routeId, send_resolutions: "never", served: investigations, cancellationToken: ct)));
        });
    }

    /// <summary>L2 of the #6593 review: one call that changes metric_match and agent_level journals what the Viewer's save would, "changed
    /// metric_match, agent_level", with the match before the change in before.</summary>
    [Fact]
    public async Task OneCall_ChangingTheMatchAndTheLevel_JournalsBoth_WithTheOldMatchInBefore()
    {
        await WithMcpRoleAsync(async (owner, asMcp, ct) =>
        {
            var routeId = RouteIdOf(await DarlingMcpAlertTools.CreateNotificationRoute(
                asMcp, "Blocking", served: OperatorPolicy(), cancellationToken: ct));

            Assert.Equal("updated", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, routeId, metric_match: "Deadlocks", agent_level: 1, served: OperatorPolicy(), cancellationToken: ct)));

            Assert.Equal("1", await ScalarAsync(owner,
                $"SELECT count(*) FROM config.investigation_journal WHERE route_id = {routeId} AND event = 'route_agent_changed' " +
                $"AND actor = 'operator:{Operator}' AND reason = 'changed metric_match, agent_level' " +
                "AND (detail -> 'before' ->> 'metric_match') = 'Blocking' AND (detail -> 'after' ->> 'metric_match') = 'Deadlocks' " +
                "AND (detail -> 'before' ->> 'agent_level') IS NULL AND (detail -> 'after' ->> 'agent_level') = '1'", ct));
        });
    }

    /// <summary>Agreed with the coordinator, 10-23: the legacy single token served the writes is operator-class, so it sets agent levels 1 and 3
    /// and edits an agent route, each change journaled under operator: and the token identity the route write logs carry. A token served the
    /// investigations instead (writes off) is not operator-class and sets no agent field.</summary>
    [Fact]
    public async Task TheLegacyWritesToken_SetsLevels1And3_AndEachIsJournaled_ButAnInvestigationsTokenSetsNone()
    {
        await WithMcpRoleAsync(async (owner, asMcp, ct) =>
        {
            var created = await DarlingMcpAlertTools.CreateNotificationRoute(
                asMcp, "Blocking", agent_level: 1, served: LegacyPolicy(), cancellationToken: ct);
            Assert.Equal("created", Status(created));
            var routeId = RouteIdOf(created);

            Assert.Equal("updated", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, routeId, agent_level: 3, served: LegacyPolicy(), cancellationToken: ct)));
            Assert.Equal("3", await ScalarAsync(owner, $"SELECT agent_level FROM config.config_notification_routes WHERE route_id = {routeId}", ct));
            Assert.Equal("2", await ScalarAsync(owner,
                $"SELECT count(*) FROM config.investigation_journal WHERE route_id = {routeId} AND event = 'route_agent_changed' " +
                "AND actor LIKE 'operator:%' AND actor NOT LIKE 'operator:" + Operator + "'", ct));
            Assert.Equal("1", await ScalarAsync(owner,
                $"SELECT count(*) FROM config.investigation_journal WHERE route_id = {routeId} AND (detail -> 'after' ->> 'agent_level') = '3'", ct));

            /* The agent route's other fields are no longer refused for it either. */
            Assert.Equal("updated", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, routeId, send_resolutions: "always", served: LegacyPolicy(), cancellationToken: ct)));

            /* Investigations on, writes off: not operator-class, so no agent field. */
            var investigations = new DarlingMcpServedPolicy(new DarlingMcpWritePolicy.Effective(false, false, Investigations: true));
            Assert.Equal("refused", Status(await DarlingMcpAlertTools.CreateNotificationRoute(
                asMcp, "Deadlocks", agent_level: 1, served: investigations, cancellationToken: ct)));
            Assert.Equal("refused", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, routeId, agent_level: 1, served: investigations, cancellationToken: ct)));
            Assert.Equal("3", await ScalarAsync(owner, $"SELECT agent_level FROM config.config_notification_routes WHERE route_id = {routeId}", ct));
        });
    }

    /// <summary>The legacy token's route change is journaled as <c>operator:legacy token</c>: the store row holds the token's kind, never the
    /// hex fingerprint of its value that the write log lines carry.</summary>
    [Fact]
    public async Task TheLegacyWritesToken_IsJournaledWithItsKind_NotItsFingerprint()
    {
        await WithMcpRoleAsync(async (owner, asMcp, ct) =>
        {
            var fingerprint = ReadCaller.OfMcpToken("a-legacy-bearer-token");
            Assert.Matches("[0-9a-f]{8}", fingerprint);
            ReadCaller.SetAmbientMcpToken(fingerprint);
            try
            {
                var created = await DarlingMcpAlertTools.CreateNotificationRoute(
                    asMcp, "Blocking", agent_level: 1, served: LegacyPolicy(), cancellationToken: ct);
                Assert.Equal("created", Status(created));
                var actor = await ScalarAsync(owner,
                    $"SELECT actor FROM config.investigation_journal WHERE route_id = {RouteIdOf(created)} AND event = 'route_agent_changed'", ct);
                Assert.Equal("operator:legacy token", actor);
                Assert.DoesNotMatch("[0-9a-f]{8}", actor!);
            }
            finally
            {
                ReadCaller.SetAmbientMcpToken(null);
            }
        });
    }

    /// <summary>L5 of the #6593 review: an operator token puts a level 2 route back to Inherit with clear_agent_level. The stored level is NULL,
    /// the limits are kept (the Viewer's Inherit keeps them), the journal row has before 2 and after null, and previous shows level 2.</summary>
    [Fact]
    public async Task AnOperatorToken_ClearsALevel2Route_BackToInherit_AndTheJournalShowsBefore2AfterNull()
    {
        await WithMcpRoleAsync(async (owner, asMcp, ct) =>
        {
            var routeId = RouteIdOf(await DarlingMcpAlertTools.CreateNotificationRoute(
                asMcp, "Blocking", agent_level: 2, agent_max_per_hour: 7, served: OperatorPolicy(), cancellationToken: ct));

            var cleared = await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, routeId, clear_agent_level: true, served: OperatorPolicy(), cancellationToken: ct);
            Assert.Equal("updated", Status(cleared));
            var doc = JsonDocument.Parse(cleared).RootElement;
            Assert.Equal(2, doc.GetProperty("previous").GetProperty("agent_level").GetInt32());
            Assert.Equal(JsonValueKind.Null, doc.GetProperty("route").GetProperty("agent_level").ValueKind);

            Assert.Equal("null|7", await ScalarAsync(owner,
                $"SELECT coalesce(agent_level::text, 'null') || '|' || agent_max_per_hour::text FROM config.config_notification_routes WHERE route_id = {routeId}", ct));
            Assert.Equal("1", await ScalarAsync(owner,
                $"SELECT count(*) FROM config.investigation_journal WHERE route_id = {routeId} AND event = 'route_agent_changed' " +
                $"AND actor = 'operator:{Operator}' AND reason = 'changed agent_level' AND (detail -> 'before' ->> 'agent_level') = '2' " +
                "AND detail -> 'after' -> 'agent_level' = 'null'::jsonb", ct));

            /* The undo of the clear puts level 2 back. */
            Assert.Equal(2, doc.GetProperty("undo").GetProperty("arguments").GetProperty("agent_level").GetInt32());
        });
    }

    /// <summary>L5 of the #6593 review: the undo of an operator token's change to the agent fields puts the level and the four limits back.</summary>
    [Fact]
    public async Task TheUndo_OfAnAgentFieldChange_RestoresTheLevelAndTheLimits()
    {
        await WithMcpRoleAsync(async (owner, asMcp, ct) =>
        {
            var routeId = RouteIdOf(await DarlingMcpAlertTools.CreateNotificationRoute(
                asMcp, "Blocking", agent_level: 1, agent_max_per_hour: 5, agent_max_per_day: 40, agent_lease_minutes: 20, agent_expiry_minutes: 60,
                served: OperatorPolicy(), cancellationToken: ct));
            const string Fields = "SELECT coalesce(agent_level::text, 'null') || '|' || agent_max_per_hour::text || '|' || agent_max_per_day::text || '|' " +
                "|| agent_lease_minutes::text || '|' || agent_expiry_minutes::text FROM config.config_notification_routes WHERE route_id = ";
            var original = await ScalarAsync(owner, Fields + routeId, ct);
            Assert.Equal("1|5|40|20|60", original);

            var changed = await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, routeId, agent_level: 3, agent_max_per_hour: 30, agent_max_per_day: 200, agent_lease_minutes: 60, agent_expiry_minutes: 600,
                served: OperatorPolicy(), cancellationToken: ct);
            Assert.Equal("updated", Status(changed));
            Assert.Equal("3|30|200|60|600", await ScalarAsync(owner, Fields + routeId, ct));

            Assert.Equal("updated", Status(await RunUndoAsync(asMcp, changed, ct)));
            Assert.Equal(original, await ScalarAsync(owner, Fields + routeId, ct));

            /* set_notification_route_enabled hands an operator token's change to an agent route to update_notification_route, so its answer's
               previous and undo carry the agent fields too, and the undo puts the route back on with them unchanged. */
            var off = await DarlingMcpAlertTools.SetNotificationRouteEnabled(asMcp, routeId, false, served: OperatorPolicy(), cancellationToken: ct);
            Assert.Equal("updated", Status(off));
            var offDoc = JsonDocument.Parse(off).RootElement;
            Assert.Equal(1, offDoc.GetProperty("previous").GetProperty("agent_level").GetInt32());
            Assert.Equal(1, offDoc.GetProperty("undo").GetProperty("arguments").GetProperty("agent_level").GetInt32());
            Assert.Equal("updated", Status(await RunUndoAsync(asMcp, off, ct)));
            Assert.Equal("true", await ScalarAsync(owner, $"SELECT enabled::text FROM config.config_notification_routes WHERE route_id = {routeId}", ct));
            Assert.Equal(original, await ScalarAsync(owner, Fields + routeId, ct));
        });
    }

    /// <summary>L5 of the #6593 review: the undo of a change made from Inherit carries clear_agent_level true, and running it puts the level back to NULL.</summary>
    [Fact]
    public async Task TheUndo_OfAChangeMadeFromInherit_PutsInheritBack()
    {
        await WithMcpRoleAsync(async (owner, asMcp, ct) =>
        {
            var routeId = RouteIdOf(await DarlingMcpAlertTools.CreateNotificationRoute(
                asMcp, "Blocking", served: OperatorPolicy(), cancellationToken: ct));

            var changed = await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, routeId, agent_level: 2, served: OperatorPolicy(), cancellationToken: ct);
            Assert.Equal("updated", Status(changed));
            var undo = JsonDocument.Parse(changed).RootElement.GetProperty("undo").GetProperty("arguments");
            Assert.True(undo.GetProperty("clear_agent_level").GetBoolean());
            Assert.False(undo.TryGetProperty("agent_level", out _));
            Assert.Equal("2", await ScalarAsync(owner, $"SELECT agent_level::text FROM config.config_notification_routes WHERE route_id = {routeId}", ct));

            Assert.Equal("updated", Status(await RunUndoAsync(asMcp, changed, ct)));
            Assert.Equal("null", await ScalarAsync(owner,
                $"SELECT coalesce(agent_level::text, 'null') FROM config.config_notification_routes WHERE route_id = {routeId}", ct));
        });
    }

    /// <summary>L5 of the #6593 review: agent_level and clear_agent_level together are invalid, and a caller that is not operator-class (an
    /// agent-class token, an investigations-only token) still cannot clear the level. Nothing changes.</summary>
    [Fact]
    public async Task ClearAgentLevel_WithAgentLevel_IsInvalid_AndANonOperatorTokenCannotClearIt()
    {
        await WithMcpRoleAsync(async (owner, asMcp, ct) =>
        {
            var routeId = RouteIdOf(await DarlingMcpAlertTools.CreateNotificationRoute(
                asMcp, "Blocking", agent_level: 2, served: OperatorPolicy(), cancellationToken: ct));

            Assert.Equal("invalid", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, routeId, agent_level: 1, clear_agent_level: true, served: OperatorPolicy(), cancellationToken: ct)));

            var agentToken = new DarlingMcpServedPolicy(new DarlingMcpWritePolicy.Effective(false, false, Investigations: true, TokenName: "triage-bot"));
            var investigations = new DarlingMcpServedPolicy(new DarlingMcpWritePolicy.Effective(false, false, Investigations: true));
            foreach (var policy in new[] { agentToken, investigations })
            {
                Assert.Equal("refused", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(
                    asMcp, routeId, clear_agent_level: true, served: policy, cancellationToken: ct)));
            }

            /* A route with no agent setting yet: the refusal for agent fields, not the agent-route one, still holds. */
            var plainId = RouteIdOf(await DarlingMcpAlertTools.CreateNotificationRoute(
                asMcp, "Deadlocks", agent_level: 0, served: OperatorPolicy(), cancellationToken: ct));
            Assert.Equal("refused", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, plainId, clear_agent_level: true, served: agentToken, cancellationToken: ct)));

            Assert.Equal("2", await ScalarAsync(owner, $"SELECT agent_level::text FROM config.config_notification_routes WHERE route_id = {routeId}", ct));
            Assert.Equal("0", await ScalarAsync(owner, $"SELECT agent_level::text FROM config.config_notification_routes WHERE route_id = {plainId}", ct));
            Assert.Equal("0", await ScalarAsync(owner,
                $"SELECT count(*) FROM config.investigation_journal WHERE route_id IN ({routeId}, {plainId}) AND reason = 'changed agent_level'", ct));
        });
    }

    private const string Approval = "SELECT action_approval || '|' || approval_single_person::text FROM config.config_notification_routes WHERE route_id = ";

    private static string Message(string answer) => JsonDocument.Parse(answer).RootElement.GetProperty("message").GetString()!;

    /// <summary>#7109: a route created over MCP gets the column defaults (approval required, no single-person approval), so MCP cannot create a
    /// Level 3 route whose actions run without approval: create_notification_route takes neither column.</summary>
    [Fact]
    public async Task ARouteCreatedOverMcp_AtLevel3_GetsApprovalRequired()
    {
        await WithMcpRoleAsync(async (owner, asMcp, ct) =>
        {
            var created = await DarlingMcpAlertTools.CreateNotificationRoute(asMcp, "Deadlocks", agent_level: 3, served: OperatorPolicy(), cancellationToken: ct);
            Assert.Equal("created", Status(created));
            var routeId = RouteIdOf(created);
            Assert.Equal("required|false", await ScalarAsync(owner, Approval + routeId, ct));
            var route = JsonDocument.Parse(created).RootElement.GetProperty("route");
            Assert.Equal("required", route.GetProperty("action_approval").GetString());
            Assert.False(route.GetProperty("approval_single_person").GetBoolean());
            Assert.Equal("required|false", await ScalarAsync(owner,
                $"SELECT (detail -> 'after' ->> 'action_approval') || '|' || (detail -> 'after' ->> 'approval_single_person') FROM config.investigation_journal " +
                $"WHERE route_id = {routeId} AND event = 'route_agent_changed'", ct));
        });
    }

    /// <summary>#7109: MCP turns approval on but never off. action_approval goes to required and approval_single_person to false, each journaled with
    /// its before and after; off and true are refused by the tool and by the store function. The undo leaves both columns out and says a person
    /// undoes them in the Viewer or on the web dashboard.</summary>
    [Fact]
    public async Task ApprovalGoesOnlyToRequired_SinglePersonOnlyToFalse_EachJournaled_AndTheUndoLeavesThemOut()
    {
        await WithMcpRoleAsync(async (owner, asMcp, ct) =>
        {
            var routeId = RouteIdOf(await DarlingMcpAlertTools.CreateNotificationRoute(asMcp, "Blocking", agent_level: 2, served: OperatorPolicy(), cancellationToken: ct));
            await ExecAsync(owner, $"UPDATE config.config_notification_routes SET action_approval = 'off', approval_single_person = true WHERE route_id = {routeId}", ct);

            var on = await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, routeId, action_approval: "required", approval_single_person: false, served: OperatorPolicy(), cancellationToken: ct);
            Assert.Equal("updated", Status(on));
            Assert.Equal("required|false", await ScalarAsync(owner, Approval + routeId, ct));
            Assert.Equal("1", await ScalarAsync(owner,
                $"SELECT count(*) FROM config.investigation_journal WHERE route_id = {routeId} AND event = 'route_agent_changed' " +
                $"AND actor = 'operator:{Operator}' AND reason = 'changed action_approval, approval_single_person' " +
                "AND (detail -> 'before' ->> 'action_approval') = 'off' AND (detail -> 'after' ->> 'action_approval') = 'required' " +
                "AND (detail -> 'before' ->> 'approval_single_person') = 'true' AND (detail -> 'after' ->> 'approval_single_person') = 'false'", ct));

            var undo = JsonDocument.Parse(on).RootElement.GetProperty("undo");
            Assert.False(undo.GetProperty("arguments").TryGetProperty("action_approval", out _));
            Assert.False(undo.GetProperty("arguments").TryGetProperty("approval_single_person", out _));
            Assert.Contains("Viewer", undo.GetProperty("approval").GetString(), StringComparison.Ordinal);

            var off = await DarlingMcpAlertTools.UpdateNotificationRoute(asMcp, routeId, action_approval: "off", served: OperatorPolicy(), cancellationToken: ct);
            Assert.Equal("refused", Status(off));
            Assert.Contains("never off", Message(off), StringComparison.Ordinal);
            var single = await DarlingMcpAlertTools.UpdateNotificationRoute(asMcp, routeId, approval_single_person: true, served: OperatorPolicy(), cancellationToken: ct);
            Assert.Equal("refused", Status(single));
            Assert.Contains("web dashboard", Message(single), StringComparison.Ordinal);

            /* The store function holds the same rule under the row's lock, for any caller of the mcp login. */
            foreach (var call in new[]
            {
                $"SELECT config.set_notification_route_agent({routeId}, 'operator:{Operator}', ARRAY['action_approval'], NULL, NULL, NULL, NULL, NULL, NULL, false, NULL, 'off', NULL)",
                $"SELECT config.set_notification_route_agent({routeId}, 'operator:{Operator}', ARRAY['approval_single_person'], NULL, NULL, NULL, NULL, NULL, NULL, false, NULL, NULL, true)",
            })
            {
                var refused = await Assert.ThrowsAsync<PostgresException>(async () =>
                {
                    await using var command = asMcp.CreateCommand(call);
                    await command.ExecuteNonQueryAsync(ct);
                });
                Assert.Equal("DL403", refused.SqlState);
            }

            Assert.Equal("required|false", await ScalarAsync(owner, Approval + routeId, ct));
        });
    }

    /// <summary>#7109: a Level 3 route with action approval required cannot be disabled (by either tool), put back to Inherit, or have its match,
    /// scope or severities changed over MCP, so no alert falls through to a route whose actions run without approval. Its level can go down,
    /// and once it is below Level 3 it can be disabled.</summary>
    [Fact]
    public async Task ALevel3RouteWithApprovalRequired_CannotBeDisabledClearedOrReshaped_ButCanBeLowered()
    {
        await WithMcpRoleAsync(async (owner, asMcp, ct) =>
        {
            var routeId = RouteIdOf(await DarlingMcpAlertTools.CreateNotificationRoute(asMcp, "Deadlocks", agent_level: 3, served: OperatorPolicy(), cancellationToken: ct));
            const string Shape = "SELECT enabled::text || '|' || coalesce(agent_level::text, 'null') || '|' || metric_match || '|' || severities || '|' || coalesce(scope_server_id::text, 'fleet') " +
                "FROM config.config_notification_routes WHERE route_id = ";
            Assert.Equal("true|3|Deadlocks|both|fleet", await ScalarAsync(owner, Shape + routeId, ct));

            var refusals = new[]
            {
                await DarlingMcpAlertTools.UpdateNotificationRoute(asMcp, routeId, enabled: false, served: OperatorPolicy(), cancellationToken: ct),
                await DarlingMcpAlertTools.SetNotificationRouteEnabled(asMcp, routeId, false, served: OperatorPolicy(), cancellationToken: ct),
                await DarlingMcpAlertTools.UpdateNotificationRoute(asMcp, routeId, clear_agent_level: true, served: OperatorPolicy(), cancellationToken: ct),
                await DarlingMcpAlertTools.UpdateNotificationRoute(asMcp, routeId, metric_match: "Blocking", served: OperatorPolicy(), cancellationToken: ct),
                await DarlingMcpAlertTools.UpdateNotificationRoute(asMcp, routeId, severities: "critical", served: OperatorPolicy(), cancellationToken: ct),
                await DarlingMcpAlertTools.UpdateNotificationRoute(asMcp, routeId, scope_server_id: 999, served: OperatorPolicy(), cancellationToken: ct),
            };
            foreach (var answer in refusals)
            {
                Assert.Equal("refused", Status(answer));
                Assert.Contains("action approval required", Message(answer), StringComparison.Ordinal);
                Assert.Contains("Viewer", Message(answer), StringComparison.Ordinal);
            }

            Assert.Equal("true|3|Deadlocks|both|fleet", await ScalarAsync(owner, Shape + routeId, ct));

            Assert.Equal("updated", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(asMcp, routeId, agent_level: 2, served: OperatorPolicy(), cancellationToken: ct)));
            Assert.Equal("updated", Status(await DarlingMcpAlertTools.SetNotificationRouteEnabled(asMcp, routeId, false, served: OperatorPolicy(), cancellationToken: ct)));
            Assert.Equal("false|2|Deadlocks|both|fleet", await ScalarAsync(owner, Shape + routeId, ct));
        });
    }

    /// <summary>#7109: a route with action approval off cannot be raised to Level 3 over MCP (unless the same call sets required), and at Level 3
    /// it cannot be enabled or have its match or severities changed, so it never takes an alert from a route whose actions wait for a person.</summary>
    [Fact]
    public async Task ARouteWithApprovalOff_CannotBeRaisedToLevel3_NorAtLevel3_EnabledOrWidened()
    {
        await WithMcpRoleAsync(async (owner, asMcp, ct) =>
        {
            var lowId = RouteIdOf(await DarlingMcpAlertTools.CreateNotificationRoute(asMcp, "Blocking", agent_level: 2, served: OperatorPolicy(), cancellationToken: ct));
            var highId = RouteIdOf(await DarlingMcpAlertTools.CreateNotificationRoute(asMcp, "Deadlocks", agent_level: 3, served: OperatorPolicy(), cancellationToken: ct));
            await ExecAsync(owner, $"UPDATE config.config_notification_routes SET action_approval = 'off' WHERE route_id IN ({lowId}, {highId})", ct);
            await ExecAsync(owner, $"UPDATE config.config_notification_routes SET enabled = false WHERE route_id = {highId}", ct);

            var raise = await DarlingMcpAlertTools.UpdateNotificationRoute(asMcp, lowId, agent_level: 3, served: OperatorPolicy(), cancellationToken: ct);
            Assert.Equal("refused", Status(raise));
            Assert.Contains("raise it to Level 3", Message(raise), StringComparison.Ordinal);
            Assert.Equal("2|off", await ScalarAsync(owner, $"SELECT agent_level::text || '|' || action_approval FROM config.config_notification_routes WHERE route_id = {lowId}", ct));

            foreach (var answer in new[]
            {
                await DarlingMcpAlertTools.UpdateNotificationRoute(asMcp, highId, enabled: true, served: OperatorPolicy(), cancellationToken: ct),
                await DarlingMcpAlertTools.SetNotificationRouteEnabled(asMcp, highId, true, served: OperatorPolicy(), cancellationToken: ct),
                await DarlingMcpAlertTools.UpdateNotificationRoute(asMcp, highId, metric_match: "performance", served: OperatorPolicy(), cancellationToken: ct),
                await DarlingMcpAlertTools.UpdateNotificationRoute(asMcp, highId, severities: "warning", served: OperatorPolicy(), cancellationToken: ct),
            })
            {
                Assert.Equal("refused", Status(answer));
                Assert.Contains("action approval off", Message(answer), StringComparison.Ordinal);
                Assert.Contains("web dashboard", Message(answer), StringComparison.Ordinal);
            }

            Assert.Equal("false|Deadlocks|both", await ScalarAsync(owner,
                $"SELECT enabled::text || '|' || metric_match || '|' || severities FROM config.config_notification_routes WHERE route_id = {highId}", ct));

            /* Raising together with required, and turning required on first, both go through. */
            Assert.Equal("updated", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(
                asMcp, lowId, agent_level: 3, action_approval: "required", served: OperatorPolicy(), cancellationToken: ct)));
            Assert.Equal("3|required", await ScalarAsync(owner, $"SELECT agent_level::text || '|' || action_approval FROM config.config_notification_routes WHERE route_id = {lowId}", ct));
            Assert.Equal("updated", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(asMcp, highId, action_approval: "required", served: OperatorPolicy(), cancellationToken: ct)));
            Assert.Equal("updated", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(asMcp, highId, enabled: true, served: OperatorPolicy(), cancellationToken: ct)));
        });
    }

    /// <summary>#7109: lowering a Level 3 route with approval required to Level 2 goes through, but while another enabled Level 3 route has approval
    /// off, the second call that would disable, clear or reshape it is refused by both tools and names that route, because the metric could fall
    /// through to it. With no such route the same call goes through.</summary>
    [Fact]
    public async Task LoweringThenDisabling_IsRefusedAtTheSecondCall_WhileALevel3RouteWithApprovalOffIsEnabled()
    {
        await WithMcpRoleAsync(async (owner, asMcp, ct) =>
        {
            var routeId = RouteIdOf(await DarlingMcpAlertTools.CreateNotificationRoute(asMcp, "Deadlocks", agent_level: 3, served: OperatorPolicy(), cancellationToken: ct));
            var offId = RouteIdOf(await DarlingMcpAlertTools.CreateNotificationRoute(asMcp, "Blocking", agent_level: 3, served: OperatorPolicy(), cancellationToken: ct));
            await ExecAsync(owner, $"UPDATE config.config_notification_routes SET action_approval = 'off' WHERE route_id = {offId}", ct);

            Assert.Equal("updated", Status(await DarlingMcpAlertTools.UpdateNotificationRoute(asMcp, routeId, agent_level: 2, served: OperatorPolicy(), cancellationToken: ct)));
            foreach (var answer in new[]
            {
                await DarlingMcpAlertTools.SetNotificationRouteEnabled(asMcp, routeId, false, served: OperatorPolicy(), cancellationToken: ct),
                await DarlingMcpAlertTools.UpdateNotificationRoute(asMcp, routeId, enabled: false, served: OperatorPolicy(), cancellationToken: ct),
                await DarlingMcpAlertTools.UpdateNotificationRoute(asMcp, routeId, clear_agent_level: true, served: OperatorPolicy(), cancellationToken: ct),
                await DarlingMcpAlertTools.UpdateNotificationRoute(asMcp, routeId, metric_match: "Blocking", served: OperatorPolicy(), cancellationToken: ct),
                await DarlingMcpAlertTools.UpdateNotificationRoute(asMcp, routeId, severities: "critical", served: OperatorPolicy(), cancellationToken: ct),
                await DarlingMcpAlertTools.UpdateNotificationRoute(asMcp, routeId, scope_server_id: 999, served: OperatorPolicy(), cancellationToken: ct),
            })
            {
                Assert.Equal("refused", Status(answer));
                Assert.Contains($"route {offId} is an enabled Level 3 route with action approval off", Message(answer), StringComparison.Ordinal);
                Assert.Contains("Viewer", Message(answer), StringComparison.Ordinal);
            }

            Assert.Equal("true|2|Deadlocks|both", await ScalarAsync(owner,
                $"SELECT enabled::text || '|' || agent_level::text || '|' || metric_match || '|' || severities FROM config.config_notification_routes WHERE route_id = {routeId}", ct));

            /* Once a person turns the other route's approval on, nothing can fall through to an unapproved route, and the disable goes through. */
            await ExecAsync(owner, $"UPDATE config.config_notification_routes SET action_approval = 'required' WHERE route_id = {offId}", ct);
            Assert.Equal("updated", Status(await DarlingMcpAlertTools.SetNotificationRouteEnabled(asMcp, routeId, false, served: OperatorPolicy(), cancellationToken: ct)));
        });
    }

    /// <summary>Runs an answer's undo, an update_notification_route call, as the operator token, with the arguments it carries.</summary>
    private static Task<string> RunUndoAsync(NpgsqlDataSource asMcp, string answer, CancellationToken ct)
    {
        var undo = JsonDocument.Parse(answer).RootElement.GetProperty("undo");
        Assert.Equal("update_notification_route", undo.GetProperty("tool").GetString());
        var a = undo.GetProperty("arguments");
        int? Int(string name) => a.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.Number ? v.GetInt32() : null;
        bool? Bool(string name) => a.TryGetProperty(name, out var v) && v.ValueKind is JsonValueKind.True or JsonValueKind.False ? v.GetBoolean() : null;
        string? Text(string name) => a.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.String ? v.GetString() : null;
        return DarlingMcpAlertTools.UpdateNotificationRoute(
            asMcp, a.GetProperty("route_id").GetInt32(), metric_match: Text("metric_match"), enabled: Bool("enabled"),
            scope_server_id: Int("scope_server_id"), scope_tag_id: Int("scope_tag_id"), fleet_wide: Bool("fleet_wide"),
            send_resolutions: Text("send_resolutions"), fallback_route_id: Int("fallback_route_id"), clear_fallback: Bool("clear_fallback"),
            email_recipients: Text("email_recipients"), agent_level: Int("agent_level"), agent_max_per_hour: Int("agent_max_per_hour"),
            agent_max_per_day: Int("agent_max_per_day"), agent_lease_minutes: Int("agent_lease_minutes"), agent_expiry_minutes: Int("agent_expiry_minutes"),
            clear_agent_level: Bool("clear_agent_level"), served: OperatorPolicy(), cancellationToken: ct);
    }

    private static async Task WithMcpRoleAsync(Func<NpgsqlDataSource, NpgsqlDataSource, CancellationToken, Task> body)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the route agent field live tests.");

        var ct = TestContext.Current.CancellationToken;
        var roleName = "route_agent_" + Guid.NewGuid().ToString("N")[..8];
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        await using var owner = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            /* The functions and the journal trigger are created by the owner, as the managed batch does; the batch's own grants, retargeted. */
            await ExecAsync(owner, DarlingManagedRoles.BuildNotificationRouteFunctionsSql("config"), ct);
            await ExecAsync(owner, DarlingManagedRoles.BuildJournalEventRulesSql("config"), ct);
            await ExecAsync(owner, $"CREATE ROLE {roleName} LOGIN NOSUPERUSER PASSWORD '{RolePassword}'", ct);
            foreach (var statement in ViewerGrantReplay.StatementsFor(roleName, scratch.DatabaseName, "mcp"))
            {
                await ExecAsync(owner, statement, ct);
            }

            var executes = ViewerGrantReplay.BatchStatements().Where(s =>
                s.StartsWith("GRANT EXECUTE ON FUNCTION config.create_notification_route", StringComparison.Ordinal)
                || s.StartsWith("GRANT EXECUTE ON FUNCTION config.update_notification_route", StringComparison.Ordinal)
                || s.StartsWith("GRANT EXECUTE ON FUNCTION config.set_notification_route_agent", StringComparison.Ordinal)).ToList();
            Assert.Equal(3, executes.Count);
            foreach (var execute in executes)
            {
                var at = execute.LastIndexOf(" TO ", StringComparison.Ordinal);
                Assert.Equal("mcp", execute[(at + 4)..].Trim());
                await ExecAsync(owner, execute[..at] + " TO " + roleName, ct);
            }

            await using var asMcp = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                Username = roleName,
                Password = RolePassword,
                Pooling = false,
            }.ConnectionString);

            await body(owner, asMcp, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, () => ExecAsync(owner, $"DROP OWNED BY {roleName}; DROP ROLE IF EXISTS {roleName};", CancellationToken.None));
        }
    }

    private static async Task ExecAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(ct);
    }
}
