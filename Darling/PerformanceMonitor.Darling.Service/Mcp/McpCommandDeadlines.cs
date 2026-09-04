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
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The explicit command deadlines for this project's MCP and web READ surface (#2874) — every command
/// under <c>Mcp/</c>, the shared <see cref="CustomViewStore"/> both surfaces persist through, and the
/// composed-query runner in <see cref="DarlingWebEndpoints"/>.
///
/// <para>Before this, all 125 of those commands set no <c>CommandTimeout</c> and inherited Npgsql's
/// undocumented 30 s default — a value nobody chose, and the defect class behind three production
/// failures (#2810, #2871, #2796): exceeding the ceiling surfaces as
/// <c>Exception while reading from stream</c>, which reads as a network fault rather than a deadline.</para>
///
/// <para><b>The enclosing constraint here is a kind this sweep had not met.</b> Not a clock
/// (<c>CancelAfter</c>, #2871) and not a connection permit (<c>MaxPoolSize</c>, #2901) but a SERVER-SIDE
/// <c>statement_timeout</c> on the login role — <c>ALTER ROLE mcp/viewer SET statement_timeout</c>, written
/// by startup provisioning from <c>config.config_service.compose_statement_timeout_seconds</c>, shipped
/// default 15 s, clamped 5–600 by
/// <see cref="StoreConfigProvider.ClampComposeStatementTimeoutSeconds"/>. It is a ROLE setting, so it
/// bounds every statement those identities run, not merely the composed queries it is named for —
/// <see cref="DarlingTrendReader.QueryStoreDurationTrendSql"/> and
/// <see cref="QueryStoreTrendRouting"/> both already reason about it by name.</para>
///
/// <para><b>Why a client-side deadline does not simply defer to that GUC.</b> Three reasons, and they land
/// differently per deployment shape rather than being one argument repeated:</para>
///
/// <para>1. <b>Bring-your-own PostgreSQL has no such GUC at all.</b> The roles are created out-of-band by
/// <c>Darling/tools/provision-roles.sql</c>, which provisions <c>admin</c> and <c>viewer</c> and
/// deliberately NO <c>mcp</c> role, and sets <c>statement_timeout</c> on <c>viewer</c> only. The MCP host in
/// that mode connects with the operator's own <c>postgres.connectionString</c>
/// (<see cref="DarlingMcpHostService"/>), which is typically the owner or <c>admin</c> identity and carries
/// no <c>statement_timeout</c>. So on a BYO store this read surface has NO server-side bound, and Npgsql's
/// undocumented default is currently the only thing bounding it. Remove that inheritance without replacing
/// it and the reads become unbounded. A BYO operator also has no knob here — the store column reaches the
/// roles only through managed provisioning — which is why these values are deliberately not aggressive.</para>
///
/// <para>2. <b>On a managed store the GUC is operator-tunable to 600 s, and it is per-ROLE.</b> An operator
/// who raises <c>compose_statement_timeout_seconds</c> so one heavy custom view can finish raises it for
/// every statement the <c>mcp</c> role runs — all 124 shipped reads included. The client-side deadline is
/// what keeps the ORDINARY read bounded while the backstop is deliberately loosened for the extraordinary
/// one.</para>
///
/// <para>3. <b>A role <c>SET</c> takes effect on that role's NEXT session</b>, which
/// <see cref="DarlingConfig.ComposeStatementTimeoutSeconds"/> states outright: "This is not a kill switch."
/// The MCP host builds its pool once at host start and
/// <see cref="DarlingManagedRoles.ReassertComposeStatementTimeoutAsync"/> logs that an already-connected
/// client keeps the old ceiling until it reconnects. A <c>CommandTimeout</c> is set per command at
/// construction, so it binds the very next command on an ALREADY-OPEN pooled connection, which is exactly
/// what the GUC cannot do.</para>
///
/// <para><b>The asymmetry that makes a client-side deadline necessary rather than merely tidy.</b> None of
/// the 136 <c>[McpServerTool]</c> methods takes a <c>CancellationToken</c> — every reader declares
/// <c>CancellationToken cancellationToken = default</c> and every tool calls it without one, and the one
/// tool that names a token passes <c>CancellationToken.None</c> explicitly
/// (<see cref="DarlingMcpCustomViewTools"/> into
/// <see cref="DarlingWebEndpoints.RunComposedPanelAsync"/>). So an agent that gives up on a tool call leaves
/// the query running AND the pooled connection held; nothing on this path propagates abandonment. The web
/// half is the mirror image and threads <c>HttpContext.RequestAborted</c> throughout, so a disconnecting
/// browser does cancel — which is why the deadline here is a backstop on that surface and the ONLY bound on
/// the MCP one.</para>
///
/// <para><b>The permit, stated at its real strength.</b> The MCP host builds its OWN data source
/// (<see cref="DarlingMcpHostService"/>), separate from the web host's and from the worker's collection
/// pool, each an independent <c>NpgsqlDataSource.Create</c> on a connection string with a different
/// <c>Username</c> — so a stalled MCP read cannot starve fleet-wide collection through the pool, and the
/// permit argument does NOT reach as far here as it did in the viewer (#2901), where one control awaited ten
/// concurrent reads against <c>MaxPoolSize = 10</c>. The widest fan-out on this surface is a
/// <c>Task.WhenAll</c> over TWO reads, against <c>MaxPoolSize = 24</c>. What the permit still buys is the
/// second-order cost the pool bound exists for: every pooled connection is a live <c>postgres.exe</c>
/// PROCESS, each spawn must re-reserve the shared memory region, and the store logged 8
/// "could not reserve shared memory region" retries on 2026-09-03 — so a longer hold means more spawns, not
/// merely a busier pool.</para>
///
/// <para><b>Two regimes, not one.</b> The 124 shipped reads take a fixed constant; the composed-query runner
/// cannot, because the operator's knob IS the declared bound for that query class and a fixed constant under
/// it would silently override a deliberate choice. Splitting them is this issue's own rule — a wrong bound is
/// worse than no bound.</para>
/// </summary>
internal static class McpCommandDeadlines
{
    /// <summary>
    /// The deadline for the 124 shipped-shape commands: everything under <c>Mcp/</c> plus
    /// <see cref="CustomViewStore"/>. Bounded on both sides, and each side comes from a different
    /// constraint rather than one argument restated.
    ///
    /// <para><b>"Read" names the SURFACE, not the verb, and six of the 124 are writes</b> — the
    /// custom-view insert/update/delete in <see cref="CustomViewStore"/>, the
    /// <c>config_alert_settings</c> update behind the alert-tuning tool, and the server-registry insert and
    /// delete behind <c>add_servers</c>/<c>remove_server</c>. They share this bound rather than getting
    /// their own because they share the REGIME exactly, which is what this sweep groups by: the same
    /// least-privilege pool, the same absent <c>CancellationToken</c>, the same role
    /// <c>statement_timeout</c>, and no enclosing budget. They are also strictly cheaper than the reads the
    /// bound was floored on — every one is a single-row statement on a <c>config</c> table keyed by primary
    /// key — so a ceiling derived from the heaviest read is comfortable for all of them. The name follows
    /// <see cref="StorageCommandDeadlines.McpReadSeconds"/> and the viewer's
    /// <c>InteractiveReadSeconds</c>, both of which cover their surface's writes for the same reason.</para>
    ///
    /// <para>ABOVE the managed store's shipped server-side ceiling, deliberately. The <c>mcp</c> role's
    /// default <c>statement_timeout</c> is 15 s and it fires on this surface in production TODAY: on
    /// 2026-09-04 <c>get_query_store_duration_trend</c> over a 7-day window returned
    /// <c>57014: canceling statement due to statement timeout</c> against the live us-east-1 store — with
    /// #2736's corrected-rollup routing already engaged — while the same read at the shipped 24 h default
    /// returned its full series. Sitting above 15 s is what keeps the server-side timeout the one that
    /// fires on a managed store, because <c>57014</c> NAMES the cause where Npgsql's own deadline renders
    /// as <c>Exception while reading from stream</c> and gets misdiagnosed as a network fault — the exact
    /// confusion #2826 exists to prevent. A client deadline at or under 15 s would start pre-empting a
    /// bound that already works and already reports well.</para>
    ///
    /// <para>BELOW the 30 s it replaces, and below the sibling half of the same surface. The viewer's pin
    /// set the standard that a deadline must be MEANINGFULLY under the inherited default rather than equal
    /// to it, and <see cref="StorageCommandDeadlines.McpReadSeconds"/> — the <c>DarlingPg*Reader</c> family
    /// serving these same 136 tools from <c>.Storage</c> — is 30, which is numerically the Npgsql default
    /// and was derived (#2888) before the role GUC was part of this sweep's reasoning. This surface's half
    /// must not be the looser of the two, so the ceiling is that constant, asserted relationally in
    /// <c>McpReadCommandTimeoutTests</c> rather than by copying its number.</para>
    /// </summary>
    internal const int ReadSeconds = 20;

    /// <summary>
    /// The composed-query deadline when the operator's configured value cannot be read — 15 s, which is the
    /// shipped default in three other places (<see cref="DarlingConfig.ComposeStatementTimeoutSeconds"/>,
    /// <c>ComposeSpec.StatementTimeout</c>, and the <c>value &lt;= 0</c> arm of
    /// <see cref="StoreConfigProvider.ClampComposeStatementTimeoutSeconds"/>), so a store that cannot answer
    /// lands on the same number a store that has never been tuned would.
    ///
    /// <para><b>Defensive on purpose, mirroring <c>DarlingManagedRoles</c>' own read of this column.</b> The
    /// row may not be seeded yet, the store may predate the V78 column, and the read runs on a
    /// least-privilege pool — none of which should turn a tuning knob into a failed panel. Failing OPEN to a
    /// larger value would be the wrong direction: it would grant more time precisely when the store is least
    /// able to answer for itself.</para>
    /// </summary>
    internal const int ComposedQueryFallbackSeconds = 15;

    /// <summary>
    /// <para>The deadline for the composed-query runner — <c>RunComposedQueryAsync</c>, reached from both the
    /// web <c>/api/compose/run</c> endpoint and the MCP <c>run_custom_view_panel</c> tool through the one
    /// shared <see cref="DarlingWebEndpoints.RunComposedPanelAsync"/>.</para>
    ///
    /// <para><b>Why this one cannot take <see cref="ReadSeconds"/>.</b> Every other command on this surface
    /// is a shipped query of fixed shape. This one runs OPERATOR-AUTHORED SQL compiled from a stored panel
    /// spec, and <c>config.config_service.compose_statement_timeout_seconds</c> exists precisely to bound it
    /// — that column is the operator's declaration of how long this query class may run. A fixed 20 s here
    /// would cut off the panel an operator raised the knob to 120 s for, which is a regression dressed as a
    /// fix.</para>
    ///
    /// <para><b>Why it is READ rather than declared, and read HERE rather than plumbed from the host.</b>
    /// The store is authoritative for this knob and #2918 made it hot-swappable — a control-plane reload
    /// re-asserts it onto the roles without a restart. So a value captured at host start is simply wrong
    /// after the first change, and the value a host COULD plumb is the file-loaded
    /// <see cref="DarlingConfig.ComposeStatementTimeoutSeconds"/>, which is only the seed: the store wins
    /// afterwards. Reading the store per panel run has no cache to go stale, and it is also what lets the two
    /// callers stay identical — <see cref="DarlingWebEndpoints.MapAll"/> has the host's config in scope and
    /// the MCP tool has only the data source, so plumbing would have given one surface the file value and the
    /// other a store read, which is exactly the divergence the single shared runner exists to prevent.</para>
    ///
    /// <para><b>The near miss worth naming</b> is three lines away in the same method:
    /// <c>ComposeStoreAvailability.GetRollupsAsync</c> is deliberately <i>cached per data source</i>, because
    /// which rollups a store HAS changes only when someone migrates it. Caching this the same way would
    /// compile, pass any pin that only checks the value's band, and silently pin the deadline to whatever the
    /// knob said the first time a panel ran. It is resolved once per RUN — not per query, so a panel with
    /// annotation overlays pays one read rather than one per source, and not once per process.</para>
    ///
    /// <para>Clamped through the store's own
    /// <see cref="StoreConfigProvider.ClampComposeStatementTimeoutSeconds"/> rather than trusted, so a
    /// hand-edited row cannot widen this deadline any more than it can widen the role GUC; the clamp also
    /// maps a null or non-positive value onto <see cref="ComposedQueryFallbackSeconds"/>' 15.</para>
    /// </summary>
    internal static async Task<int> ResolveComposedQuerySecondsAsync(
        NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        try
        {
            await using var command = postgres.CreateCommand(ComposeDeadlineSql);
            command.CommandTimeout = ReadSeconds;
            var value = await command.ExecuteScalarAsync(cancellationToken);

            return StoreConfigProvider.ClampComposeStatementTimeoutSeconds(value is int seconds ? seconds : 0);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return ComposedQueryFallbackSeconds;
        }
    }

    /// <summary>
    /// Schema-QUALIFIED, like <c>DarlingManagedRoles</c>' read of the same column and unlike most reads on
    /// this surface: the least-privilege pools carry a <c>collect,config</c> search path, but this one read
    /// also runs on paths where that cannot be assumed, and <c>config_service</c> is not a name worth
    /// resolving by luck.
    /// </summary>
    private const string ComposeDeadlineSql =
        "SELECT compose_statement_timeout_seconds FROM config.config_service WHERE id = 1";
}
