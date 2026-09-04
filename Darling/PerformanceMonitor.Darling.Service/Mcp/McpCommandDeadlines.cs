/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

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
    /// The read deadline for the 124 shipped-shape commands: everything under <c>Mcp/</c> plus
    /// <see cref="CustomViewStore"/>. Bounded on both sides, and each side comes from a different
    /// constraint rather than one argument restated.
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
    /// The deadline for the composed-query runner —
    /// <see cref="!:DarlingWebEndpoints.RunComposedQueryAsync"/>, reached from both the web
    /// <c>/api/compose/run</c> endpoint and the MCP <c>run_custom_view_panel</c> tool through the one shared
    /// <see cref="DarlingWebEndpoints.RunComposedPanelAsync"/>.
    ///
    /// <para><b>Why this one cannot take <see cref="ReadSeconds"/>.</b> Every other command on this surface
    /// is a shipped query of fixed shape. This one runs OPERATOR-AUTHORED SQL compiled from a stored panel
    /// spec, and <c>compose_statement_timeout_seconds</c> exists precisely to bound it — that column is the
    /// operator's declaration of how long this query class may run. A fixed 20 s here would cut off the
    /// panel an operator raised the knob to 120 s for, which is a regression dressed as a fix.</para>
    ///
    /// <para>So the value is the operator's own declared MAXIMUM,
    /// <see cref="StoreConfigProvider.MaxComposeStatementTimeoutSeconds"/>, taken by reference rather than
    /// copied. Because the clamp guarantees no configured value can exceed it, this deadline can never be
    /// the binding bound on a managed store — the role GUC always fires first, whatever it is set to — while
    /// on a BYO store, where no role GUC exists, it replaces "whatever Npgsql defaults to" with the
    /// product's own stated ceiling for this query class.</para>
    ///
    /// <para><b>It narrows rather than closes.</b> Ten minutes is a long time to hold one of 24 permits, and
    /// the honest fix is to thread the LIVE configured value down to the runner so the client deadline
    /// tracks the operator's actual setting instead of its upper clamp. That needs
    /// <see cref="DarlingWebEndpoints.MapAll"/>'s signature and the MCP tool's call to carry it, which is a
    /// wider change than removing an inherited default; it is filed rather than smuggled in here.</para>
    /// </summary>
    internal const int ComposedQuerySeconds = StoreConfigProvider.MaxComposeStatementTimeoutSeconds;
}
