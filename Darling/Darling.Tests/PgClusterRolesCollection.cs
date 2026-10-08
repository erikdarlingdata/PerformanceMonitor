/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using Xunit;

namespace Darling.Tests;

/// <summary>
/// The one place that runs the live classes whose effect is not confined to their own scratch database (#5602).
///
/// <para><c>ScratchPostgres</c> gives a test its own database, but a role, a role membership, a database-level
/// grant, an <c>ALTER ROLE ... SET</c> and an <c>ALTER SYSTEM</c> write live in the CLUSTER's catalogs and
/// <c>postgresql.auto.conf</c>. Two tests that wrote the same one of them at the same time made Postgres refuse
/// the second with "tuple concurrently updated" (#5560). The product provisions FIXED role names
/// (<c>admin</c>, <c>viewer</c>, <c>mcp</c>), so any test that runs the product's provisioning batch, or drops or
/// alters those roles, shares them with every other such test whatever scratch database it aims at, and a test
/// cannot rename them without a product change.</para>
///
/// <para>Tests whose roles carry a per-run name (a GUID suffix, or the scratch database's name) need nothing:
/// nobody else can see the catalog row they write. This collection is for the rest. It carries
/// <c>DisableParallelization</c>, so xUnit runs it alone, after every parallel collection has finished: nothing
/// else touches the cluster while a member runs. That is stronger than <c>live-postgres</c>, whose members only
/// take turns with each other and still overlap every class outside the collection.</para>
///
/// <para>It deliberately has no collection fixture: its members build their own scratch database (or change
/// only cluster settings), so none of them reads the shared store's migrated tables, and a second
/// <see cref="LivePostgresStoreFixture"/> would run its residue check against a store the parallel collections
/// are still writing. <see cref="PgClusterObjectCensusTests"/> fails when a live class changes a cluster-wide
/// object under a fixed name and is neither here nor on its allow list.</para>
/// </summary>
[CollectionDefinition("pg-cluster-roles", DisableParallelization = true)]
public sealed class PgClusterRolesCollection
{
}
