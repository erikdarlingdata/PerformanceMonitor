/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using Xunit;
using Xunit.v3;

namespace Darling.Tests;

/// <summary>
/// Fails the test that leaves PUBLIC without <c>CONNECT</c> on the shared test database (#5618).
///
/// <para><b>Why it exists.</b> The product's managed provisioning batch writes
/// <c>REVOKE ALL ON DATABASE darling FROM PUBLIC</c>, and database privileges live in <c>pg_database</c>, which every
/// database in the cluster shares. A live test that ran that batch inside its own scratch database still took CONNECT
/// away from PUBLIC on the cluster's <c>darling</c> database, and nothing gave it back. The next test that opened a
/// connection as a freshly created role (a role has CONNECT only through PUBLIC) failed with 42501, and it was
/// that victim, not the test that did it, whose name appeared in the nightly run. It took a day of measuring to find
/// the cause. This puts the failure on the test that does it.</para>
///
/// <para><b>What it compares.</b> PUBLIC's CONNECT on each database before the test against the same read after it,
/// and fails only a change from granted to revoked. It does not demand that PUBLIC hold CONNECT: a developer's own
/// managed store has it revoked on purpose, and a guard that failed there would fail every test in the collection.
/// The databases are the one <c>DARLING_TEST_PG</c> names and the managed store's fixed name <c>darling</c>, which are
/// one database in CI and two on a laptop that keeps its suite in another database.</para>
///
/// <para><b>Where it runs.</b> On <see cref="PgClusterRolesCollection"/>, the collection for the tests that change
/// cluster-wide state under fixed names. That collection runs alone, so a change between one test's start and end is
/// that test's. <see cref="PgClusterObjectCensusTests"/> keeps every other class that changes a database-level
/// privilege out of the parallel collections. Without <c>DARLING_TEST_PG</c> it does nothing, as the tests skip.</para>
///
/// <para><b>#1776 own-store</b>, not <c>[Collection("live-postgres")]</c>: it reads one cluster-wide catalog
/// (<c>pg_database</c>) and no row or relation of the shared store, so it has nothing to serialize. It runs around the
/// tests of the one collection that does.</para>
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class PublicConnectGuardAttribute : BeforeAfterTestAttribute
{
    /// <summary>The state read in <see cref="Before"/>. Static so it does not depend on whether xUnit reuses one attribute
    /// instance; the collection it guards runs one test at a time, so one slot is enough.</summary>
    private static Dictionary<string, bool>? s_before;

    public override void Before(MethodInfo methodUnderTest, IXunitTest test)
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        s_before = string.IsNullOrEmpty(cs) ? null : PublicConnectOn(cs);
    }

    public override void After(MethodInfo methodUnderTest, IXunitTest test)
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        var before = s_before;
        s_before = null;
        if (string.IsNullOrEmpty(cs) || before is null)
        {
            return;
        }

        var lost = PublicConnectDelta.DatabasesThatLostPublicConnect(before, PublicConnectOn(cs));
        if (lost.Count > 0)
        {
            throw new InvalidOperationException(
                $"{test.TestDisplayName} left PUBLIC without CONNECT on database {string.Join(", ", lost.Select(d => "\"" + d + "\""))} " +
                "(#5618). Database privileges live in the cluster's shared catalog, so a test that runs the managed provisioning batch " +
                "(ProvisioningTarget.Managed, whose PUBLIC revoke names the shared database) takes CONNECT away " +
                "from every later test that relies on it. Point the batch at the test's own scratch database " +
                "(ProvisioningTarget.ComposeStore(owner, scratch.DatabaseName), or the managed text with its database-level statements retargeted), " +
                "or grant CONNECT back in a finally.");
        }
    }

    /// <summary>
    /// Whether PUBLIC holds CONNECT on each of <paramref name="databases"/> that exists on the cluster
    /// <paramref name="connectionString"/> reaches (<c>DARLING_TEST_PG</c>'s own database and the managed store's
    /// fixed name when none is passed). Unpooled: a pooled session must not outlive a read of a shared catalog.
    /// </summary>
    internal static Dictionary<string, bool> PublicConnectOn(string connectionString, params string[] databases)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString) { Pooling = false };
        var names = databases.Length > 0
            ? databases
            : new[] { builder.Database ?? string.Empty, ProvisioningTarget.Managed.DatabaseIdentifier };

        using var connection = new NpgsqlConnection(builder.ConnectionString);
        connection.Open();
        using var command = new NpgsqlCommand(
            "SELECT datname, has_database_privilege('public', datname, 'CONNECT') FROM pg_database WHERE datname = ANY (@names)", connection);
        command.Parameters.AddWithValue("names", names.Distinct(StringComparer.Ordinal).ToArray());
        var state = new Dictionary<string, bool>(StringComparer.Ordinal);
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            state[reader.GetString(0)] = reader.GetBoolean(1);
        }

        return state;
    }
}
