/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3691, the PostgreSQL-target exit criterion: a pass in which one fact family could not be read says so on
/// the <c>analyze_server</c> payload, through the REAL tool method against a server stamped
/// <c>engine_kind='postgres'</c> with 25 hours of the <c>pg_database_stats</c> witness.
///
/// <para>The planted fault is a table the target collector reads in exactly ONE family: <c>pg_autovacuum_stats</c>
/// is the vacuum family's backlog read and nothing else's (the write family's <c>pg_write_stats</c>, the STEP's
/// example, is also read by the buffer family and the WAL baseline, and would plant two failures and a
/// detector fault). The table is renamed away for the blind pass and renamed back — in the <c>finally</c>,
/// ahead of the row cleanup — so the store the next live class inherits is whole. The vacuum family's other
/// other reads (<c>pg_wraparound_stats</c>, <c>pg_xmin_horizon</c>) still run, which is why each entry names the
/// READ beside the family (<c>vacuum</c>, off the partial file), and why <c>families_failed</c> is 1 of the
/// collector's 16 however many vacuum reads the table serves — the backlog read, and since step 22 the
/// autovacuum-disabled read too; the entries are asserted over the family, never by count.</para>
///
/// <para>Both arms on the same store: the clean pass first, whose envelope carries no <c>collection_caveats</c>
/// at all (not a null — the field is absent), then the blind one.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgTargetCollectionCaveatsLiveTests
{
    private const string ServerName = "darling-pgtarget-collection-caveats-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string HiddenName = "pg_autovacuum_stats_hidden_3691";

    [Fact]
    public async Task APassWhoseVacuumFamilyCouldNotBeRead_SaysSo_AndACleanPassCarriesNoCaveat()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the collection-caveat e2e.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        var renamed = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, ServerId, ServerName, MonitoredEngineKind.Postgres, 17, ct);

            /* 25 hours of the witness, one row a minute, so the 24h gate passes and the 4h window is observed. */
            var end = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified).AddMinutes(-1);
            const int minutes = 25 * 60;
            using (var plant = new NpgsqlCommand(@"
INSERT INTO pg_database_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     xact_commit, xact_rollback, blks_read, blks_hit, temp_files, temp_bytes, deadlocks, stats_reset)
SELECT $1 + n, $2 + (n * interval '1 minute'), $3, $4, 'appdb', 1000 + n, 10, 100, 9000, 0, 0, 0, NULL
FROM generate_series(0, $5) AS n", connection) { CommandTimeout = 300 })
            {
                plant.Parameters.AddWithValue(CollectionIdGenerator.Next());
                plant.Parameters.AddWithValue(end.AddMinutes(-minutes));
                plant.Parameters.AddWithValue(ServerId);
                plant.Parameters.AddWithValue(ServerName);
                plant.Parameters.AddWithValue(minutes);
                await plant.ExecuteNonQueryAsync(ct);
            }

            var service = new DarlingAnalysisService(postgres);

            /* ── clean: no collection_caveats anywhere in the payload, the family total stamped. */
            var clean = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName);
            using (var doc = JsonDocument.Parse(clean))
            {
                var status = doc.RootElement.GetProperty("status").GetString();
                Assert.True(status is "empty" or "findings", $"the clean pass must be a real pass, got {status}: {clean}");
                Assert.False(Envelope(doc.RootElement, status!).TryGetProperty("collection_caveats", out _), "a clean pass must not carry collection_caveats — not even as null");
            }
            Assert.DoesNotContain("COLLECTION CAVEAT", clean, StringComparison.Ordinal);
            Assert.Empty(service.LastCollectionFailures);
            /* 16 → 19 with the #3691 v3 plumbing (plan, kernel, memory stubs), 20 with lane 38's object-growth family — the
               denominator counts family READS. */
            Assert.Equal(20, service.LastCollectionFamilyCount);

            /* ── one family blind. */
            await ExecuteAsync(connection, $"ALTER TABLE pg_autovacuum_stats RENAME TO {HiddenName}", ct);
            renamed = true;

            var blind = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName);
            using (var doc = JsonDocument.Parse(blind))
            {
                var status = doc.RootElement.GetProperty("status").GetString();
                Assert.True(status is "empty" or "findings", $"the blind pass must still be a pass, got {status}: {blind}");
                var envelope = Envelope(doc.RootElement, status!);

                /* ONE family missing, however many of its reads the table serves: the backlog read (v1) and, since
                   #3691 step 22, the autovacuum-disabled read both fail on the same 42P01, and families_failed
                   counts DISTINCT families. The entries are asserted as a set over the family, not by count, so a
                   further read of this table in the vacuum family joins the list without moving this pin. */
                var caveats = envelope.GetProperty("collection_caveats");
                Assert.Equal(1, caveats.GetProperty("families_failed").GetInt32());
                Assert.Equal(20, caveats.GetProperty("families_total").GetInt32());
                var entries = caveats.GetProperty("entries").EnumerateArray().ToList();
                Assert.NotEmpty(entries);
                Assert.All(entries, entry =>
                {
                    Assert.Equal("vacuum", entry.GetProperty("family").GetString());
                    Assert.Equal("missing_schema", entry.GetProperty("outcome").GetString());
                    Assert.DoesNotContain("pg_autovacuum_stats", entry.GetProperty("message").GetString(), StringComparison.Ordinal);
                    Assert.Equal(
                        "PostgresException, SQLSTATE 42P01; a table or column the read needs is missing, logged only at Debug level",
                        entry.GetProperty("message").GetString());
                });
                Assert.Contains(entries, entry => entry.GetProperty("read").GetString() == "ReadAutovacuumBacklogAsync");

                /* Appended LAST — every property the clean payload had precedes it. */
                Assert.Equal("collection_caveats", envelope.EnumerateObject().Last().Name);

                /* The sentence, in the message on `empty` and in the caveat on `findings`. */
                var prose = status == "empty"
                    ? doc.RootElement.GetProperty("message").GetString()
                    : doc.RootElement.GetProperty("caveat").GetString();
                Assert.Contains("1 of 20 fact families could not be read (vacuum (missing_schema)) — the absence of findings is not evidence", prose, StringComparison.Ordinal);
                if (status == "empty")
                {
                    Assert.Contains(" COLLECTION CAVEAT: ", prose, StringComparison.Ordinal);
                    Assert.True(envelope.GetProperty("fact_count").ValueKind == JsonValueKind.Number);
                    Assert.True(envelope.GetProperty("facts_scored").ValueKind == JsonValueKind.Number);
                }
            }
            Assert.NotEmpty(service.LastCollectionFailures);
            Assert.All(service.LastCollectionFailures, failure =>
            {
                Assert.Equal("vacuum", failure.Family);
                Assert.Equal(PerformanceMonitor.Analysis.CollectionFailureOutcome.MissingSchema, failure.Outcome);
            });

            /* get_analysis_facts on the same blind store: the block rides on the data result too. */
            var facts = await DarlingMcpTools.GetAnalysisFacts(service, postgres, ServerName);
            using (var doc = JsonDocument.Parse(facts))
            {
                var root = doc.RootElement;
                Assert.True(root.TryGetProperty("total_facts", out _), $"the facts read must return data over an observed window: {facts}");
                Assert.Contains("1 of 20 fact families could not be read (vacuum (missing_schema))", root.GetProperty("caveat").GetString(), StringComparison.Ordinal);
                Assert.All(root.GetProperty("collection_caveats").GetProperty("entries").EnumerateArray(), entry => Assert.Equal("vacuum", entry.GetProperty("family").GetString()));
                Assert.Equal("collection_caveats", root.EnumerateObject().Last().Name);
            }

            bodySucceeded = true;
        }
        finally
        {
            /* The table comes back BEFORE the row cleanup, whatever happened above — the next live class must
               find the store whole — and inside the #1902 shape, on the cleanup's own connection, so a failed
               rename can neither replace the body's exception nor abandon the deletes behind it. */
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                if (renamed)
                    await ExecuteAsync(cleanup, $"ALTER TABLE {HiddenName} RENAME TO pg_autovacuum_stats", cleanupCt);
                await DeleteRowsAsync(cleanup, cleanupCt);
            });
        }
    }

    /// <summary>Where the block lives: under <c>hints</c> on the shared miss envelope, at the root on a data result.</summary>
    private static JsonElement Envelope(JsonElement root, string status) =>
        status == "empty" ? root.GetProperty("hints") : root;

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_database_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_findings WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_muted WHERE server_id = {ServerId}; " +
            $"DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
