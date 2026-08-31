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
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.PI;
using Amazon.PI.Model;
using Amazon.RDS;
using Amazon.RDS.Model;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Targets;

/// <summary>
/// Instance-level CPU for Aurora and RDS Postgres targets, read from AWS Performance Insights (#2719) — the
/// same "reach the target through the AWS API instead of a database connection" shape #2538 established for
/// plan capture and deadlocks, but for a signal PostgreSQL never exposes at all rather than one a managed
/// target merely can't reach over <c>pg_read_file</c>. See <see cref="PgCpuUtilizationCollector"/>'s doc
/// comment for why PI's <c>os.cpuUtilization.total.avg</c> was chosen over CloudWatch.
///
/// <para><b>Its own resume watermark, read from the store rather than kept in memory.</b>
/// <see cref="RdsLogSource"/>'s marker is in-memory because re-reading the log tail is harmless (rows dedup on
/// a content hash) — but a CPU reading has no content identity to dedup on, only a timestamp, and this
/// ingestor is constructed fresh by <see cref="DarlingCollectorRunner"/> whenever <c>_rdsCpu</c> is null
/// rather than held across the process lifetime the way <c>_rdsDeadlocks</c>/<c>_rdsPlans</c> are (there is no
/// shared-marker starvation risk to avoid here — nothing else reads PI for this server), so an in-memory
/// watermark would silently reset on every restart and re-store a cycle's worth of already-collected points.
/// Reading <c>MAX(sample_time)</c> from <c>collect.pg_cpu_utilization</c> costs one indexed query per cycle
/// and survives a restart the way every other watermark-driven collector already does.</para>
/// </summary>
public sealed class RdsCpuIngestor
{
    /// <summary>
    /// How far back to ask Performance Insights when this server has no watermark yet (first contact) or the
    /// watermark is older than this. Bounded, matching <see cref="RdsLogSource.FirstReadLines"/>'s reasoning:
    /// PI keeps far more than this, and nothing needs a backfill deeper than a few missed cycles at this
    /// collector's 5-minute cadence (<c>CollectorScheduleDefaults["pg_cpu_utilization"]</c>).
    /// </summary>
    private static readonly TimeSpan LookbackWindow = TimeSpan.FromMinutes(15);

    private const string CpuMetric = "os.cpuUtilization.total.avg";

    private readonly NpgsqlDataSource _postgres;
    private readonly Func<string, IAmazonRDS> _rdsClientFactory;
    private readonly Func<string, IAmazonPI> _piClientFactory;
    private readonly ILogger? _logger;

    public RdsCpuIngestor(
        NpgsqlDataSource postgres,
        Func<string, IAmazonRDS>? rdsClientFactory = null,
        Func<string, IAmazonPI>? piClientFactory = null,
        ILogger? logger = null)
    {
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
        _rdsClientFactory = rdsClientFactory ?? (region => new AmazonRDSClient(RegionEndpoint.GetBySystemName(region)));
        _piClientFactory = piClientFactory ?? (region => new AmazonPIClient(RegionEndpoint.GetBySystemName(region)));
        _logger = logger;
    }

    /// <param name="host">The target's connection host. A non-RDS host is skipped silently — self-hosted
    /// PostgreSQL has no CPU route at all (see <see cref="PgCpuUtilizationCollector"/>'s doc comment), so
    /// there is nowhere else for it to fall back to.</param>
    /// <returns>Rows stored, or zero when this target is not RDS/Aurora or PI had nothing new.</returns>
    public async Task<int> IngestAsync(
        int serverId,
        string storageName,
        string host,
        CancellationToken cancellationToken = default)
    {
        var endpoint = RdsEndpoint.TryParse(host);

        if (endpoint is null)
        {
            return 0;
        }

        var parsed = endpoint.Value;

        if (parsed.Kind is RdsEndpointKind.ClusterReader or RdsEndpointKind.ClusterCustom)
        {
            /* Same refusal as RdsLogSource, for the same reason: a round-robin endpoint does not resolve to
               one stable instance between calls, so CPU readings pulled through it would be attributed to
               whichever replica happened to answer this cycle — not a coherent series for any one instance. */
            throw new InvalidOperationException(
                $"'{host}' is an Aurora {(parsed.Kind == RdsEndpointKind.ClusterReader ? "reader" : "custom")} "
                + "endpoint, which does not resolve to a stable instance — it moves between replicas call to "
                + "call. Point the target at the cluster writer endpoint or at a specific instance so CPU "
                + "readings belong to a server that can be named.");
        }

        List<DataPoint> dataPoints;

        try
        {
            using var rds = _rdsClientFactory(parsed.Region);

            var instanceId = parsed.Kind == RdsEndpointKind.ClusterWriter
                ? await ResolveWriterAsync(rds, parsed.Identifier, cancellationToken)
                : parsed.Identifier;

            var dbiResourceId = await ResolveDbiResourceIdAsync(rds, instanceId, cancellationToken);

            var watermark = await GetWatermarkAsync(serverId, cancellationToken);
            var now = DateTime.UtcNow;
            var startTime = watermark.HasValue && watermark.Value > now - LookbackWindow
                ? watermark.Value
                : now - LookbackWindow;

            using var pi = _piClientFactory(parsed.Region);

            var response = await pi.GetResourceMetricsAsync(
                new GetResourceMetricsRequest
                {
                    ServiceType = ServiceType.RDS,
                    Identifier = dbiResourceId,
                    MetricQueries = new List<Amazon.PI.Model.MetricQuery> { new() { Metric = CpuMetric } },
                    StartTime = startTime,
                    EndTime = now,
                    PeriodInSeconds = 60,
                },
                cancellationToken);

            dataPoints = response.MetricList
                .SelectMany(m => m.DataPoints)
                /* PI returns a data point with a null Value for a period it has no sample for — nothing to
                   store, and Value.HasValue is required below so the row is never null-cast. */
                .Where(p => p.Timestamp.HasValue && p.Value.HasValue
                    && (!watermark.HasValue || p.Timestamp.Value > watermark.Value))
                .OrderBy(p => p.Timestamp!.Value)
                .ToList();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* #2633's fix, shared shape: rethrown so the runner degrades an authorization refusal to
               PERMISSIONS, naming which kind of nothing was found, instead of a SUCCESS row claiming CPU was
               read and simply had nothing new — the same trap #2633 fixed for the log-download route. */
            _logger?.LogWarning(
                "PI/RDS CPU metrics unavailable for {Server}: {Message} — CPU capture is skipped for this "
                + "target this cycle; every other collector is unaffected.",
                storageName, ex.Message);

            throw new PiMetricsUnavailableException(
                ex.Message, RdsLogUnavailableException.IsAuthorizationRefusal(ex), ex);
        }

        if (dataPoints.Count == 0)
        {
            return 0;
        }

        return await WriteAsync(serverId, storageName, dataPoints, cancellationToken);
    }

    /// <summary>Identical resolution to <see cref="RdsLogSource"/>'s own — kept as a separate copy rather
    /// than shared, matching this codebase's existing precedent of each RDS-API ingestor owning its own
    /// AWS calls end to end (see <see cref="RdsDeadlockIngestor"/>'s doc comment on why it holds its own
    /// <see cref="RdsLogSource"/> rather than sharing one).</summary>
    private static async Task<string> ResolveWriterAsync(
        IAmazonRDS client, string clusterId, CancellationToken cancellationToken)
    {
        var clusters = await client.DescribeDBClustersAsync(
            new DescribeDBClustersRequest { DBClusterIdentifier = clusterId }, cancellationToken);

        var cluster = clusters.DBClusters.FirstOrDefault()
            ?? throw new InvalidOperationException($"Aurora cluster '{clusterId}' was not found.");

        var writer = cluster.DBClusterMembers.FirstOrDefault(m => m.IsClusterWriter == true)
            ?? throw new InvalidOperationException(
                $"Aurora cluster '{clusterId}' reports no writer. That is a real state during a failover, "
                + "so this is worth retrying rather than treating as a configuration error.");

        return writer.DBInstanceIdentifier;
    }

    /// <summary>
    /// Performance Insights identifies an instance by its <c>DbiResourceId</c> (a stable, opaque id), not by
    /// the <c>DBInstanceIdentifier</c> name RDS log capture uses — a second API call this collector needs
    /// that the log route does not.
    /// </summary>
    private static async Task<string> ResolveDbiResourceIdAsync(
        IAmazonRDS client, string instanceId, CancellationToken cancellationToken)
    {
        var instances = await client.DescribeDBInstancesAsync(
            new DescribeDBInstancesRequest { DBInstanceIdentifier = instanceId }, cancellationToken);

        var instance = instances.DBInstances.FirstOrDefault()
            ?? throw new InvalidOperationException($"RDS/Aurora instance '{instanceId}' was not found.");

        return instance.DbiResourceId
            ?? throw new InvalidOperationException($"RDS/Aurora instance '{instanceId}' reports no DbiResourceId.");
    }

    private async Task<DateTime?> GetWatermarkAsync(int serverId, CancellationToken cancellationToken)
    {
        await using var command = _postgres.CreateCommand(
            "SELECT MAX(sample_time) FROM collect.pg_cpu_utilization WHERE server_id = $1");
        command.Parameters.AddWithValue(serverId);

        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is DateTime dt ? DateTime.SpecifyKind(dt, DateTimeKind.Utc) : null;
    }

    /// <summary>The same binary COPY the collector runner uses, driven by the collector's own definition —
    /// see <see cref="RdsDeadlockIngestor.WriteAsync"/>, which this mirrors exactly.</summary>
    private async Task<int> WriteAsync(
        int serverId,
        string storageName,
        IReadOnlyList<DataPoint> dataPoints,
        CancellationToken cancellationToken)
    {
        var definition = PgCpuUtilizationCollector.Instance;

        /* Naive UTC, the store's convention for every collector timestamp. */
        var collectionTime = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);

        await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);

        var writer = new PgCollectorRowWriter();
        var written = 0;

        using (var importer = await connection.BeginBinaryImportAsync(
            PgCollectorRowWriter.CopyCommandFor(definition), cancellationToken))
        {
            writer.Importer = importer;

            foreach (var point in dataPoints)
            {
                await importer.StartRowAsync(cancellationToken);

                if (definition.IncludesCollectionId)
                {
                    writer.Value(CollectionIdGenerator.Next());
                }

                writer.Value(collectionTime)
                      .Value(serverId)
                      .Value(storageName);

                writer.BeginPayload();
                definition.WritePayload(
                    new PgCpuUtilizationCollector.Row(
                        DateTime.SpecifyKind(point.Timestamp!.Value, DateTimeKind.Utc), point.Value),
                    writer,
                    NullContext(serverId, storageName, collectionTime));
                writer.EndPayload(definition.PayloadColumns.Count);
                written++;
            }

            await importer.CompleteAsync(cancellationToken);
        }

        _logger?.LogInformation(
            "Stored {Count} CPU reading(s) for {Server} from Performance Insights.", written, storageName);

        return written;
    }

    /* WritePayload takes a context for the collectors that consult deltas or watermarks. This one reads
       none of it - the rows are already fully formed from PI's response - so the context exists to satisfy
       the signature rather than to carry anything. Mirrors RdsDeadlockIngestor.NullContext exactly. */
    private static CollectorContext NullContext(int serverId, string storageName, DateTime collectionTime)
        => new()
        {
            ServerId = serverId,
            ServerName = storageName,
            CollectionTime = collectionTime,
            Deltas = new CollectorDeltaCalculator(),
            Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql },
        };
}
