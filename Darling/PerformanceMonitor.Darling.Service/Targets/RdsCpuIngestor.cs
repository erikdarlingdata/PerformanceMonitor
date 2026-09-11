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
///
/// <para><b>Four metric names, one call</b> (#3281). CPU alone is percent of the capacity currently
/// allocated, which on Aurora Serverless v2 is not a saturation figure, so the capacity-headroom gauge is
/// collected beside it — and it arrives on the <see cref="GetResourceMetricsRequest"/> this ingestor was
/// already making, by naming the metrics in the existing <c>MetricQueries</c> list. See
/// <see cref="RequestedMetrics"/> for the names and why they cannot be paraphrased, and
/// <see cref="BuildSamples"/> for why four metric queries need the response keyed by metric rather than
/// flattened.</para>
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

    /// <summary>Percent of the capacity CURRENTLY ALLOCATED that is not idle. Real, and not a saturation
    /// signal on a serverless instance class — see <see cref="PgCpuUtilizationCollector"/>.</summary>
    private const string CpuMetric = "os.cpuUtilization.total.avg";

    /// <summary>Percent of the CONFIGURED ACU ceiling in use (#3281) — the capacity-headroom gauge every
    /// band reads, because 100% of it means the ceiling really is reached.</summary>
    private const string AcuUtilizationMetric = "os.general.acuUtilization.avg";

    /// <summary>ACU allocated at this minute — <see cref="AcuUtilizationMetric"/>'s numerator.</summary>
    private const string ServerlessCapacityMetric = "os.general.serverlessDatabaseCapacity.avg";

    /// <summary>The configured ACU ceiling — <see cref="AcuUtilizationMetric"/>'s denominator.</summary>
    private const string MaxConfiguredAcuMetric = "os.general.maxConfiguredAcu.avg";

    /// <summary>
    /// Every metric name asked for, in one <c>GetResourceMetrics</c> call — which is what makes #3281's fix
    /// cost nothing: no new SDK package, no second AWS API, and no IAM change, because the instance
    /// profile's existing <c>pi:GetResourceMetrics</c> grant already covers all four.
    ///
    /// <para><b>The <c>os.general.</c> prefix is exact and is not guessable.</b> <c>os.acuUtilization.avg</c>
    /// does NOT exist — Performance Insights rejects it with
    /// <c>InvalidArgumentException: The specified metric is not a known metric</c>, which fails the whole
    /// call and therefore the whole cycle for that target. All four names above were verified live against a
    /// production instance before being written here.</para>
    ///
    /// <para>Internal so a test can assert that every name requested is a name this collector STORES, and
    /// the reverse. A name dropped from this list leaves a column that is silently never populated, which
    /// reads downstream as "the capacity was never measured" — permanently, on every target, with nothing
    /// failing.</para>
    /// </summary>
    internal static readonly IReadOnlyList<string> RequestedMetrics = new[]
    {
        CpuMetric,
        AcuUtilizationMetric,
        ServerlessCapacityMetric,
        MaxConfiguredAcuMetric,
    };

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

    /// <param name="host">The target's connection host. A non-RDS host means Performance Insights was never
    /// queried — self-hosted PostgreSQL has no CPU route at all (see
    /// <see cref="PgCpuUtilizationCollector"/>'s doc comment), so there is nowhere else for it to fall back
    /// to — and the outcome says so rather than reporting an empty sample window (#3017).</param>
    /// <returns>Rows stored and whether Performance Insights was reached at all. This ingestor is the one
    /// that meets the not-reached case routinely: <c>pg_cpu_utilization</c>'s dispatch is UNCONDITIONAL, with
    /// no <c>pg_read_file</c>-shaped fallback for a self-hosted target, so every cycle of every self-hosted
    /// PostgreSQL target lands here and no-ops.</returns>
    public async Task<RdsIngestOutcome> IngestAsync(
        int serverId,
        string storageName,
        string host,
        CancellationToken cancellationToken = default)
    {
        var endpoint = RdsEndpoint.TryParse(host);

        if (endpoint is null)
        {
            /* #3017: NOT_REACHED, not zero rows. No AWS call has been made at this point — nothing is known
               about this instance's CPU, which is a different fact from PI answering with no samples, and
               the runner's note now says which one it was. This branch is reached on every cycle of every
               self-hosted PostgreSQL target, because the dispatch above it is unconditional. */
            return RdsIngestOutcome.NotReached;
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

        List<PgCpuUtilizationCollector.Row> samples;

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
                    /* All four names in one call — see RequestedMetrics for why that is the whole cost of
                       #3281's fix, and for why the os.general. prefix cannot be paraphrased. */
                    MetricQueries = RequestedMetrics
                        .Select(metric => new Amazon.PI.Model.MetricQuery { Metric = metric })
                        .ToList(),
                    StartTime = startTime,
                    EndTime = now,
                    PeriodInSeconds = 60,
                },
                cancellationToken);

            /* Absent and empty mean the same thing coming out of PI — no sample in this window — and the
               caller already names that outcome ("no new Performance Insights CPU samples this cycle"), so
               an empty sequence is the honest reading of a null here. It is NOT the honest reading of an
               absent RDS log-file list, where "no file" means nothing was opened at all; see
               RdsLogSource.NewestLogFileAsync for why that branch raises instead. Both collections are null
               rather than empty when the service omits them, and LINQ over either raises
               ArgumentNullException whose whole message is "Value cannot be null. (Parameter 'source')". */
            samples = BuildSamples(response.MetricList, watermark);
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

        if (samples.Count == 0)
        {
            /* READ, and it held nothing new. PI answered — the caller's "no new Performance Insights CPU
               samples this cycle" is a true statement here, which is exactly why the not-reached branch
               above must not share it. */
            return RdsIngestOutcome.Read(0);
        }

        return RdsIngestOutcome.Read(
            await WriteAsync(serverId, storageName, samples, cancellationToken));
    }

    /// <summary>
    /// One stored row per minute Performance Insights returned a CPU sample for, with the three capacity
    /// metrics from the SAME minute attached (#3281).
    ///
    /// <para><b>Keyed by metric name rather than flattened.</b> Four metric queries come back as four
    /// <c>MetricList</c> entries whose data points carry no metric of their own, so flattening them into
    /// one sequence — which is correct for a single metric query — would store four unlabelled values
    /// per minute as four CPU readings.</para>
    ///
    /// <para><b>The row's identity is the CPU sample, and that is deliberate.</b> A minute PI has a
    /// capacity reading for but no CPU reading produces no row. The reason is the resume watermark: it is
    /// <c>MAX(sample_time)</c>, so a capacity-only row would advance it past a CPU point that arrived one
    /// cycle later and retire that point unread. The cost of the choice is
    /// bounded and self-describing instead: <c>acu_utilization_percent</c> stays NULL for a minute PI had no
    /// capacity sample for, and a NULL there bands Unknown, never Healthy.</para>
    ///
    /// <para>Internal and static so the grouping is assertable without a store, an AWS client or a
    /// connection — it is the step that decides which value lands in which column.</para>
    /// </summary>
    internal static List<PgCpuUtilizationCollector.Row> BuildSamples(
        IEnumerable<MetricKeyDataPoints>? metricList, DateTime? watermark)
    {
        var byMetric = new Dictionary<string, Dictionary<DateTime, double>>(StringComparer.Ordinal);

        foreach (var metric in metricList ?? [])
        {
            var name = metric?.Key?.Metric;

            if (name is null)
            {
                continue;
            }

            if (!byMetric.TryGetValue(name, out var points))
            {
                points = new Dictionary<DateTime, double>();
                byMetric[name] = points;
            }

            foreach (var point in metric!.DataPoints ?? [])
            {
                /* PI returns a data point with a null Value for a period it has no sample for. Nothing is
                   recorded for it, so the column stays NULL — never a 0, which for a capacity ratio would
                   read as measured headroom. */
                if (point.Timestamp.HasValue && point.Value.HasValue)
                {
                    points[point.Timestamp.Value] = point.Value.Value;
                }
            }
        }

        if (!byMetric.TryGetValue(CpuMetric, out var cpu))
        {
            return [];
        }

        return cpu
            .Where(p => !watermark.HasValue || p.Key > watermark.Value)
            .OrderBy(p => p.Key)
            .Select(p => new PgCpuUtilizationCollector.Row(
                DateTime.SpecifyKind(p.Key, DateTimeKind.Utc),
                p.Value,
                ValueAt(byMetric, AcuUtilizationMetric, p.Key),
                ValueAt(byMetric, ServerlessCapacityMetric, p.Key),
                ValueAt(byMetric, MaxConfiguredAcuMetric, p.Key)))
            .ToList();
    }

    /// <summary>One metric's value at one minute, or null where PI had no sample for that pairing. Null
    /// rather than 0 for <see cref="BuildSamples"/>'s reason.</summary>
    private static double? ValueAt(
        Dictionary<string, Dictionary<DateTime, double>> byMetric, string metric, DateTime timestamp) =>
        byMetric.TryGetValue(metric, out var points) && points.TryGetValue(timestamp, out var value)
            ? value
            : null;

    /// <summary>Identical resolution to <see cref="RdsLogSource"/>'s own — kept as a separate copy rather
    /// than shared, matching this codebase's existing precedent of each RDS-API ingestor owning its own
    /// AWS calls end to end (see <see cref="RdsDeadlockIngestor"/>'s doc comment on why it holds its own
    /// <see cref="RdsLogSource"/> rather than sharing one). That includes the null tests: a response
    /// collection the service omitted arrives as null, and LINQ over one raises an ArgumentNullException
    /// naming only "source", so each null goes into the message describing its own branch.</summary>
    private static async Task<string> ResolveWriterAsync(
        IAmazonRDS client, string clusterId, CancellationToken cancellationToken)
    {
        var clusters = await client.DescribeDBClustersAsync(
            new DescribeDBClustersRequest { DBClusterIdentifier = clusterId }, cancellationToken);

        var cluster = clusters.DBClusters?.FirstOrDefault()
            ?? throw new InvalidOperationException(
                $"Aurora cluster '{clusterId}' was not found: DescribeDBClusters returned no cluster for it.");

        var writer = cluster.DBClusterMembers?.FirstOrDefault(m => m.IsClusterWriter == true)
            ?? throw new InvalidOperationException(
                $"Aurora cluster '{clusterId}' reports no writer among its members. That is a real state "
                + "during a failover, so this is worth retrying rather than treating as a configuration "
                + "error.");

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

        var instance = instances.DBInstances?.FirstOrDefault()
            ?? throw new InvalidOperationException(
                $"RDS/Aurora instance '{instanceId}' was not found: DescribeDBInstances returned no "
                + "instance for it.");

        return instance.DbiResourceId
            ?? throw new InvalidOperationException($"RDS/Aurora instance '{instanceId}' reports no DbiResourceId.");
    }

    private async Task<DateTime?> GetWatermarkAsync(int serverId, CancellationToken cancellationToken)
    {
        await using var command = _postgres.CreateCommand(
            "SELECT MAX(sample_time) FROM collect.pg_cpu_utilization WHERE server_id = $1");
        command.CommandTimeout = ServiceCommandDeadlines.CollectionSweepSeconds;
        command.Parameters.AddWithValue(serverId);

        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is DateTime dt ? DateTime.SpecifyKind(dt, DateTimeKind.Utc) : null;
    }

    /// <summary>The same binary COPY the collector runner uses, driven by the collector's own definition —
    /// see <see cref="RdsDeadlockIngestor.WriteAsync"/>, which this mirrors exactly.</summary>
    private async Task<int> WriteAsync(
        int serverId,
        string storageName,
        IReadOnlyList<PgCpuUtilizationCollector.Row> samples,
        CancellationToken cancellationToken)
    {
        var definition = PgCpuUtilizationCollector.Instance;

        /* Naive UTC, the store's convention for every collector timestamp. */
        var collectionTime = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);

        await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);

        var writer = new PgCollectorRowWriter();
        var written = 0;

        /* The start phase's deadline. It is separate from the importer's because the importer does not
           exist until Begin returns, and the await that returns it runs under the connection's
           CommandTimeout, which Npgsql exposes read-only. StoreCopyStartDeadline carries the value, the
           stop-versus-deadline discrimination and the fault shape — see it for why a breach is re-raised
           as a TimeoutException rather than left as the cancellation Npgsql threw. */
        using var startDeadline = StoreCopyStartDeadline.Start(cancellationToken);

        /* Which COPY phase a fault came out of, on the same terms as
           DarlingCollectorRunner.CopyBatchOnceAsync — see CollectorFaultCopyPhase, which is the authority
           for what each value means and what it authorises.

           Start until Begin returns, and the transition sits INSIDE the block for that reason: Start has to
           mean strictly "the importer never came back". A fault anywhere past that line may have sent rows,
           so it must read as Data even where it happens to have sent none. */
        var copyPhase = StoreCopyPhase.Start;

        try
        {
            using (var importer = await connection.BeginBinaryImportAsync(
                PgCollectorRowWriter.CopyCommandFor(definition), startDeadline.Token))
            {
                copyPhase = StoreCopyPhase.Data;

                /* #2874: the COPY's own deadline, on NpgsqlBinaryImporter.Timeout — a TimeSpan on a different type
                   from the rest of the regime, invisible to a command-shaped regex, and inherited from the
                   connection's CommandTimeout (30 s) when left unset. Same constant, same regime, and it reaches
                   the row loop only — startDeadline above bounds the Begin that returns the importer, on the same
                   terms as DarlingCollectorRunner.CopyBatchOnceAsync. */
                importer.Timeout = TimeSpan.FromSeconds(ServiceCommandDeadlines.CollectionSweepSeconds);

                writer.Importer = importer;

                foreach (var sample in samples)
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
                    /* Already fully formed by BuildSamples, including the Kind the store contract needs —
                       the payload writer is the one place the column order lives. */
                    definition.WritePayload(
                        sample,
                        writer,
                        NullContext(serverId, storageName, collectionTime));
                    writer.EndPayload(definition.PayloadColumns.Count);
                    written++;
                }

                await importer.CompleteAsync(cancellationToken);
            }
        }
        /* The start phase's deadline, re-raised as the shape a client-side deadline has here, on the same
           terms as DarlingCollectorRunner.CopyBatchOnceAsync. A throw from a catch arm leaves the whole
           try, so this fault is stamped HERE rather than by the arm below; the phase term in the filter is
           what keeps the arm unreachable once the row loop has begun, whatever Npgsql throws from inside
           it, so a data-phase fault cannot be relabelled as the one a re-attempt trusts. */
        catch (OperationCanceledException cancellation)
            when (copyPhase == StoreCopyPhase.Start && startDeadline.Breached())
        {
            var breach = StoreCopyStartDeadline.Breach(cancellation);
            CollectorFaultCopyPhase.Stamp(breach, copyPhase);
            throw breach;
        }
        /* Stamped, then rethrown bare, for CopyBatchOnceAsync's reason: the fault keeps its own type,
           message and inner chain, so every classification arm upstream sees exactly what it sees without
           this. OperationCanceledException is excluded because a stopping token says the service is shutting
           down, not which protocol exchange was in flight. */
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            CollectorFaultCopyPhase.Stamp(ex, copyPhase);
            throw;
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
