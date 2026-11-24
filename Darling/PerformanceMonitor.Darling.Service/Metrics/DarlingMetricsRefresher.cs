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
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Metrics;

/// <summary>
/// #9013: the one store read behind the /metrics snapshot, which <see cref="DarlingMetricsSnapshotCache"/> runs at most
/// once a minute. It reads the fleet exactly as <c>/api/fleet</c> does, admitted under that route's name and band, then
/// the server-set statements on one connection, and labels both with one server map.
///
/// <para>The data source is the host's (it resolves the store login lazily), so no connection string is built here.
/// The token given is passed to every await: it carries the cache's timeout, and nothing else ends a hung read.</para>
/// </summary>
internal sealed class DarlingMetricsRefresher
{
    internal const string AdmissionRoute = "get_fleet_overview";

    private readonly Func<CancellationToken, Task<NpgsqlDataSource?>> _dataSource;
    private readonly ReadAdmission _admission;
    private readonly MonitoredServerRegistryState? _registry;
    private readonly CollectorRuntimeState _collector;
    private readonly TimeProvider _time;
    private readonly ILogger _logger;

    /* Ticks, -1 for "not measured yet": a TimeSpan? cannot be written atomically, and LastFleetDuration and
       LastServerSetDuration are read on another thread than the refresh that writes them. */
    private long _fleetTicks = -1;
    private long _serverSetTicks = -1;

    /* #9013: the collision sets last logged, so the Information line comes once per change, not once a minute. Only the
       refresh that holds the cache's single-flight slot runs RefreshAsync, so one writer at a time. */
    private string _loggedCollisions = string.Empty;

    public DarlingMetricsRefresher(
        Func<CancellationToken, Task<NpgsqlDataSource?>> dataSource,
        ReadAdmission admission,
        MonitoredServerRegistryState? registry,
        CollectorRuntimeState collector,
        TimeProvider time,
        ILogger logger)
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
        _admission = admission ?? throw new ArgumentNullException(nameof(admission));
        _registry = registry;
        _collector = collector ?? throw new ArgumentNullException(nameof(collector));
        _time = time ?? throw new ArgumentNullException(nameof(time));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>How long the last fleet read took; null before the first one finished.</summary>
    internal TimeSpan? LastFleetDuration => ToSpan(Interlocked.Read(ref _fleetTicks));

    /// <summary>How long the last server-set read took; null before the first one finished.</summary>
    internal TimeSpan? LastServerSetDuration => ToSpan(Interlocked.Read(ref _serverSetTicks));

    /* #9013: until the worker collects, the store may still be migrating: a scrape then answers meta only. */
    internal bool CanRefresh() => _collector.Read()?.Phase == CollectorRuntimeState.CollectorPhase.Collecting;

    internal async Task<MetricsSnapshot> RefreshAsync(CancellationToken ct)
    {
        var postgres = await _dataSource(ct).ConfigureAwait(false)
            ?? throw new InvalidOperationException("metrics: the store login is not available yet");

        /* #9013: admitted as /api/fleet is, so a scraper adds to the same heavy-read limit the dashboard draws on. The
           ticket is held for the whole refresh, the server set included. */
        using var ticket = await _admission.AdmitAsync(ReadSurface.Web, AdmissionRoute, ReadWindowBand.OneHour, ct).ConfigureAwait(false);
        if (ticket.Refusal is not null)
        {
            throw new InvalidOperationException("metrics: the store read was refused by read admission (too many heavy reads running)");
        }

        if (ticket.CallerLeft)
        {
            throw new OperationCanceledException(ct);
        }

        var now = _time.GetUtcNow().UtcDateTime;

        var fleetStarted = _time.GetTimestamp();
        var registry = _registry;
        var fleet = await DarlingFleetReader.GetFleetOverviewAsync(
            postgres, now.AddHours(-1), now, now,
            separatelyMonitored: registry is null
                ? null
                : (serverId, token) => DarlingWorker.AnalysisSeparatelyMonitoredDatabasesAsync(serverId, registry.Read(), postgres, token),
            logger: _logger,
            allowedTagIds: null,
            cancellationToken: ct).ConfigureAwait(false);
        var fleetDuration = _time.GetElapsedTime(fleetStarted);
        Interlocked.Exchange(ref _fleetTicks, fleetDuration.Ticks);

        var labels = DarlingMetricsFleetMapper.AssignServerLabels(
            fleet.Cards.Select(card => (card.ServerId, card.DisplayName)), out var collisions);
        var collisionKey = string.Join(";", collisions.Select(set => string.Join(",", set)));
        if (!string.Equals(collisionKey, _loggedCollisions, StringComparison.Ordinal))
        {
            _loggedCollisions = collisionKey;
            foreach (var set in collisions)
            {
                _logger.LogInformation(
                    "Metrics: servers {ServerIds} share a display name, so each one's server label carries \" #<server_id>\".",
                    string.Join(", ", set));
            }
        }

        var (fleetSamples, stamps) = DarlingMetricsFleetMapper.Map(fleet.Cards, labels);

        var serverSetStarted = _time.GetTimestamp();
        IReadOnlyList<ServerSetValue> values;
        await using (var connection = await postgres.OpenConnectionAsync(StoreSessionNames.MetricsRefresh, ct).ConfigureAwait(false))
        {
            values = await DarlingMetricsServerSetReader.ReadAsync(connection, now, ct).ConfigureAwait(false);
        }

        var serverSetDuration = _time.GetElapsedTime(serverSetStarted);
        Interlocked.Exchange(ref _serverSetTicks, serverSetDuration.Ticks);

        var samples = new List<MetricSample>(fleetSamples.Count + values.Count);
        samples.AddRange(fleetSamples);
        foreach (var value in values)
        {
            /* #9013: a server the fleet read did not see (disabled or added since) has no label; its values wait for the
               next refresh rather than appear under a name no band series carries. */
            if (!labels.TryGetValue(value.ServerId, out var server))
            {
                continue;
            }

            var sampleLabels = new List<KeyValuePair<string, string>>(1 + value.Labels.Count)
            {
                new(DarlingMetricCatalog.ServerLabel, server),
            };
            sampleLabels.AddRange(value.Labels);
            samples.Add(new MetricSample(value.Metric, sampleLabels, value.Value));
        }

        _logger.LogDebug(
            "Metrics refresh read {Servers} servers: fleet read {FleetMs:F0} ms, server set {ServerSetMs:F0} ms.",
            fleet.Cards.Count,
            fleetDuration.TotalMilliseconds,
            serverSetDuration.TotalMilliseconds);

        return new MetricsSnapshot(now, samples, stamps);
    }

    private static TimeSpan? ToSpan(long ticks) => ticks < 0 ? null : TimeSpan.FromTicks(ticks);
}
