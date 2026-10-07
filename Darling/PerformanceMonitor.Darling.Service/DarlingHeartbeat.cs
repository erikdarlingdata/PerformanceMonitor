/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net.Http;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The <c>heartbeat</c> block of darling.json (#5450, proposal 2): an outbound GET to a dead-man's-switch URL so a
/// check OUTSIDE the box notices when the service stops, or runs but no longer collects. Off by default.
///
/// <para><b>FILE-ONLY, like <see cref="PeersConfig"/>.</b> The web Admin and MCP cannot edit it and no read surface
/// returns it: <see cref="Url"/> usually carries the check's token, so the WHOLE URL is a secret. It takes the same
/// <c>env:</c>/<c>file:</c> reference the other secrets take (<see cref="DarlingSecretSource"/>, #1804), resolved where
/// it is used, so a rotated token file is picked up on the next ping. The URL is never logged and never put in a
/// validation message: logs and problems name the scheme and host only.</para>
/// </summary>
public sealed class HeartbeatConfig
{
    /// <summary>Seconds between pings when <c>intervalSeconds</c> is omitted.</summary>
    public const int DefaultIntervalSeconds = 300;

    /// <summary>The shortest interval a config may ask for.</summary>
    public const int MinIntervalSeconds = 60;

    /// <summary>The longest interval a config may ask for.</summary>
    public const int MaxIntervalSeconds = 3600;

    /// <summary>
    /// The URL to GET, or an <c>env:NAME</c>/<c>file:/path</c> reference to it. Absent or empty = the heartbeat is off
    /// (the default). An absolute http or https URL only. A secret: never logged, never returned by any read surface.
    /// </summary>
    [JsonPropertyName("url")]
    public string? Url { get; set; }

    /// <summary>Seconds between pings, <see cref="MinIntervalSeconds"/> to <see cref="MaxIntervalSeconds"/>.</summary>
    [JsonPropertyName("intervalSeconds")]
    public int IntervalSeconds { get; set; } = DefaultIntervalSeconds;

    /// <summary>True when a URL (or a reference to one) is set. Whitespace counts as not set.</summary>
    [JsonIgnore]
    public bool IsConfigured => !string.IsNullOrWhiteSpace(Url);

    /// <summary>
    /// Resolves <see cref="Url"/> to an absolute http/https <see cref="Uri"/>. A failure's <paramref name="problem"/>
    /// names the setting, and for a reference the variable or file it points at, and NEVER the URL itself.
    /// </summary>
    internal bool TryResolveUrl(out Uri? uri, out string? problem)
    {
        uri = null;
        problem = null;
        if (!IsConfigured)
        {
            return true;
        }

        var text = Url!.Trim();
        if (DarlingSecretSource.IsReference(text))
        {
            try
            {
                text = DarlingSecretSource.Resolve(text, "heartbeat.url").Trim();
            }
            catch (InvalidOperationException ex)
            {
                problem = ex.Message;
                return false;
            }
        }

        if (!Uri.TryCreate(text, UriKind.Absolute, out var parsed)
            || (parsed.Scheme != Uri.UriSchemeHttp && parsed.Scheme != Uri.UriSchemeHttps)
            || string.IsNullOrEmpty(parsed.Host))
        {
            problem = "heartbeat.url must be an absolute http or https URL. (The value is not echoed: the URL is a secret.)";
            return false;
        }

        uri = parsed;
        return true;
    }

    /// <summary>
    /// Validates the block; returns human-readable problems (empty = valid). Fatal, like the rest of
    /// <see cref="DarlingConfig.Validate"/>: a heartbeat that would silently never ping is worse than a refused start.
    /// </summary>
    public static IReadOnlyList<string> Validate(HeartbeatConfig? heartbeat)
    {
        var problems = new List<string>();
        if (heartbeat is null)
        {
            return problems;
        }

        if (heartbeat.IntervalSeconds < MinIntervalSeconds || heartbeat.IntervalSeconds > MaxIntervalSeconds)
        {
            problems.Add(string.Create(CultureInfo.InvariantCulture,
                $"heartbeat.intervalSeconds must be between {MinIntervalSeconds} and {MaxIntervalSeconds} (got {heartbeat.IntervalSeconds})."));
        }

        if (!heartbeat.TryResolveUrl(out _, out var problem) && problem is not null)
        {
            problems.Add(problem);
        }

        return problems;
    }
}

/// <summary>What one heartbeat read of the store found.</summary>
/// <param name="Ok">False when the read failed.</param>
/// <param name="EnabledServers">How many monitored servers are enabled.</param>
/// <param name="NewestUtc">The newest <c>collection_log</c> time across the enabled servers; null when none has a row.</param>
internal readonly record struct HeartbeatRead(bool Ok, int EnabledServers, DateTime? NewestUtc);

/// <summary>Whether a tick pings.</summary>
internal enum HeartbeatDecision
{
    /// <summary>Send the GET.</summary>
    Ping,

    /// <summary>The process runs but collection is stale (or nothing has been collected): no ping.</summary>
    SkipStale,

    /// <summary>The store read failed: no ping.</summary>
    SkipReadFailed,
}

/// <summary>The edge a ping result produced for the log.</summary>
internal enum HeartbeatLogEdge
{
    /// <summary>Nothing to log.</summary>
    None,

    /// <summary>Log one warning.</summary>
    Warn,

    /// <summary>Log the one recovery line.</summary>
    Recovered,
}

/// <summary>
/// The failure-log throttle: the first failure warns, then at most one warning per hour until a ping succeeds, and
/// that success logs one recovery line. Pure state, so the clock is a parameter.
/// </summary>
internal sealed class HeartbeatFailureGate
{
    internal static readonly TimeSpan WarnEvery = TimeSpan.FromHours(1);

    private bool _failing;
    private DateTime _lastWarnUtc;

    internal HeartbeatLogEdge OnFailure(DateTime nowUtc)
    {
        if (!_failing)
        {
            _failing = true;
            _lastWarnUtc = nowUtc;
            return HeartbeatLogEdge.Warn;
        }

        if (nowUtc - _lastWarnUtc >= WarnEvery)
        {
            _lastWarnUtc = nowUtc;
            return HeartbeatLogEdge.Warn;
        }

        return HeartbeatLogEdge.None;
    }

    internal HeartbeatLogEdge OnSuccess()
    {
        if (!_failing)
        {
            return HeartbeatLogEdge.None;
        }

        _failing = false;
        return HeartbeatLogEdge.Recovered;
    }
}

/// <summary>
/// The outbound heartbeat loop (#5450, proposal 2). Each interval it reads the newest <c>collection_log</c> time
/// across enabled servers (#5454's read, one index probe per server) and GETs the URL only when the service is
/// provably collecting: no enabled servers, or the newest time inside
/// <see cref="DarlingSelfAlertEvaluator.CollectionGapAtStartThreshold"/>. A stale read or a failed read skips the
/// ping, so the external check alerts when the process stops AND when it runs but no longer collects.
///
/// <para>A ping never raises an alert, never blocks collection (its own task, its own connection, a 10 second
/// timeout) and ends cleanly on shutdown. It follows no redirect and counts only a 2xx as success. The URL is a
/// secret and reaches no log line: a warning names the scheme and host and a reason that is a status code, a
/// timeout, or an exception TYPE, never an exception message.</para>
/// </summary>
internal sealed class DarlingHeartbeat
{
    /// <summary>How long one ping may take, from the send to the response headers.</summary>
    internal static readonly TimeSpan PingTimeout = TimeSpan.FromSeconds(10);

    /// <summary>
    /// The store read: the enabled-server count beside #5454's newest-time probe
    /// (<see cref="DarlingSelfAlertEvaluator.NewestCollectionTimeSql"/>, shared, not copied). The count is what tells
    /// "no server to collect from" (a heartbeat is right) from "servers but nothing collected" (it is not).
    /// </summary>
    internal static readonly string ReadSql =
        "SELECT (SELECT count(*) FROM config.config_monitored_servers WHERE is_enabled), ("
        + DarlingSelfAlertEvaluator.NewestCollectionTimeSql + ")";

    /* Mirrors the webhook senders' pooled clients (WebhookAlertService): one client for the process's life rather
       than a handler per ping, with redirects off so a 3xx is reported as the failure it is and is never followed
       to a host the operator did not configure. */
    private static readonly HttpClient s_client =
        new(new SocketsHttpHandler { PooledConnectionLifetime = TimeSpan.FromMinutes(5), AllowAutoRedirect = false })
        { Timeout = TimeSpan.FromSeconds(30) };

    /// <summary>The production client, so a test can prove its redirect setting rather than a copy's.</summary>
    internal static HttpClient SharedClient => s_client;

    private readonly ILogger _logger;
    private readonly HttpClient _client;
    private readonly Func<DateTime> _utcNow;
    private readonly HeartbeatFailureGate _gate = new();
    private bool _skipping;

    internal DarlingHeartbeat(ILogger logger, HttpClient? client = null, Func<DateTime>? utcNow = null)
    {
        _logger = logger;
        _client = client ?? s_client;
        _utcNow = utcNow ?? (() => DateTime.UtcNow);
    }

    /// <summary>
    /// The ping decision. Fresh = ping; no enabled server = ping (a service with nothing to collect is up);
    /// stale, or enabled servers with no row at all = skip; a failed read = skip. A newest time AFTER now (clock
    /// skew) counts as fresh, like #5454's negative gap.
    /// </summary>
    internal static HeartbeatDecision Decide(HeartbeatRead read, DateTime nowUtc)
    {
        if (!read.Ok)
        {
            return HeartbeatDecision.SkipReadFailed;
        }

        if (read.EnabledServers == 0)
        {
            return HeartbeatDecision.Ping;
        }

        if (read.NewestUtc is not { } newest)
        {
            return HeartbeatDecision.SkipStale;
        }

        return nowUtc - newest < DarlingSelfAlertEvaluator.CollectionGapAtStartThreshold
            ? HeartbeatDecision.Ping
            : HeartbeatDecision.SkipStale;
    }

    /// <summary>
    /// Reads the enabled-server count and the newest collection time. A failure is a result (<c>Ok</c> false), not a
    /// throw, so the loop skips the ping; only cancellation propagates.
    /// </summary>
    internal async Task<HeartbeatRead> ReadAsync(NpgsqlDataSource store, CancellationToken cancellationToken)
    {
        try
        {
            await using var command = store.CreateCommand(ReadSql);
            command.CommandTimeout = 30;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                return new HeartbeatRead(false, 0, null);
            }

            var servers = Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture);
            DateTime? newest = reader.IsDBNull(1) ? null : DateTime.SpecifyKind(reader.GetDateTime(1), DateTimeKind.Utc);
            return new HeartbeatRead(true, servers, newest);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* The type only: a driver message can carry the connection's host. */
            _logger.LogWarning("Heartbeat could not read the newest collection time ({Reason}); no ping this interval.", ex.GetType().Name);
            return new HeartbeatRead(false, 0, null);
        }
    }

    /// <summary>
    /// One GET. Success is a 2xx and nothing else; redirects are not followed, so a 3xx is a failure. A timeout is a
    /// failure with its own reason, and a cancelled <paramref name="cancellationToken"/> propagates as the stop it is.
    /// The reason never carries the URL or an exception message.
    /// </summary>
    internal static async Task<(bool Success, string? Reason)> PingAsync(
        HttpClient client, Uri url, TimeSpan timeout, CancellationToken cancellationToken)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        linked.CancelAfter(timeout);
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, url);
            using var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, linked.Token);
            var code = (int)response.StatusCode;
            return code is >= 200 and <= 299
                ? (true, null)
                : (false, string.Create(CultureInfo.InvariantCulture, $"HTTP {code}"));
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return (false, string.Create(CultureInfo.InvariantCulture, $"no answer within {(int)timeout.TotalSeconds} seconds"));
        }
        catch (HttpRequestException ex)
        {
            return (false, "request failed: " + ex.HttpRequestError);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return (false, "request failed: " + ex.GetType().Name);
        }
    }

    /// <summary>
    /// One interval: resolve the URL, read the store, decide, and ping or skip. Returns what it did. Never throws
    /// except for cancellation.
    /// </summary>
    internal async Task<HeartbeatDecision?> TickAsync(
        HeartbeatConfig config, Func<CancellationToken, Task<HeartbeatRead>> read, CancellationToken cancellationToken)
    {
        if (!config.TryResolveUrl(out var url, out var problem) || url is null)
        {
            /* The problem names the setting and the variable or file, never the URL. Counted as a failed ping so it
               warns once and then hourly, and the external check alerts, which is what it should do. */
            LogFailure("the heartbeat.url reference could not be resolved: " + problem, null);
            return null;
        }

        var snapshot = await read(cancellationToken);
        var now = _utcNow();
        var decision = Decide(snapshot, now);
        if (decision != HeartbeatDecision.Ping)
        {
            if (!_skipping)
            {
                _skipping = true;
                _logger.LogWarning(
                    decision == HeartbeatDecision.SkipReadFailed
                        ? "Heartbeat ping skipped: the store could not be read. The external check will alert until collection can be confirmed."
                        : "Heartbeat ping skipped: no enabled server has collected within {Minutes} minutes. The external check will alert until collection resumes.",
                    (int)DarlingSelfAlertEvaluator.CollectionGapAtStartThreshold.TotalMinutes);
            }

            return decision;
        }

        if (_skipping)
        {
            _skipping = false;
            _logger.LogInformation("Heartbeat pings resume: collection is current.");
        }

        var (success, reason) = await PingAsync(_client, url, PingTimeout, cancellationToken);
        if (success)
        {
            if (_gate.OnSuccess() == HeartbeatLogEdge.Recovered)
            {
                _logger.LogInformation("Heartbeat ping to {Scheme}://{Host} succeeded again.", url.Scheme, url.Host);
            }
        }
        else
        {
            LogFailure(reason ?? "unknown", url);
        }

        return decision;
    }

    private void LogFailure(string reason, Uri? url)
    {
        if (_gate.OnFailure(_utcNow()) != HeartbeatLogEdge.Warn)
        {
            return;
        }

        if (url is null)
        {
            _logger.LogWarning("Heartbeat ping not sent ({Reason}). Further failures are logged at most once an hour.", reason);
            return;
        }

        _logger.LogWarning(
            "Heartbeat ping to {Scheme}://{Host} failed ({Reason}). Further failures are logged at most once an hour.",
            url.Scheme, url.Host, reason);
    }

    /// <summary>
    /// The loop: a tick now, then one per <see cref="HeartbeatConfig.IntervalSeconds"/> until the token is cancelled.
    /// Returns at once when no URL is set. A tick that throws (it should not) is logged by type and the loop goes on.
    /// </summary>
    internal async Task RunAsync(HeartbeatConfig config, NpgsqlDataSource store, CancellationToken stoppingToken)
    {
        if (!config.IsConfigured)
        {
            return;
        }

        var interval = TimeSpan.FromSeconds(config.IntervalSeconds);
        if (config.TryResolveUrl(out var first, out _) && first is not null)
        {
            _logger.LogInformation(
                "Heartbeat on: GET {Scheme}://{Host} every {Seconds} s while collection is current.",
                first.Scheme, first.Host, config.IntervalSeconds);
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await TickAsync(config, ct => ReadAsync(store, ct), stoppingToken);
                await Task.Delay(interval, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogWarning("Heartbeat tick failed ({Reason}).", ex.GetType().Name);
                try
                {
                    await Task.Delay(interval, stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }
    }
}
