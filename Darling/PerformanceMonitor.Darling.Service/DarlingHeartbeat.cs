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
using System.IO;
using System.Net.Http;
using System.Text;
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
/// <c>env:</c>/<c>file:</c> reference the other secrets take (<see cref="DarlingSecretSource"/>, #1804). Validation
/// checks only the SHAPE of a reference and never resolves it; the loop resolves it on every tick, so a rotated token
/// file is picked up on the next ping. The URL is never logged and never put in a validation message: logs name the
/// scheme and host only, and every problem text is fixed.</para>
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

    /// <summary>Most bytes read from a <c>file:</c> reference: a URL is far under this, so a larger file is a mistake
    /// (or a device that never ends), not a secret.</summary>
    internal const int MaxReferenceFileBytes = 8 * 1024;

    /// <summary>How long resolving a reference may take before the tick counts it as a failure.</summary>
    internal static readonly TimeSpan ResolveTimeout = TimeSpan.FromSeconds(10);

    /* Fixed texts: none carries the URL, a variable name, a path, a file's contents or an exception message. The
       problem list reaches /api/ping, the stopped marker and the CLI, so it is shaped for that audience. */
    internal const string UrlShapeProblem =
        "heartbeat.url must be an absolute http or https URL. (The value is not echoed: the URL is a secret.)";
    internal const string UserInfoProblem =
        "heartbeat.url must not carry a user name or password; put the check's token in the path. (The value is not echoed: the URL is a secret.)";
    internal const string EnvShapeProblem =
        "heartbeat.url: an env: reference must name an environment variable. (The value is not echoed.)";
    internal const string FileShapeProblem =
        "heartbeat.url: a file: reference must give an absolute path. (The value is not echoed.)";
    internal const string EnvUnsetProblem =
        "the environment variable named by the heartbeat.url env: reference is not set, or is blank";
    internal const string FileUnreadableProblem =
        "the file named by the heartbeat.url file: reference could not be read";
    internal const string FileTooLargeProblem =
        "the file named by the heartbeat.url file: reference is larger than 8 KB";
    internal const string FileEmptyProblem =
        "the file named by the heartbeat.url file: reference is empty";
    internal const string FileTimeoutProblem =
        "the file named by the heartbeat.url file: reference did not answer within 10 seconds";

    /// <summary>
    /// The SHAPE of a value, nothing more (#5460 ruling): a plain URL is fully validated; an <c>env:</c> or <c>file:</c>
    /// reference is only checked for being well formed (a name, an absolute path) and is NOT resolved, because the
    /// secure setups (a variable in the service's own environment, a file the service account alone can read) are
    /// the ones an operator's shell cannot resolve, and every CLI verb that validates would refuse them. Null when it
    /// is fine; otherwise one of the fixed problem texts.
    /// </summary>
    internal static string? ShapeProblem(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        var text = value.Trim();
        if (!DarlingSecretSource.IsReference(text))
        {
            return TryParseUrl(text, out _);
        }

        if (text.StartsWith("env:", StringComparison.Ordinal))
        {
            return IsWellFormedEnvName(text["env:".Length..].Trim()) ? null : EnvShapeProblem;
        }

        var path = text["file:".Length..].Trim();
        return path.Length > 0 && path.IndexOf('\0', StringComparison.Ordinal) < 0 && Path.IsPathFullyQualified(path)
            ? null
            : FileShapeProblem;
    }

    private static bool IsWellFormedEnvName(string name)
    {
        if (name.Length == 0)
        {
            return false;
        }

        foreach (var c in name)
        {
            if (c == '=' || char.IsWhiteSpace(c) || char.IsControl(c))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Parses a literal URL: absolute, http or https, a host, and no user info. Null problem = fine.</summary>
    private static string? TryParseUrl(string text, out Uri? uri)
    {
        uri = null;
        if (!Uri.TryCreate(text, UriKind.Absolute, out var parsed)
            || (parsed.Scheme != Uri.UriSchemeHttp && parsed.Scheme != Uri.UriSchemeHttps)
            || string.IsNullOrEmpty(parsed.Host))
        {
            return UrlShapeProblem;
        }

        /* HttpClient never turns user info into an Authorization header, so a URL with it would just fail as HTTP 401
           with no hint. Refused up front instead. */
        if (!string.IsNullOrEmpty(parsed.UserInfo))
        {
            return UserInfoProblem;
        }

        uri = parsed;
        return null;
    }

    /// <summary>
    /// Resolves <see cref="Url"/> to an absolute http/https <see cref="Uri"/>, for the loop's tick (and the diagnostics
    /// bundle's alias seed). A failure's problem is one of the fixed texts, never the URL, a name, a path or an exception
    /// message. A <c>file:</c> reference is read asynchronously, capped at <see cref="MaxReferenceFileBytes"/>, and given
    /// <see cref="ResolveTimeout"/>, so a hung share or a device cannot stall a thread or the shutdown. Only
    /// <paramref name="cancellationToken"/> being cancelled throws.
    /// </summary>
    internal async Task<(Uri? Uri, string? Problem)> ResolveAsync(CancellationToken cancellationToken)
    {
        if (!IsConfigured)
        {
            return (null, "heartbeat.url is not set");
        }

        var text = Url!.Trim();
        if (text.StartsWith("env:", StringComparison.Ordinal))
        {
            var name = text["env:".Length..].Trim();
            var value = IsWellFormedEnvName(name) ? Environment.GetEnvironmentVariable(name) : null;
            if (string.IsNullOrWhiteSpace(value))
            {
                return (null, EnvUnsetProblem);
            }

            text = value.Trim();
        }
        else if (text.StartsWith("file:", StringComparison.Ordinal))
        {
            var (contents, problem) = await ReadReferenceFileAsync(text["file:".Length..].Trim(), cancellationToken);
            if (contents is null)
            {
                return (null, problem);
            }

            text = contents;
        }

        var parseProblem = TryParseUrl(text, out var uri);
        return parseProblem is null ? (uri, null) : (null, parseProblem);
    }

    private static async Task<(string? Contents, string? Problem)> ReadReferenceFileAsync(string path, CancellationToken cancellationToken)
    {
        try
        {
            /* The open itself can block (a share that never answers, a pipe), so the whole read runs off the caller's
               thread and the caller waits at most ResolveTimeout. */
            var bytes = await Task.Run(() => ReadCappedAsync(path, cancellationToken), cancellationToken)
                .WaitAsync(ResolveTimeout, cancellationToken);
            if (bytes is null)
            {
                return (null, FileTooLargeProblem);
            }

            var start = bytes.Length >= 3 && bytes[0] == 0xEF && bytes[1] == 0xBB && bytes[2] == 0xBF ? 3 : 0;
            var contents = Encoding.UTF8.GetString(bytes, start, bytes.Length - start).Trim();
            return contents.Length == 0 ? (null, FileEmptyProblem) : (contents, null);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (TimeoutException)
        {
            return (null, FileTimeoutProblem);
        }
        catch (Exception)
        {
            /* Deliberately no type or message here: a path can ride in either, and the problem is fixed text. */
            return (null, FileUnreadableProblem);
        }
    }

    /// <summary>The file's bytes, or null when it holds more than <see cref="MaxReferenceFileBytes"/>.</summary>
    private static async Task<byte[]?> ReadCappedAsync(string path, CancellationToken cancellationToken)
    {
        await using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete,
            bufferSize: 0, FileOptions.Asynchronous);
        var buffer = new byte[MaxReferenceFileBytes + 1];
        var total = 0;
        while (total < buffer.Length)
        {
            var read = await stream.ReadAsync(buffer.AsMemory(total), cancellationToken);
            if (read == 0)
            {
                break;
            }

            total += read;
        }

        return total > MaxReferenceFileBytes ? null : buffer.AsSpan(0, total).ToArray();
    }

    /// <summary>
    /// Validates the block; returns human-readable problems (empty = valid). Fatal, like the rest of
    /// <see cref="DarlingConfig.Validate"/>: a heartbeat that would silently never ping is worse than a refused start.
    /// A null block (<c>"heartbeat": null</c>) is the same as an absent one: off.
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

        var shape = ShapeProblem(heartbeat.Url);
        if (shape is not null)
        {
            problems.Add(shape);
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

    /// <summary>The newest time is further in the future than the clock-skew allowance: no ping (#5460).</summary>
    SkipFuture,
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
    /// <summary>
    /// How far past now a newest collection time may sit and still count as fresh (#5460). A time beyond this is a
    /// skewed clock or a stray row, and trusting it would hold the ping green until wall-clock time caught up.
    /// </summary>
    internal static readonly TimeSpan FutureTolerance = TimeSpan.FromMinutes(5);

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
    private readonly Func<TimeSpan, CancellationToken, Task> _delay;
    private readonly HeartbeatFailureGate _gate = new();
    private readonly HeartbeatFailureGate _tickGate = new();
    private HeartbeatDecision? _loggedSkip;
    private bool _announced;
    private bool _httpWarned;

    internal DarlingHeartbeat(
        ILogger logger, HttpClient? client = null, Func<DateTime>? utcNow = null,
        Func<TimeSpan, CancellationToken, Task>? delay = null)
    {
        _logger = logger;
        _client = client ?? s_client;
        _utcNow = utcNow ?? (() => DateTime.UtcNow);
        _delay = delay ?? ((span, token) => Task.Delay(span, token));
    }

    /// <summary>
    /// The ping decision. Fresh = ping; no enabled server = ping (a service with nothing to collect is up);
    /// stale, or enabled servers with no row at all = skip; a failed read = skip. Fresh means
    /// <c>now - threshold &lt;= newest &lt;= now + <see cref="FutureTolerance"/></c> (#5460): a newest time further in the
    /// future than that is skipped, because for a liveness signal the safe direction on a skewed clock or a stray
    /// future-dated row is silence, not a green ping.
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

        if (newest > nowUtc + FutureTolerance)
        {
            return HeartbeatDecision.SkipFuture;
        }

        return nowUtc - newest <= DarlingSelfAlertEvaluator.CollectionGapAtStartThreshold
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
    /// One interval: resolve the URL, read the store, decide, and ping or skip. Returns what it did (null when the URL
    /// could not be resolved). Throws only for cancellation or a failure of <paramref name="read"/>, which the loop logs.
    /// </summary>
    internal async Task<HeartbeatDecision?> TickAsync(
        HeartbeatConfig config, Func<CancellationToken, Task<HeartbeatRead>> read, CancellationToken cancellationToken)
    {
        /* Resolved on every tick, so a rotated token file is picked up. A failure is fixed text only. */
        var (url, problem) = await config.ResolveAsync(cancellationToken);
        if (url is null)
        {
            /* Counted as a failed ping so it warns once and then hourly, and the external check alerts, which is what
               it should do. */
            LogFailure("the URL could not be resolved: " + problem, null);
            return null;
        }

        if (!_announced)
        {
            _announced = true;
            _logger.LogInformation(
                "Heartbeat on: GET {Scheme}://{Host} every {Seconds} s while collection is current.",
                url.Scheme, url.Host, config.IntervalSeconds);
        }

        if (url.Scheme == Uri.UriSchemeHttp && !_httpWarned)
        {
            _httpWarned = true;
            _logger.LogWarning(
                "Heartbeat URL uses http://, so the check's token crosses the network unencrypted on every ping. Use https:// where the check host offers it.");
        }

        var snapshot = await read(cancellationToken);
        var now = _utcNow();
        var decision = Decide(snapshot, now);
        if (decision != HeartbeatDecision.Ping)
        {
            if (_loggedSkip != decision)
            {
                _loggedSkip = decision;
                _logger.LogWarning(
                    decision switch
                    {
                        HeartbeatDecision.SkipReadFailed =>
                            "Heartbeat ping skipped: the store could not be read. The external check will alert until collection can be confirmed.",
                        HeartbeatDecision.SkipFuture =>
                            "Heartbeat ping skipped: the newest collection time is in the future (a clock jump or a stray row). The external check will alert until it is no longer ahead of the clock.",
                        _ =>
                            "Heartbeat ping skipped: no enabled server has collected within {Minutes} minutes. The external check will alert until collection resumes.",
                    },
                    (int)DarlingSelfAlertEvaluator.CollectionGapAtStartThreshold.TotalMinutes);
            }

            return decision;
        }

        if (_loggedSkip is not null)
        {
            _loggedSkip = null;
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
    /// Returns at once when no URL is set, and a null block counts as unset (#5460). It yields first, so the first tick
    /// (a reference resolve, a store command) never runs on the caller's startup path.
    /// </summary>
    internal Task RunAsync(HeartbeatConfig? config, NpgsqlDataSource store, CancellationToken stoppingToken)
        => RunLoopAsync(config, ct => ReadAsync(store, ct), stoppingToken);

    /// <summary>
    /// The loop over an injected read. Only shutdown ends it: a cancellation that is NOT the stopping token (a driver or
    /// handler giving up internally), and any other exception a tick throws, is logged by type, throttled like a failed
    /// ping, and the loop goes on (#5460).
    /// </summary>
    internal async Task RunLoopAsync(
        HeartbeatConfig? config, Func<CancellationToken, Task<HeartbeatRead>> read, CancellationToken stoppingToken)
    {
        if (config is null || !config.IsConfigured)
        {
            return;
        }

        await Task.Yield();
        var interval = TimeSpan.FromSeconds(config.IntervalSeconds);
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await TickAsync(config, read, stoppingToken);
                _tickGate.OnSuccess();
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                if (_tickGate.OnFailure(_utcNow()) == HeartbeatLogEdge.Warn)
                {
                    _logger.LogWarning(
                        "Heartbeat tick failed ({Reason}). Further failures are logged at most once an hour.", ex.GetType().Name);
                }
            }

            try
            {
                await _delay(interval, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
        }
    }
}
