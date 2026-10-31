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
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using ModelContextProtocol;
using ModelContextProtocol.Client;
using ModelContextProtocol.Protocol;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;

namespace PerformanceMonitor.Darling.Service.FrontDoor;

/// <summary>Why a store could not be asked.</summary>
internal enum FrontDoorPeerFailure
{
    /// <summary>The store did not answer: it is down, or the connection broke.</summary>
    Unreachable,

    /// <summary>The store refused the token (401 or 403) or answered 429, or it is already held for that. Nothing was sent while held.</summary>
    AuthFailed,

    /// <summary>The front door holds no token for that store and role, so nothing was sent. An agent-role caller whose store has no
    /// agent token is refused this way: there is no fallback to the operator token.</summary>
    NoToken,

    /// <summary>The store did not answer within <c>peerTimeoutSeconds</c>.</summary>
    TimedOut,
}

/// <summary>
/// A store that cannot be asked (#6840). <see cref="Exception.Message"/> says which store and why in words safe to show a caller:
/// it never carries a token, a header or an address. A caller that cancels gets <see cref="OperationCanceledException"/> instead,
/// and a store that answers with a protocol error (an unknown tool, say) gets that error passed through unchanged.
/// </summary>
internal sealed class FrontDoorPeerException : Exception
{
    public FrontDoorPeerException(string store, FrontDoorPeerFailure kind, string message)
        : base($"Store {store}: {message}")
    {
        Store = store;
        Kind = kind;
    }

    /// <summary>The configured store name.</summary>
    public string Store { get; }

    /// <summary>Why the store could not be asked.</summary>
    public FrontDoorPeerFailure Kind { get; }
}

/// <summary>
/// The front door's calls to its stores' MCP endpoints (#6840): one long-lived <see cref="McpClient"/> per store and role over
/// Streamable HTTP, carrying that role's bearer token. A call goes out with the caller's cancellation linked to
/// <c>peerTimeoutSeconds</c>, so a caller that disconnects cancels the HTTP request to the store and the store's read stops.
///
/// <para>A busy answer is a tool result like any other and is passed back: nothing here waits or retries on it. A 401, 403 or
/// 429 at the HTTP level puts the store on hold in the directory and is thrown as <see cref="FrontDoorPeerFailure.AuthFailed"/>;
/// while a store is held no call goes out. A call is retried once on a rebuilt connection only when the failure happened before
/// anything reached the store (the connection was refused or could not be made), so a read or a write never runs twice.</para>
/// </summary>
internal sealed class DarlingFrontDoorMcpPeers : IFrontDoorMcpPeers, IAsyncDisposable
{
    private readonly IFrontDoorSecrets _secrets;
    private readonly Func<IFrontDoorDirectory> _directory;
    private readonly ILogger _logger;
    private readonly TimeSpan _callTimeout;
    private readonly Dictionary<string, Link> _links;

    public DarlingFrontDoorMcpPeers(
        FrontDoorConfig config, IFrontDoorSecrets secrets, Func<IFrontDoorDirectory> directory,
        ILogger<DarlingFrontDoorMcpPeers>? logger = null,
        Func<FrontDoorStore, HttpMessageHandler>? innerHandler = null)
    {
        _secrets = secrets;
        _directory = directory;
        _logger = (ILogger?)logger ?? NullLogger.Instance;
        _callTimeout = config.EffectivePeerTimeout();
        _links = config.ToStores().ToDictionary(s => s.Name, s => new Link(s, innerHandler), StringComparer.OrdinalIgnoreCase);
    }

    public async Task<CallToolResult> CallAsync(string store, CallToolRequestParams call, bool agentRole, CancellationToken ct, McpDataDetail detail = McpDataDetail.Full)
    {
        if (!_links.TryGetValue(store?.Trim() ?? string.Empty, out var link))
        {
            throw new FrontDoorPeerException(store ?? string.Empty, FrontDoorPeerFailure.Unreachable, "is not a configured store.");
        }

        var name = link.Store.Name;
        var token = _secrets.McpToken(name, agentRole);
        if (string.IsNullOrEmpty(token))
        {
            throw new FrontDoorPeerException(
                name, FrontDoorPeerFailure.NoToken,
                agentRole
                    ? "has no agent-role token configured, so an agent-role caller is not forwarded to it."
                    : "has no token configured, so it is not called.");
        }

        if (_directory().Stores.FirstOrDefault(s => string.Equals(s.Store.Name, name, StringComparison.OrdinalIgnoreCase))
                is { Health: FrontDoorStoreHealth.AuthFailed })
        {
            throw new FrontDoorPeerException(
                name, FrontDoorPeerFailure.AuthFailed, "refused the front door's token or asked it to wait, and is held. No call was sent.");
        }

        using var limit = CancellationTokenSource.CreateLinkedTokenSource(ct);
        limit.CancelAfter(_callTimeout);

        for (var attempt = 0; ; attempt++)
        {
            var refusalsBefore = link.Recorder.RefusalCount;
            McpClient? client = null;
            try
            {
                client = await link.GetClientAsync(agentRole, detail, token, limit.Token).ConfigureAwait(false);
                return await client.CallToolAsync(call, limit.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (OperationCanceledException)
            {
                throw new FrontDoorPeerException(
                    name, FrontDoorPeerFailure.TimedOut, $"did not answer within {(int)_callTimeout.TotalSeconds} seconds.");
            }
            catch (McpProtocolException) when (client is not null && link.Recorder.RefusalCount == refusalsBefore)
            {
                /* The store answered with a JSON-RPC error (an unknown tool, bad arguments): it is reachable and the connection is fine. */
                throw;
            }
            catch (Exception ex)
            {
                link.Discard(agentRole, detail, client);

                if (link.Recorder.RefusalSince(refusalsBefore) is { } refusal)
                {
                    var why = $"answered {(int)refusal.Status}";
                    _directory().Hold(name, refusal.RetryAfter ?? TimeSpan.Zero, why);
                    throw new FrontDoorPeerException(
                        name, FrontDoorPeerFailure.AuthFailed,
                        $"refused the front door's token or asked it to wait (HTTP {(int)refusal.Status}). It is held and gets no calls for a while.");
                }

                if (attempt == 0 && IsBeforeAnythingSent(ex))
                {
                    continue;
                }

                _logger.LogWarning(
                    "Front door: a call to store {Store} failed ({Type}: {Message}).", name, ex.GetType().Name, ex.Message);
                throw new FrontDoorPeerException(
                    name, FrontDoorPeerFailure.Unreachable, $"could not be reached ({ex.GetType().Name}).");
            }
        }
    }

    /// <summary>True for a failure that happened while making the connection: the request was never written, so the call is safe to send again.</summary>
    private static bool IsBeforeAnythingSent(Exception ex)
    {
        for (var e = ex; e is not null; e = e.InnerException)
        {
            if (e is HttpRequestException
                {
                    HttpRequestError: HttpRequestError.ConnectionError or HttpRequestError.NameResolutionError
                        or HttpRequestError.SecureConnectionError
                })
            {
                return true;
            }
        }

        return false;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var link in _links.Values)
        {
            await link.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>One store: its HTTP client, and one MCP client per role and caller data detail level (#7023: the level is a header of the
    /// client, so a caller at one level never rides another's connection; at most six clients per store).</summary>
    private sealed class Link : IAsyncDisposable
    {
        private readonly HttpClient _http;
        private readonly Dictionary<(bool Agent, McpDataDetail Detail), McpClient> _clients = new();
        private readonly SemaphoreSlim _gate = new(1, 1);

        public Link(FrontDoorStore store, Func<FrontDoorStore, HttpMessageHandler>? innerHandler)
        {
            Store = store;
            Recorder = new RefusalRecorder(innerHandler?.Invoke(store) ?? DarlingFrontDoorHttp.CreateHandler(store, TimeSpan.FromSeconds(10)));
            /* No overall HttpClient timeout: the SSE reply of a long read has no fixed length, and each call carries its own deadline. */
            _http = new HttpClient(Recorder, disposeHandler: true) { Timeout = Timeout.InfiniteTimeSpan };
        }

        public FrontDoorStore Store { get; }

        public RefusalRecorder Recorder { get; }

        public async Task<McpClient> GetClientAsync(bool agentRole, McpDataDetail detail, string token, CancellationToken ct)
        {
            lock (_clients)
            {
                if (_clients.TryGetValue((agentRole, detail), out var existing) && !existing.Completion.IsCompleted)
                {
                    return existing;
                }
            }

            await _gate.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                McpClient? stale = null;
                lock (_clients)
                {
                    if (_clients.TryGetValue((agentRole, detail), out var existing))
                    {
                        if (!existing.Completion.IsCompleted)
                        {
                            return existing;
                        }

                        stale = existing;
                        _clients.Remove((agentRole, detail));
                    }
                }

                if (stale is not null)
                {
                    await stale.DisposeAsync().ConfigureAwait(false);
                }

                var transport = new HttpClientTransport(
                    new HttpClientTransportOptions
                    {
                        Endpoint = Store.McpUrl,
                        Name = "front-door:" + Store.Name,
                        TransportMode = HttpTransportMode.StreamableHttp,
                        AdditionalHeaders = HeadersFor(token, detail),
                        /* The store answers statelessly, so there is nothing to push: do not hold a second stream open. */
                        EnableStandaloneGetStream = false,
                    },
                    _http,
                    loggerFactory: null,
                    ownsHttpClient: false);

                var client = await McpClient.CreateAsync(
                    transport,
                    new McpClientOptions { ClientInfo = new Implementation { Name = "PerformanceMonitorDarlingFrontDoor", Version = "1.0.0" } },
                    loggerFactory: null,
                    ct).ConfigureAwait(false);
                lock (_clients)
                {
                    _clients[(agentRole, detail)] = client;
                }

                return client;
            }
            finally
            {
                _gate.Release();
            }
        }

        /// <summary>The headers of a store client: the token, and below <c>full</c> the caller's level (#7023), which the store reads as a
        /// narrowing of its own level for the calls on this connection.</summary>
        private static Dictionary<string, string> HeadersFor(string token, McpDataDetail detail)
        {
            var headers = new Dictionary<string, string> { ["Authorization"] = "Bearer " + token };
            if (detail != McpDataDetail.Full)
            {
                headers[DarlingMcpTokenSet.DataDetailHeader] = McpDataDetailModes.Word(detail);
            }

            return headers;
        }

        /// <summary>Drops a client after a failure so the next call builds a new one.</summary>
        public void Discard(bool agentRole, McpDataDetail detail, McpClient? client)
        {
            if (client is null)
            {
                return;
            }

            lock (_clients)
            {
                if (_clients.TryGetValue((agentRole, detail), out var current) && ReferenceEquals(current, client))
                {
                    _clients.Remove((agentRole, detail));
                }
            }

            _ = Task.Run(async () =>
            {
                try
                {
                    await client.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception)
                {
                    /* The client is already broken: there is nothing more to do with it. */
                }
            });
        }

        public async ValueTask DisposeAsync()
        {
            McpClient[] clients;
            lock (_clients)
            {
                clients = _clients.Values.ToArray();
                _clients.Clear();
            }

            foreach (var client in clients)
            {
                try
                {
                    await client.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception)
                {
                    /* Shutting down: a client that will not close cleanly is dropped. */
                }
            }

            _http.Dispose();
            _gate.Dispose();
        }
    }

    /// <summary>Notes each 401, 403 and 429 a store answers, with its Retry-After, because the MCP client turns the status into an
    /// exception that no longer carries the headers.</summary>
    private sealed class RefusalRecorder : DelegatingHandler
    {
        private long _count;
        private (HttpStatusCode Status, TimeSpan? RetryAfter)? _last;

        public RefusalRecorder(HttpMessageHandler inner)
            : base(inner)
        {
        }

        public long RefusalCount => Interlocked.Read(ref _count);

        /// <summary>The newest refusal recorded after the call started (when the count was <paramref name="countBefore"/>), or null.</summary>
        public (HttpStatusCode Status, TimeSpan? RetryAfter)? RefusalSince(long countBefore)
        {
            lock (this)
            {
                return Interlocked.Read(ref _count) > countBefore ? _last : null;
            }
        }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var response = await base.SendAsync(request, cancellationToken).ConfigureAwait(false);
            if (response.StatusCode is HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden or HttpStatusCode.TooManyRequests)
            {
                var retry = response.Headers.RetryAfter;
                TimeSpan? after = retry?.Delta ?? (retry?.Date is { } date ? date - DateTimeOffset.UtcNow : null);
                lock (this)
                {
                    _last = (response.StatusCode, after);
                    Interlocked.Increment(ref _count);
                }
            }

            return response;
        }
    }
}
