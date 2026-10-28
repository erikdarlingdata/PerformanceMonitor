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
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using ModelContextProtocol.Protocol;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.FrontDoor;

/// <summary>
/// The front door's view of which store holds which server (#6840). It reads <c>list_servers</c> from every store on a timer and
/// keeps the last good answer of each, so a store that goes quiet is still routable by its last known list and says so
/// (<see cref="FrontDoorStoreHealth.Stale"/>), and a store that has never answered says that instead
/// (<see cref="FrontDoorStoreHealth.Unreachable"/>).
///
/// <para>A store that refuses its token (401 or 403) or answers 429 is held: no call goes to it until the hold ends. A store's
/// bearer check counts every wrong token toward a hold that also locks out that store's other clients, so a wrong token sent on
/// every call would cost the store its other callers. The hold is at least <see cref="MinimumHold"/>, longer when the store's
/// Retry-After says so.</para>
/// </summary>
internal sealed class DarlingFrontDoorDirectory : IFrontDoorDirectory, IDisposable
{
    /// <summary>The shortest hold after a refused token: five minutes, the time a wrong token takes to stop counting at the store.</summary>
    internal static readonly TimeSpan MinimumHold = TimeSpan.FromSeconds(300);

    /// <summary>The shortest gap between two refreshes that a missed server name asks for.</summary>
    internal static readonly TimeSpan MissRefreshGap = TimeSpan.FromSeconds(30);

    /// <summary>How long one store may take to answer <c>list_servers</c>.</summary>
    internal static readonly TimeSpan PeerTimeout = TimeSpan.FromSeconds(30);

    private readonly IReadOnlyList<FrontDoorStore> _configured;
    private readonly IFrontDoorMcpPeers _peers;
    private readonly ILogger _logger;
    private readonly TimeProvider _clock;
    private readonly object _gate = new();
    private readonly Dictionary<string, Entry> _entries = new(StringComparer.OrdinalIgnoreCase);
    private readonly SemaphoreSlim _refreshing = new(1, 1);
    private DateTimeOffset _lastMissRefresh = DateTimeOffset.MinValue;

    public DarlingFrontDoorDirectory(
        FrontDoorConfig config, IFrontDoorMcpPeers peers, ILogger<DarlingFrontDoorDirectory>? logger = null, TimeProvider? clock = null)
    {
        _configured = config.ToStores();
        _peers = peers;
        _logger = (ILogger?)logger ?? NullLogger.Instance;
        _clock = clock ?? TimeProvider.System;
        RefreshInterval = config.EffectiveRefreshInterval();
        Active = config.Enabled && _configured.Count > 0;
        foreach (var store in _configured)
        {
            _entries[store.Name] = new Entry();
        }
    }

    public void Dispose() => _refreshing.Dispose();

    /// <summary>True when front-door mode is on and at least one store is configured: the only time the refresh loop runs.</summary>
    internal bool Active { get; }

    /// <summary>How long the refresh loop waits between refreshes.</summary>
    internal TimeSpan RefreshInterval { get; }

    public IReadOnlyList<FrontDoorStoreState> Stores
    {
        get
        {
            var now = _clock.GetUtcNow();
            lock (_gate)
            {
                return _configured.Select(store => Snapshot(store, _entries[store.Name], now)).ToList();
            }
        }
    }

    public FrontDoorStore? Find(string name)
        => _configured.FirstOrDefault(s => string.Equals(s.Name, name?.Trim(), StringComparison.OrdinalIgnoreCase));

    public void Hold(string store, TimeSpan span, string why)
    {
        var found = Find(store);
        if (found is null)
        {
            return;
        }

        var until = _clock.GetUtcNow() + (span > MinimumHold ? span : MinimumHold);
        bool extended;
        lock (_gate)
        {
            var entry = _entries[found.Name];
            extended = entry.HeldUntil is null || until > entry.HeldUntil;
            if (extended)
            {
                entry.HeldUntil = until;
                entry.Error = why;
            }
        }

        if (extended)
        {
            /* The store name and the reason, never a token or an address. */
            _logger.LogWarning(
                "Front door: store {Store} is held until {Until:HH:mm:ss} UTC and gets no calls until then ({Why}).",
                found.Name, until, why);
        }
    }

    /// <summary>Reads <c>list_servers</c> from every store that is not held, in parallel, each with its own 30 second limit. A store
    /// that fails keeps its last good list. Refreshes never overlap: a second caller waits for the one running.</summary>
    internal async Task RefreshAsync(CancellationToken ct)
    {
        await _refreshing.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var now = _clock.GetUtcNow();
            List<FrontDoorStore> due;
            lock (_gate)
            {
                due = _configured.Where(s => !IsHeld(_entries[s.Name], now)).ToList();
            }

            await Task.WhenAll(due.Select(store => RefreshOneAsync(store, ct))).ConfigureAwait(false);
        }
        finally
        {
            _refreshing.Release();
        }
    }

    public async Task RefreshOnMissAsync(CancellationToken ct)
    {
        var now = _clock.GetUtcNow();
        lock (_gate)
        {
            if (now - _lastMissRefresh < MissRefreshGap)
            {
                return;
            }

            _lastMissRefresh = now;
        }

        await RefreshAsync(ct).ConfigureAwait(false);
    }

    public FrontDoorRoute ResolveServer(string serverName)
    {
        var name = (serverName ?? string.Empty).Trim();
        if (name == "*")
        {
            return new FrontDoorRoute.FanOut();
        }

        var states = Stores;
        if (name.Length == 0)
        {
            return new FrontDoorRoute.Refused("A server name is needed.");
        }

        /* Each store matches with the rule a single store uses for its own reads. Exact beats partial across stores too: a name
           that is one store's server exactly is not made ambiguous by another store's longer name that merely contains it. */
        var matches = states
            .Select(s => (State: s, Match: MatchIn(s.Servers, name)))
            .Where(m => m.Match.Candidates.Count > 0)
            .ToList();
        var tier = matches.Any(m => m.Match.MatchedBy == "exact") ? "exact" : "partial";
        var hits = matches.Where(m => m.Match.MatchedBy == tier).ToList();

        if (hits.Count == 1)
        {
            return new FrontDoorRoute.ToStore(hits[0].State.Store.Name);
        }

        if (hits.Count > 1)
        {
            var text = new StringBuilder();
            text.Append('\'').Append(name).Append("' matches servers in ").Append(hits.Count).Append(" stores")
                .Append(tier == "exact" ? " (the name is the same in each)" : " (as part of their names)")
                .Append(". Pass the store argument with one of these store names, or use a server's full name:");
            foreach (var hit in hits)
            {
                text.Append("\nstore ").Append(hit.State.Store.Name).Append(": ")
                    .Append(string.Join(", ", hit.Match.Candidates.Select(Describe)));
            }

            return new FrontDoorRoute.Refused(text.ToString(), Ambiguous: true, Stores: hits.Select(h => h.State.Store.Name).ToList());
        }

        var listing = string.Join("; ", states.Select(s =>
            s.Store.Name + " (" + s.Servers.Count.ToString(CultureInfo.InvariantCulture) + " servers"
            + (s.Health == FrontDoorStoreHealth.Ok ? string.Empty : ", " + s.Health.ToString().ToLowerInvariant()) + ")"));
        return new FrontDoorRoute.Refused(
            $"'{name}' matches no server in any store. Stores: {(listing.Length == 0 ? "none configured" : listing)}. "
            + "A store that is stale, unreachable or held is searched by its last known list, or not at all.");
    }

    /// <summary>One store's servers against a name, with the rule a single store uses for its own reads. The web proxy matches through
    /// this too, so the front door has one rule for what a name means.</summary>
    internal static ServerNameMatch MatchIn(IReadOnlyList<RegisteredServer> servers, string name)
        => ServerNameMatcher.Match(servers, name, storageNameIgnoresCase: true, nameClash: NameClash.StoredNameWins);

    private static string Describe(RegisteredServer s)
        => string.IsNullOrEmpty(s.DisplayName) || s.DisplayName == s.ServerName ? s.ServerName : $"{s.ServerName} ({s.DisplayName})";

    private async Task RefreshOneAsync(FrontDoorStore store, CancellationToken ct)
    {
        try
        {
            using var limit = CancellationTokenSource.CreateLinkedTokenSource(ct);
            limit.CancelAfter(PeerTimeout);
            var call = new CallToolRequestParams { Name = "list_servers" };
            CallToolResult result;
            try
            {
                result = await _peers.CallAsync(store.Name, call, agentRole: false, limit.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                throw new FrontDoorPeerException(store.Name, FrontDoorPeerFailure.TimedOut, "did not answer list_servers in time");
            }

            var servers = ParseServers(result);
            lock (_gate)
            {
                var entry = _entries[store.Name];
                var recovered = entry.Failed;
                entry.Servers = servers;
                entry.HasGoodList = true;
                entry.Failed = false;
                entry.Error = null;
                entry.LastOkUtc = _clock.GetUtcNow().UtcDateTime;
                if (recovered)
                {
                    _logger.LogInformation("Front door: store {Store} answers again ({Count} servers).", store.Name, servers.Count);
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            /* A refusal already put the hold on through the peer client; Hold again is harmless and covers a client that did not. */
            if (ex is FrontDoorPeerException { Kind: FrontDoorPeerFailure.AuthFailed })
            {
                Hold(store.Name, MinimumHold, ex.Message);
            }

            var message = ex is FrontDoorPeerException ? ex.Message : ex.GetType().Name;
            lock (_gate)
            {
                var entry = _entries[store.Name];
                var first = !entry.Failed;
                entry.Failed = true;
                entry.Error = message;
                if (first)
                {
                    _logger.LogWarning(
                        "Front door: store {Store} did not answer list_servers ({Message}). It keeps its last known list.",
                        store.Name, message);
                }
            }
        }
    }

    private static List<RegisteredServer> ParseServers(CallToolResult result)
    {
        if (result.IsError == true)
        {
            throw new InvalidOperationException("the store answered list_servers with an error");
        }

        var text = result.Content.OfType<TextContentBlock>().FirstOrDefault()?.Text
            ?? throw new InvalidOperationException("the store's list_servers answer had no text");
        using var doc = JsonDocument.Parse(text);
        if (doc.RootElement.ValueKind != JsonValueKind.Object
            || !doc.RootElement.TryGetProperty("servers", out var array)
            || array.ValueKind != JsonValueKind.Array)
        {
            throw new InvalidOperationException("the store's list_servers answer has no servers list");
        }

        var rows = new List<RegisteredServer>();
        foreach (var row in array.EnumerateArray())
        {
            if (row.ValueKind == JsonValueKind.Object
                && row.TryGetProperty("server_name", out var nameElement)
                && nameElement.ValueKind == JsonValueKind.String
                && nameElement.GetString() is { Length: > 0 } serverName)
            {
                var display = row.TryGetProperty("display_name", out var d) && d.ValueKind == JsonValueKind.String ? d.GetString() : null;
                rows.Add(new RegisteredServer(rows.Count + 1, serverName, display));
            }
        }

        return rows;
    }

    private static bool IsHeld(Entry entry, DateTimeOffset now) => entry.HeldUntil is { } until && until > now;

    private static FrontDoorStoreState Snapshot(FrontDoorStore store, Entry entry, DateTimeOffset now)
    {
        var health = IsHeld(entry, now)
            ? FrontDoorStoreHealth.AuthFailed
            : entry.Failed || !entry.HasGoodList
                ? (entry.HasGoodList ? FrontDoorStoreHealth.Stale : FrontDoorStoreHealth.Unreachable)
                : FrontDoorStoreHealth.Ok;
        return new FrontDoorStoreState(store, health, entry.Servers, entry.LastOkUtc, entry.Error);
    }

    private sealed class Entry
    {
        public IReadOnlyList<RegisteredServer> Servers = Array.Empty<RegisteredServer>();
        public bool HasGoodList;
        public bool Failed;
        public DateTime? LastOkUtc;
        public string? Error;
        public DateTimeOffset? HeldUntil;
    }
}
