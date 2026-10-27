/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service.Mcp;

namespace PerformanceMonitor.Darling.Service.FrontDoor;

/// <summary>
/// A store could not be reached or would not sign the front door in (#6840). The text names the store and what happened and never a
/// token, so the web proxy can put it in an answer to the browser as it is.
/// </summary>
internal sealed class FrontDoorPeerUnavailableException : Exception
{
    internal FrontDoorPeerUnavailableException(string store, string message)
        : base(message)
    {
        Store = store;
    }

    /// <summary>The configured name of the store.</summary>
    internal string Store { get; }
}

/// <summary>
/// The front door's web session with each store (#6840). A store's web dashboard takes its token only from a form POST to its
/// <c>/auth/login</c> and then recognizes the session cookie it hands back, so this signs in once per store with the store's web
/// token, keeps that cookie in memory (it never reaches a browser) and reuses it for as long as the store's own cookie lives.
///
/// <para>One sign-in per store runs at a time: requests that arrive while it is in flight wait for its answer instead of posting
/// the token again. A sign-in that fails (wrong token, a refusal, the store down or slow) holds the store for
/// <see cref="FailedSignInHold"/>, or for the store's own Retry-After when that is longer, so a wrong token is not posted on every
/// request and does not feed the store's sign-in wait.</para>
///
/// <para>Each store has its own <see cref="HttpClient"/> on <see cref="DarlingFrontDoorHttp.CreateHandler"/>: one store's cookie is
/// only ever sent to that store's address, and the handler keeps no cookies of its own.</para>
/// </summary>
internal sealed class DarlingFrontDoorWebSession : IDisposable
{
    /// <summary>How long a store whose sign-in failed is left alone.</summary>
    internal static readonly TimeSpan FailedSignInHold = TimeSpan.FromSeconds(60);

    /// <summary>How long one sign-in POST may take.</summary>
    internal static readonly TimeSpan SignInTimeout = TimeSpan.FromSeconds(30);

    /// <summary>Connecting to a store gives up after this long.</summary>
    internal static readonly TimeSpan ConnectTimeout = TimeSpan.FromSeconds(10);

    /// <summary>How long a cookie is reused: the store's own session lifetime less a margin, so a request does not start on a cookie
    /// that is about to lapse. A store that restarted, or that runs a shorter lifetime, answers 401 and the proxy signs in again.</summary>
    internal static readonly TimeSpan CookieReuse = DarlingWebHostService.SessionLifetime - TimeSpan.FromMinutes(5);

    private readonly IFrontDoorSecrets _secrets;
    private readonly TimeProvider _time;
    private readonly ConcurrentDictionary<string, PeerState> _peers = new(StringComparer.OrdinalIgnoreCase);

    internal DarlingFrontDoorWebSession(IFrontDoorSecrets secrets, TimeProvider? time = null)
    {
        _secrets = secrets;
        _time = time ?? TimeProvider.System;
    }

    private sealed record Cookie(string Pair, DateTimeOffset ExpiresAt);

    private sealed class PeerState : IDisposable
    {
        internal PeerState(FrontDoorStore store)
        {
            Client = new HttpClient(DarlingFrontDoorHttp.CreateHandler(store, ConnectTimeout), disposeHandler: true)
            {
                /* The proxy's own token bounds each call (the page closing, and the peer timeout); the client's fixed timeout would
                   cut a long read at 100 seconds whatever the config says. */
                Timeout = Timeout.InfiniteTimeSpan,
            };
        }

        internal HttpClient Client { get; }

        internal SemaphoreSlim SignInGate { get; } = new(1, 1);

        internal Cookie? Current;

        internal DateTimeOffset HeldUntil;

        internal string? HeldWhy;

        public void Dispose() => Client.Dispose();
    }

    private PeerState StateFor(FrontDoorStore store)
        => _peers.GetOrAdd(store.Name, _ => new PeerState(store));

    /// <summary>The client for one store. Every call to that store goes through it.</summary>
    internal HttpClient ClientFor(FrontDoorStore store) => StateFor(store).Client;

    /// <summary>The cookie to send to the store, signing in first when there is none or it has lapsed. Throws
    /// <see cref="FrontDoorPeerUnavailableException"/> when the store is held or the sign-in fails. A cancelled caller is not a failure and
    /// does not hold the store.</summary>
    internal async Task<string> GetCookieAsync(FrontDoorStore store, CancellationToken ct)
    {
        var state = StateFor(store);
        if (Fresh(state) is { } cookie)
        {
            return cookie.Pair;
        }

        await state.SignInGate.WaitAsync(ct);
        try
        {
            if (Fresh(state) is { } signedIn)
            {
                return signedIn.Pair;
            }

            var now = _time.GetUtcNow();
            if (state.HeldUntil > now)
            {
                throw new FrontDoorPeerUnavailableException(
                    store.Name,
                    $"Store '{store.Name}' did not accept the front door's sign-in a moment ago ({state.HeldWhy}). It is left alone for another "
                    + $"{Math.Max(1, (int)Math.Ceiling((state.HeldUntil - now).TotalSeconds))} seconds.");
            }

            var fresh = await SignInAsync(store, state, ct);
            Volatile.Write(ref state.Current, fresh);
            return fresh.Pair;
        }
        finally
        {
            state.SignInGate.Release();
        }
    }

    /// <summary>Forgets <paramref name="cookiePair"/> after the store answered 401 to it. Only that cookie is dropped: a request that
    /// was already holding a newer one does not lose it to a late 401.</summary>
    internal void Discard(FrontDoorStore store, string cookiePair)
    {
        var state = StateFor(store);
        var current = Volatile.Read(ref state.Current);
        if (current is not null && string.Equals(current.Pair, cookiePair, StringComparison.Ordinal))
        {
            Interlocked.CompareExchange(ref state.Current, null, current);
        }
    }

    private Cookie? Fresh(PeerState state)
        => Volatile.Read(ref state.Current) is { } current && current.ExpiresAt > _time.GetUtcNow() ? current : null;

    private async Task<Cookie> SignInAsync(FrontDoorStore store, PeerState state, CancellationToken ct)
    {
        if (store.WebUrl is null)
        {
            throw new FrontDoorPeerUnavailableException(
                store.Name, $"Store '{store.Name}' has no web address in the front door's configuration.");
        }

        var token = _secrets.WebToken(store.Name);
        if (string.IsNullOrEmpty(token))
        {
            throw new FrontDoorPeerUnavailableException(
                store.Name, $"Store '{store.Name}' has no web token in the front door's configuration, so the front door cannot sign in to it.");
        }

        /* No Origin, Referer or Sec-Fetch-Site: the store's sign-in judges a POST that names none of them by its own rules, and a
           value copied from a browser would name the front door's host, which the store refuses as another site. */
        using var request = new HttpRequestMessage(HttpMethod.Post, DarlingFrontDoorWebProxy.PeerUri(store.WebUrl, DarlingWebHostService.LoginPath, null))
        {
            Content = new FormUrlEncodedContent(new[]
            {
                new KeyValuePair<string, string>("token", token),
                new KeyValuePair<string, string>("return", "/"),
            }),
        };

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(SignInTimeout);
        try
        {
            using var response = await state.Client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, timeout.Token);

            if (ReadSessionCookie(response) is { } pair)
            {
                return new Cookie(pair, _time.GetUtcNow() + CookieReuse);
            }

            var status = (int)response.StatusCode;
            var hold = FailedSignInHold;
            if (response.Headers.RetryAfter?.Delta is { } wait && wait > hold)
            {
                hold = wait;
            }

            var why = status switch
            {
                429 => "the store is holding sign-ins and answered 429",
                403 => "the store refused the sign-in with 403 (check allowFrom, and that the web token is not the MCP token)",
                200 => "the store answered its login page, so it did not accept the web token",
                _ => $"the store answered HTTP {status} to the sign-in",
            };
            throw Hold(store, state, hold, why);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            throw Hold(store, state, FailedSignInHold, $"no answer within {(int)SignInTimeout.TotalSeconds} seconds");
        }
        catch (HttpRequestException ex)
        {
            throw Hold(store, state, FailedSignInHold, DarlingFrontDoorWebProxy.DescribeConnectFailure(ex));
        }
    }

    private FrontDoorPeerUnavailableException Hold(FrontDoorStore store, PeerState state, TimeSpan span, string why)
    {
        state.HeldUntil = _time.GetUtcNow() + span;
        state.HeldWhy = why;
        return new FrontDoorPeerUnavailableException(
            store.Name, $"Store '{store.Name}' did not accept the front door's sign-in: {why}. It is left alone for {(int)Math.Ceiling(span.TotalSeconds)} seconds.");
    }

    /// <summary>The <c>name=value</c> pair of the store's session cookie from a sign-in answer, or null when it set none.</summary>
    private static string? ReadSessionCookie(HttpResponseMessage response)
    {
        if (!response.Headers.TryGetValues("Set-Cookie", out var values))
        {
            return null;
        }

        var prefix = DarlingWebHostService.SessionCookieName + "=";
        foreach (var header in values)
        {
            if (header.StartsWith(prefix, StringComparison.Ordinal))
            {
                var end = header.IndexOf(';', StringComparison.Ordinal);
                var pair = end < 0 ? header : header[..end];
                return pair.Length > prefix.Length ? pair : null;
            }
        }

        return null;
    }

    public void Dispose()
    {
        foreach (var peer in _peers.Values.ToList())
        {
            peer.Dispose();
        }
    }
}
