/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

#nullable enable

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service.Hosting;

namespace Darling.Tests.FrontDoor;

/// <summary>
/// A store's web dashboard, reduced to what the front door's web proxy talks to (#6840): a Kestrel app on 127.0.0.1 with a free port.
/// <c>POST /auth/login</c> takes a url-encoded <c>token</c> and answers 302 with a session cookie (a wrong token gets 200 and no
/// cookie), and every <c>/api/*</c> call records what it was sent and answers canned JSON, but only to a request holding a cookie the
/// peer issued: any other gets 401 as the real dashboard's gate does. Each answer also carries a <c>Set-Cookie</c>, a
/// <c>Content-Security-Policy</c> and an <c>X-Peer-Marker</c> header, so a test can see whether the proxy let a store's own headers
/// through to the browser. The knobs below make it slow, rate-limited or forgetful about its sessions.
/// </summary>
internal sealed class FakeWebPeer : IAsyncDisposable
{
    internal const string SessionCookie = "darling_web_session";
    internal const string PeerCsp = "default-src 'none'; peer-marker";

    private readonly WebApplication _app;
    private readonly HashSet<string> _valid = new(StringComparer.Ordinal);
    private int _issued;

    private FakeWebPeer(string name, string token, WebApplication app, Uri baseUri)
    {
        Name = name;
        Token = token;
        _app = app;
        BaseUri = baseUri;
    }

    /// <summary>What one request carried.</summary>
    internal sealed record Seen(string Method, string Path, string Query, IReadOnlyDictionary<string, string> Headers, string Body);

    internal string Name { get; }

    /// <summary>The web token the peer's sign-in accepts.</summary>
    internal string Token { get; }

    internal Uri BaseUri { get; }

    /// <summary>Every <c>/api/*</c> request that arrived, signed in or not.</summary>
    internal ConcurrentQueue<Seen> Api { get; } = new();

    /// <summary>Every <c>POST /auth/login</c> that arrived.</summary>
    internal ConcurrentQueue<Seen> Logins { get; } = new();

    /// <summary>Sign-ins that handed out a cookie.</summary>
    internal int SignIns => Volatile.Read(ref _issued);

    /// <summary>The next this-many <c>/api/*</c> calls answer 401 and forget the cookie they carried, as a restarted peer would.</summary>
    internal int UnauthorizedNext;

    /// <summary>Every <c>/api/*</c> call answers 401, whatever the cookie.</summary>
    internal bool AlwaysUnauthorized;

    /// <summary>How long an <c>/api/*</c> call takes before it answers.</summary>
    internal TimeSpan Delay;

    /// <summary>When set, every signed-in <c>/api/*</c> call answers 429 with this <c>Retry-After</c>.</summary>
    internal int? RateLimitSeconds;

    /// <summary>When set, the sign-in answers 429 with this <c>Retry-After</c> instead of looking at the token.</summary>
    internal int? SignInRateLimitSeconds;

    /// <summary>When set, every signed-in <c>/api/*</c> call answers a redirect to this address instead of its answer.</summary>
    internal string? RedirectTo;

    /// <summary>When set, the peer acts as a build without the front door's role header (#6840): it never confirms it.</summary>
    internal bool OlderBuild;

    /// <summary>When set, every signed-in <c>/api/*</c> call answers this <c>Content-Type</c> instead of JSON.</summary>
    internal string? AnswerContentType;

    /// <summary>Completes when a call reaches the delay.</summary>
    internal TaskCompletionSource ApiStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Completes when the peer sees the front door drop a call it was still holding.</summary>
    internal TaskCompletionSource ApiAborted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    internal static async Task<FakeWebPeer> StartAsync(string name, string token = "fake-peer-web-token-0123456789abcdef")
    {
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions { EnvironmentName = Environments.Production });
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(options => options.Listen(IPAddress.Loopback, 0));
        var app = builder.Build();

        FakeWebPeer? peer = null;
        app.MapPost("/auth/login", async (HttpContext context) => await peer!.LoginAsync(context));
        app.MapMethods("/api/{**rest}", new[] { "GET", "POST", "PUT", "PATCH", "DELETE" }, (RequestDelegate)(context => peer!.ApiAsync(context)));

        await app.StartAsync();
        var address = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
        peer = new FakeWebPeer(name, token, app, new Uri(address.Replace("[::]", "127.0.0.1", StringComparison.Ordinal) + "/"));
        return peer;
    }

    /// <summary>Forgets every session, as a restart would.</summary>
    internal void ForgetSessions()
    {
        lock (_valid)
        {
            _valid.Clear();
        }
    }

    private async Task LoginAsync(HttpContext context)
    {
        var attempt = await Record(context);
        Logins.Enqueue(attempt);

        if (SignInRateLimitSeconds is { } wait)
        {
            context.Response.Headers.RetryAfter = wait.ToString(System.Globalization.CultureInfo.InvariantCulture);
            context.Response.StatusCode = StatusCodes.Status429TooManyRequests;
            return;
        }

        var form = QueryHelpers.ParseQuery(attempt.Body);
        if (!string.Equals(form.TryGetValue("token", out var posted) ? posted.ToString() : string.Empty, Token, StringComparison.Ordinal))
        {
            context.Response.StatusCode = StatusCodes.Status200OK;
            await context.Response.WriteAsync("<html><body>login</body></html>");
            return;
        }

        var value = "sess-" + Interlocked.Increment(ref _issued).ToString(System.Globalization.CultureInfo.InvariantCulture) + "-" + Guid.NewGuid().ToString("N");
        lock (_valid)
        {
            _valid.Add(value);
        }

        context.Response.Cookies.Append(SessionCookie, value, new CookieOptions { HttpOnly = true, Path = "/", SameSite = SameSiteMode.Strict });
        context.Response.Redirect("/");
    }

    private async Task ApiAsync(HttpContext context)
    {
        Api.Enqueue(await Record(context));

        var cookie = context.Request.Cookies[SessionCookie];
        bool signedIn;
        lock (_valid)
        {
            signedIn = cookie is not null && _valid.Contains(cookie);
            if (signedIn && Interlocked.Decrement(ref UnauthorizedNext) >= 0)
            {
                _valid.Remove(cookie!);
                signedIn = false;
            }
        }

        if (AlwaysUnauthorized || !signedIn)
        {
            context.Response.StatusCode = StatusCodes.Status401Unauthorized;
            context.Response.ContentType = "application/json; charset=utf-8";
            await context.Response.WriteAsync("{\"error\":\"session expired\",\"login\":\"/\"}");
            return;
        }

        /* What a current store's sign-in gate does with the front door's role header, before any route runs. */
        if (!OlderBuild && context.Request.Headers[DarlingWebSeat.FrontDoorRoleHeader] == DarlingWebSeat.FrontDoorViewerRole)
        {
            context.Response.Headers[DarlingWebSeat.FrontDoorRoleAppliedHeader] = DarlingWebSeat.FrontDoorViewerRole;
        }

        if (RedirectTo is { } redirect)
        {
            context.Response.Redirect(redirect);
            return;
        }

        ApiStarted.TrySetResult();
        if (Delay > TimeSpan.Zero)
        {
            try
            {
                await Task.Delay(Delay, context.RequestAborted);
            }
            catch (OperationCanceledException)
            {
                ApiAborted.TrySetResult();
                return;
            }
        }

        /* What a real dashboard adds to every answer and the front door must not pass on. */
        context.Response.Cookies.Append("peer_marker", "1", new CookieOptions { Path = "/" });
        context.Response.Headers.ContentSecurityPolicy = PeerCsp;
        context.Response.Headers["X-Peer-Marker"] = "yes";

        if (RateLimitSeconds is { } wait)
        {
            context.Response.Headers.RetryAfter = wait.ToString(System.Globalization.CultureInfo.InvariantCulture);
            context.Response.StatusCode = StatusCodes.Status429TooManyRequests;
            context.Response.ContentType = "application/json; charset=utf-8";
            await context.Response.WriteAsync("{\"error\":\"slow down\"}");
            return;
        }

        context.Response.ContentType = AnswerContentType ?? "application/json; charset=utf-8";
        await context.Response.WriteAsync("{\"peer\":\"" + Name + "\",\"path\":\"" + context.Request.Path.Value + "\"}");
    }

    private static async Task<Seen> Record(HttpContext context)
    {
        var headers = context.Request.Headers.ToDictionary(h => h.Key, h => h.Value.ToString(), StringComparer.OrdinalIgnoreCase);
        using var reader = new StreamReader(context.Request.Body);
        var body = await reader.ReadToEndAsync();
        return new Seen(context.Request.Method, context.Request.Path.Value ?? string.Empty, context.Request.QueryString.Value ?? string.Empty, headers, body);
    }

    public async ValueTask DisposeAsync()
    {
        await _app.StopAsync();
        await _app.DisposeAsync();
    }
}
