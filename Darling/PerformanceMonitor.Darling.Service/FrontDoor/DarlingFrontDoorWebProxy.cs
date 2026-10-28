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
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.FrontDoor;

/// <summary>
/// The front door's web routes (#6840): they replace the dashboard's own endpoint map when the service runs as a front door, so one
/// web address serves the pages and sends each <c>/api/*</c> call to the store it belongs to. The sign-in, the OIDC leg, the role gate,
/// the Host guard and the session cookie are the web host's own and run before any route here: a request reaches this class only
/// after it has passed them, and a read-only role never gets a change through.
///
/// <para><b>Routes.</b> <c>/api/ping</c>, <c>/api/session</c>, <c>GET /api/front-door</c> and <c>GET /api/front-door/locate</c> are
/// answered here. Every other <c>/api/*</c> call is forwarded to one store. <c>/auth/*</c> and the static pages are never forwarded.</para>
///
/// <para><b>Which store.</b> The <c>X-Darling-Store</c> header (a configured name, else 400), then <c>?server=</c> through the
/// directory, then the only configured store. A name found in more than one store is refused with 409 and nothing is sent. A change
/// (anything but GET) needs the header or a server name that matches exactly, and never falls back to the only store. A call with no
/// store gets 400 <c>store_required</c> and the list.</para>
///
/// <para><b>What crosses.</b> Out: <c>Accept</c>, <c>Accept-Language</c>, <c>Content-Type</c>, the body, and the store's own session
/// cookie from <see cref="DarlingFrontDoorWebSession"/>. Nothing else is copied, so the browser's cookie, <c>Authorization</c>,
/// <c>Origin</c>, <c>Referer</c>, <c>X-Forwarded-*</c> and <c>X-Darling-Store</c> never reach a store, and <c>Host</c> is the store's
/// own. Back: the status, <c>Content-Type</c>, <c>Retry-After</c> and the body, streamed. A store's <c>Set-Cookie</c> and its security
/// headers are dropped: a browser never holds a store's session, and the front door's own headers are the ones on every answer. An
/// answer whose type is not JSON is sent as a download (<see cref="CopyContentType"/>), so it never opens as a page on the front
/// door's address.</para>
///
/// <para><b>A viewer's role at the store.</b> The store's session is the shared token's, which can edit. For a person whose seat cannot
/// edit, every forwarded request carries <see cref="DarlingWebSeat.FrontDoorRoleHeader"/>, and the store answers it as a reads-only
/// caller (<see cref="DarlingWebSeat.ApplyFrontDoorRole"/>): its own handlers refuse what they refuse a viewer and report a viewer's
/// flags. An answer without <see cref="DarlingWebSeat.FrontDoorRoleAppliedHeader"/> is refused with 502, because a store on a build
/// without this support would have answered as the shared-token seat.</para>
/// </summary>
internal sealed class DarlingFrontDoorWebProxy
{
    /// <summary>The request header that names the store a call is for.</summary>
    internal const string StoreHeader = "X-Darling-Store";

    /// <summary>The most a change may carry. The body is held in memory so a store that answers 401 can be sent it again.</summary>
    internal const int MaxBodyBytes = 8 * 1024 * 1024;

    private static readonly string[] ForwardedMethods = { "GET", "POST", "PUT", "PATCH", "DELETE" };

    private static readonly TimeSpan LogEvery = TimeSpan.FromSeconds(30);

    private readonly IFrontDoorDirectory _directory;
    private readonly DarlingFrontDoorWebSession _session;
    private readonly TimeSpan _peerTimeout;
    private readonly ILogger _logger;
    private readonly TimeProvider _time;
    private readonly ConcurrentDictionary<string, DateTimeOffset> _lastLogged = new();

    internal DarlingFrontDoorWebProxy(
        IFrontDoorDirectory directory, DarlingFrontDoorWebSession session, TimeSpan peerTimeout, ILogger logger, TimeProvider? time = null)
    {
        _directory = directory;
        _session = session;
        _peerTimeout = peerTimeout;
        _logger = logger;
        _time = time ?? TimeProvider.System;
    }

    /// <summary>The proxy the service builds when it starts as a front door: <c>frontDoor.peerTimeoutSeconds</c> bounds each call.</summary>
    internal static DarlingFrontDoorWebProxy Create(
        IFrontDoorDirectory directory, IFrontDoorSecrets secrets, FrontDoorConfig config, ILogger logger)
        => new(
            directory,
            new DarlingFrontDoorWebSession(secrets),
            TimeSpan.FromSeconds(Math.Clamp(config.PeerTimeoutSeconds, FrontDoorConfig.MinPeerTimeoutSeconds, FrontDoorConfig.MaxPeerTimeoutSeconds)),
            logger);

    /// <summary>Maps every <c>/api/*</c> route. It runs where the dashboard's own endpoints would, after the sign-in and role gates.</summary>
    internal void AddRoutes(WebApplication app)
    {
        app.Lifetime.ApplicationStopped.Register(_session.Dispose);
        app.MapMethods("/api/{**rest}", ForwardedMethods, (RequestDelegate)DispatchAsync);
    }

    private Task DispatchAsync(HttpContext context)
    {
        var path = (context.Request.Path.Value ?? "/").TrimEnd('/');
        if (path.Equals("/api/ping", StringComparison.OrdinalIgnoreCase))
        {
            return LocalGetAsync(context, PingAsync);
        }

        if (path.Equals("/api/session", StringComparison.OrdinalIgnoreCase))
        {
            return LocalGetAsync(context, SessionAsync);
        }

        if (path.Equals("/api/front-door", StringComparison.OrdinalIgnoreCase))
        {
            return LocalGetAsync(context, StoresAsync);
        }

        if (path.Equals("/api/front-door/locate", StringComparison.OrdinalIgnoreCase))
        {
            return LocalGetAsync(context, LocateAsync);
        }

        /* The namespace is the front door's own: a name under it is never a store's route. */
        if (path.StartsWith("/api/front-door/", StringComparison.OrdinalIgnoreCase))
        {
            return WriteJsonAsync(context, StatusCodes.Status404NotFound, new JsonObject { ["error"] = "Not found." });
        }

        return ForwardAsync(context);
    }

    private static Task LocalGetAsync(HttpContext context, Func<HttpContext, Task> answer)
    {
        if (!HttpMethods.IsGet(context.Request.Method))
        {
            context.Response.Headers.Allow = "GET";
            return WriteJsonAsync(context, StatusCodes.Status405MethodNotAllowed, new JsonObject { ["error"] = "This route answers GET only." });
        }

        return answer(context);
    }

    /* ── local routes ── */

    /// <summary>The front door holds no store and runs no collector, so there is no collection verdict to report: it is up when it answers.</summary>
    private static Task PingAsync(HttpContext context)
        => WriteJsonAsync(context, StatusCodes.Status200OK, new JsonObject { ["status"] = "ok", ["front_door"] = true });

    /// <summary>The front door's own role. It is the seat the front door's gate gave this request, not a store's: a store sees only the
    /// front door's shared-token session, so personal views are off here (<c>can_own</c>) and the stores answer their own refusals.</summary>
    private static Task SessionAsync(HttpContext context)
    {
        var seat = DarlingWebSeat.FromContext(context);
        return WriteJsonAsync(context, StatusCodes.Status200OK, new JsonObject
        {
            ["can_edit"] = seat.CanEdit,
            ["can_own"] = false,
            ["signed_in_admin"] = DarlingWebSeat.IsSignedInAdmin(context),
            ["managed_store"] = false,
            ["admin_writes_off"] = false,
            ["front_door"] = true,
        });
    }

    private Task StoresAsync(HttpContext context)
    {
        var stores = new JsonArray();
        foreach (var state in _directory.Stores)
        {
            stores.Add(new JsonObject
            {
                ["name"] = state.Store.Name,
                ["health"] = HealthWord(state.Health),
                ["server_count"] = state.Servers.Count,
                ["last_ok_utc"] = state.LastOkUtc is { } last
                    ? DateTime.SpecifyKind(last, DateTimeKind.Utc).ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", System.Globalization.CultureInfo.InvariantCulture)
                    : null,
            });
        }

        return WriteJsonAsync(context, StatusCodes.Status200OK, new JsonObject { ["front_door"] = true, ["stores"] = stores });
    }

    private static string HealthWord(FrontDoorStoreHealth health) => health switch
    {
        FrontDoorStoreHealth.Ok => "ok",
        FrontDoorStoreHealth.Stale => "stale",
        FrontDoorStoreHealth.AuthFailed => "auth_failed",
        _ => "unreachable",
    };

    private async Task LocateAsync(HttpContext context)
    {
        var name = context.Request.Query["server"].ToString().Trim();
        if (name.Length == 0 || name == "*")
        {
            await WriteJsonAsync(context, StatusCodes.Status400BadRequest,
                new JsonObject { ["status"] = "invalid", ["message"] = "Give the server's name in the server parameter." });
            return;
        }

        var route = await ResolveWithRefreshAsync(name, context.RequestAborted);
        switch (route)
        {
            case FrontDoorRoute.ToStore found:
                {
                    var servers = _directory.Stores.FirstOrDefault(s => string.Equals(s.Store.Name, found.Store, StringComparison.OrdinalIgnoreCase))?.Servers
                        ?? Array.Empty<RegisteredServer>();
                    var match = DarlingFrontDoorDirectory.MatchIn(servers, name);
                    await WriteJsonAsync(context, StatusCodes.Status200OK, new JsonObject
                    {
                        ["store"] = found.Store,
                        ["server_name"] = match.Candidates.Count == 1 ? match.Candidates[0].ServerName : name,
                    });
                    return;
                }

            case FrontDoorRoute.Refused { Ambiguous: true } ambiguous:
                await WriteJsonAsync(context, StatusCodes.Status409Conflict, new JsonObject
                {
                    ["status"] = "ambiguous",
                    ["message"] = ambiguous.Message,
                    ["stores"] = StoreArray(ambiguous.Stores ?? Array.Empty<string>()),
                });
                return;

            case FrontDoorRoute.Refused refused:
                await WriteJsonAsync(context, StatusCodes.Status404NotFound, new JsonObject { ["status"] = "not_found", ["message"] = refused.Message });
                return;

            default:
                await WriteJsonAsync(context, StatusCodes.Status400BadRequest,
                    new JsonObject { ["status"] = "invalid", ["message"] = "Give one server's name, not a pattern." });
                return;
        }
    }

    /* ── forwarding ── */

    private async Task ForwardAsync(HttpContext context)
    {
        var request = context.Request;
        var isGet = HttpMethods.IsGet(request.Method);

        /* The dashboard's change routes each refuse a body that is not JSON (415), which is what makes a cross-site form post unable to
           change anything: a browser only sends JSON after a preflight the dashboard never answers. The front door refuses it first so
           the call never leaves this process. */
        if (!isGet && !DarlingWebEndpoints.IsJsonContentType(request.ContentType))
        {
            await WriteJsonAsync(context, StatusCodes.Status415UnsupportedMediaType, new JsonObject { ["error"] = "Content-Type must be application/json." });
            return;
        }

        var store = await ResolveStoreAsync(context, isGet);
        if (store is null)
        {
            return;
        }

        if (store.WebUrl is null)
        {
            await WriteStoreErrorAsync(context, StatusCodes.Status502BadGateway, "store_unavailable", store.Name,
                $"Store '{store.Name}' has no web address in the front door's configuration.");
            return;
        }

        byte[]? body = null;
        if (!isGet)
        {
            body = await ReadBodyAsync(request, context.RequestAborted);
            if (body is null)
            {
                await WriteJsonAsync(context, StatusCodes.Status413PayloadTooLarge, new JsonObject { ["error"] = "The request body is too large." });
                return;
            }
        }

        /* Decoded, so a %0D%0A in the address is a real line break by now: the log gets the sanitized form. */
        var route = DarlingHttpRefusalLog.Sanitize(request.Path.Value);
        var viewer = !DarlingWebSeat.FromContext(context).CanEdit;
        var started = Stopwatch.GetTimestamp();

        /* The page closing cancels RequestAborted, and the timeout cancels the same token: the peer call, its sign-in and the copy of
           its answer all stop on either. */
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(context.RequestAborted);
        linked.CancelAfter(_peerTimeout);
        var ct = linked.Token;

        /* The store's status for a change (0 until there is one), which the finally below logs: who made each write (D1). */
        var finalStatus = 0;
        try
        {
            var signedInAgain = false;
            while (true)
            {
                var cookie = await _session.GetCookieAsync(store, ct);
                using var peerRequest = BuildRequest(request, store.WebUrl, body, cookie, viewer);
                using var response = await _session.ClientFor(store).SendAsync(peerRequest, HttpCompletionOption.ResponseHeadersRead, ct);

                if (response.StatusCode == System.Net.HttpStatusCode.Unauthorized)
                {
                    /* The store's gate refused the session before any route ran (it restarted, or its cookie lapsed early), so nothing
                       was done and the call can be sent again: sign in once more and retry once. A second 401 is not passed on: to the
                       page a 401 means the person's own session ended and sends them to the sign-in. */
                    _session.Discard(store, cookie);
                    if (!signedInAgain)
                    {
                        signedInAgain = true;
                        continue;
                    }

                    throw new FrontDoorPeerUnavailableException(
                        store.Name, $"Store '{store.Name}' did not accept the front door's session even after a new sign-in.");
                }

                var status = (int)response.StatusCode;
                if (status is >= 300 and < 400 && status != StatusCodes.Status304NotModified)
                {
                    /* A redirect has no Location here, so the page could not follow it: a store behind a proxy that redirects (http to
                       https, say) is a setup to fix in the front door's webUrl. */
                    throw new FrontDoorPeerUnavailableException(
                        store.Name, $"Store '{store.Name}' answered with a redirect (HTTP {status}). Check that the store's webUrl in the front door's configuration is the address it serves.");
                }

                if (viewer && !response.Headers.Contains(DarlingWebSeat.FrontDoorRoleAppliedHeader))
                {
                    throw new FrontDoorPeerUnavailableException(
                        store.Name, $"Store '{store.Name}' did not confirm it answered as a viewer, so its answer is not passed on. Update it to a build whose dashboard reads the front door's role header.");
                }

                finalStatus = status;
                context.Response.StatusCode = status;
                CopyContentType(response.Content.Headers.ContentType, context.Response);

                if (response.Headers.TryGetValues("Retry-After", out var retryAfter))
                {
                    context.Response.Headers.RetryAfter = retryAfter.ToArray();
                }

                await response.Content.CopyToAsync(context.Response.Body, ct);
                return;
            }
        }
        catch (FrontDoorPeerUnavailableException ex) when (!context.Response.HasStarted)
        {
            finalStatus = StatusCodes.Status502BadGateway;
            Note(store.Name, route, StatusCodes.Status502BadGateway, ex.Message, started);
            await WriteStoreErrorAsync(context, StatusCodes.Status502BadGateway, "store_unavailable", store.Name, ex.Message);
        }
        catch (OperationCanceledException) when (context.RequestAborted.IsCancellationRequested)
        {
            /* The page went away: nobody is left to answer, and the peer call was cancelled with it. */
        }
        catch (OperationCanceledException)
        {
            finalStatus = StatusCodes.Status504GatewayTimeout;
            Note(store.Name, route, StatusCodes.Status504GatewayTimeout, "timed out", started);
            if (context.Response.HasStarted)
            {
                context.Abort();
            }
            else
            {
                await WriteStoreErrorAsync(context, StatusCodes.Status504GatewayTimeout, "store_timeout", store.Name,
                    $"Store '{store.Name}' did not answer within {(int)_peerTimeout.TotalSeconds} seconds.");
            }
        }
        catch (HttpRequestException ex) when (!context.Response.HasStarted)
        {
            var why = DescribeConnectFailure(ex);
            finalStatus = StatusCodes.Status502BadGateway;
            Note(store.Name, route, StatusCodes.Status502BadGateway, why, started);
            await WriteStoreErrorAsync(context, StatusCodes.Status502BadGateway, "store_unavailable", store.Name, $"Store '{store.Name}' could not be reached: {why}.");
        }
        catch (Exception ex) when (ex is HttpRequestException or FrontDoorPeerUnavailableException or IOException)
        {
            /* The answer had started: nothing can be written over it, so the connection ends and the page sees the failure. */
            finalStatus = context.Response.StatusCode;
            Note(store.Name, route, context.Response.StatusCode, "the connection ended mid-answer", started);
            context.Abort();
        }
        finally
        {
            if (!isGet && finalStatus != 0)
            {
                /* One line per change: the signed-in person and role, the store, the method, the path without its query, and the
                   store's status. Never the body, a token, a cookie or the query string. */
                var seat = DarlingWebSeat.FromContext(context);
                _logger.LogInformation(
                    "Front door web change: {User} ({Role}) sent {Method} {Route} to store {Store}, which answered {Status}",
                    seat.EditorPrincipal, seat.CanEdit ? "admin" : "viewer", request.Method, route, store.Name, finalStatus);
            }
        }
    }

    /// <summary>
    /// Copies a store's <c>Content-Type</c> (#6840). JSON passes as it is: it is what every dashboard read answers, and the front door's
    /// own <c>X-Content-Type-Options: nosniff</c> keeps a browser from reading it as anything else. Any other type, or none, is sent as
    /// a download (<c>Content-Disposition: attachment</c>, plus <c>nosniff</c> on the answer itself): a page's own fetch reads the body
    /// the same either way, but an answer that was changed on a plain http link to a store, to <c>text/html</c> or a script, never runs
    /// on the front door's address, which holds a session for every store.
    /// </summary>
    internal static void CopyContentType(MediaTypeHeaderValue? contentType, HttpResponse response)
    {
        if (contentType is not null)
        {
            response.ContentType = contentType.ToString();
        }

        if (!string.Equals(contentType?.MediaType, "application/json", StringComparison.OrdinalIgnoreCase))
        {
            response.Headers.ContentDisposition = "attachment";
            response.Headers.XContentTypeOptions = "nosniff";
        }
    }

    private static HttpRequestMessage BuildRequest(HttpRequest request, Uri webUrl, byte[]? body, string cookie, bool viewer)
    {
        var message = new HttpRequestMessage(new HttpMethod(request.Method), PeerUri(webUrl, request.Path.ToUriComponent(), request.QueryString.Value));

        /* Set by the front door alone (the browser's own copy is not on the allow-list below), and only for a seat that cannot edit. */
        if (viewer)
        {
            message.Headers.TryAddWithoutValidation(DarlingWebSeat.FrontDoorRoleHeader, DarlingWebSeat.FrontDoorViewerRole);
        }

        /* An allow-list, not a drop-list: what the browser sent that a store does not read stays here. Host comes from the target. */
        foreach (var name in new[] { "Accept", "Accept-Language" })
        {
            if (request.Headers.TryGetValue(name, out var values) && !string.IsNullOrWhiteSpace(values.ToString()))
            {
                message.Headers.TryAddWithoutValidation(name, values.ToString());
            }
        }

        if (body is not null)
        {
            var content = new ByteArrayContent(body);
            if (MediaTypeHeaderValue.TryParse(request.ContentType, out var contentType))
            {
                content.Headers.ContentType = contentType;
            }

            message.Content = content;
        }

        message.Headers.TryAddWithoutValidation("Cookie", cookie);
        return message;
    }

    /// <summary>The store's address for one path: the configured base, then the path the browser asked for, which the web host has already
    /// decoded and cleaned of dot segments. Throws when the result leaves the configured address or the store's <c>/api</c> tree.</summary>
    internal static Uri PeerUri(Uri webUrl, string escapedPath, string? query)
    {
        var basePath = webUrl.AbsolutePath.TrimEnd('/');
        var target = new Uri(webUrl.GetLeftPart(UriPartial.Authority) + basePath + escapedPath + query, UriKind.Absolute);
        var inApi = target.AbsolutePath.StartsWith(basePath + "/api/", StringComparison.OrdinalIgnoreCase);
        var isLogin = string.Equals(target.AbsolutePath, basePath + DarlingWebHostService.LoginPath, StringComparison.OrdinalIgnoreCase);
        if (!string.Equals(target.Scheme, webUrl.Scheme, StringComparison.Ordinal)
            || !string.Equals(target.Authority, webUrl.Authority, StringComparison.OrdinalIgnoreCase)
            || !(inApi || isLogin))
        {
            throw new FrontDoorPeerUnavailableException(string.Empty, "The request path does not stay inside the store's address.");
        }

        return target;
    }

    private static async Task<byte[]?> ReadBodyAsync(HttpRequest request, CancellationToken ct)
    {
        if (request.ContentLength is > MaxBodyBytes)
        {
            return null;
        }

        using var buffer = new MemoryStream();
        var chunk = new byte[81920];
        int read;
        while ((read = await request.Body.ReadAsync(chunk, ct)) > 0)
        {
            if (buffer.Length + read > MaxBodyBytes)
            {
                return null;
            }

            buffer.Write(chunk, 0, read);
        }

        return buffer.ToArray();
    }

    /* ── choosing the store ── */

    /// <summary>The store this call is for, or null after writing the refusal.</summary>
    private async Task<FrontDoorStore?> ResolveStoreAsync(HttpContext context, bool isGet)
    {
        var configured = _directory.Stores;
        var names = configured.Select(s => s.Store.Name).ToArray();

        var header = context.Request.Headers[StoreHeader];
        if (header.Count > 1)
        {
            await WriteJsonAsync(context, StatusCodes.Status400BadRequest,
                new JsonObject { ["status"] = "invalid", ["message"] = StoreHeader + " may be given once." });
            return null;
        }

        var named = header.Count == 1 ? (header[0] ?? string.Empty).Trim() : string.Empty;
        if (named.Length > 0)
        {
            if (_directory.Find(named) is { } byName)
            {
                return byName;
            }

            await WriteJsonAsync(context, StatusCodes.Status400BadRequest, new JsonObject
            {
                ["status"] = "unknown_store",
                ["message"] = "No store with that name is configured.",
                ["stores"] = StoreArray(names),
            });
            return null;
        }

        var serverName = context.Request.Query["server"].ToString().Trim();
        if (serverName.Length > 0)
        {
            var route = await ResolveWithRefreshAsync(serverName, context.RequestAborted);
            switch (route)
            {
                case FrontDoorRoute.ToStore found when _directory.Find(found.Store) is { } routed:
                    if (!isGet && !MatchesExactly(found.Store, serverName))
                    {
                        /* A partial name could belong to another store's server tomorrow, or to a replica: a change names its store. */
                        await WriteStoreRequiredAsync(context, names,
                            "A change names its store in the " + StoreHeader + " header, or a server by its exact name.");
                        return null;
                    }

                    return routed;

                case FrontDoorRoute.Refused { Ambiguous: true } ambiguous:
                    await WriteJsonAsync(context, StatusCodes.Status409Conflict, new JsonObject
                    {
                        ["status"] = "ambiguous",
                        ["message"] = ambiguous.Message,
                        ["stores"] = StoreArray(ambiguous.Stores ?? Array.Empty<string>()),
                    });
                    return null;
            }
        }

        if (isGet && configured.Count == 1)
        {
            return configured[0].Store;
        }

        await WriteStoreRequiredAsync(context, names,
            isGet ? "Say which store this call is for with the " + StoreHeader + " header, or a server parameter a store holds."
                  : "A change names its store in the " + StoreHeader + " header, or a server by its exact name.");
        return null;
    }

    /// <summary>The directory's answer, looked up again once after a refresh when the name matched nothing in any store (a server added since).</summary>
    private async Task<FrontDoorRoute> ResolveWithRefreshAsync(string serverName, CancellationToken ct)
    {
        var route = _directory.ResolveServer(serverName);
        if (route is FrontDoorRoute.Refused { Ambiguous: false })
        {
            await _directory.RefreshOnMissAsync(ct);
            route = _directory.ResolveServer(serverName);
        }

        return route;
    }

    private bool MatchesExactly(string store, string serverName)
        => _directory.Stores.FirstOrDefault(s => string.Equals(s.Store.Name, store, StringComparison.OrdinalIgnoreCase)) is { } state
           && DarlingFrontDoorDirectory.MatchIn(state.Servers, serverName).MatchedBy == "exact";

    /* ── answers ── */

    private static JsonArray StoreArray(IEnumerable<string> names)
    {
        var array = new JsonArray();
        foreach (var name in names)
        {
            array.Add(name);
        }

        return array;
    }

    private static Task WriteStoreRequiredAsync(HttpContext context, IEnumerable<string> names, string message)
        => WriteJsonAsync(context, StatusCodes.Status400BadRequest, new JsonObject
        {
            ["status"] = "store_required",
            ["message"] = message,
            ["stores"] = StoreArray(names),
        });

    private static Task WriteStoreErrorAsync(HttpContext context, int status, string word, string store, string message)
        => WriteJsonAsync(context, status, new JsonObject { ["status"] = word, ["store"] = store, ["message"] = message });

    private static Task WriteJsonAsync(HttpContext context, int status, JsonNode body)
    {
        context.Response.StatusCode = status;
        context.Response.ContentType = "application/json; charset=utf-8";
        return context.Response.WriteAsync(body.ToJsonString(), context.RequestAborted);
    }

    /// <summary>Why a connection to a store failed, in words that name no address and no certificate detail.</summary>
    internal static string DescribeConnectFailure(HttpRequestException ex) => ex.HttpRequestError switch
    {
        HttpRequestError.NameResolutionError => "its address did not resolve",
        HttpRequestError.SecureConnectionError => "its certificate was not accepted",
        HttpRequestError.ConnectionError => "the connection was refused or timed out",
        _ => "the connection failed",
    };

    /// <summary>Logs one failure line per store and kind every <see cref="LogEvery"/>: a page that polls a store that is down would otherwise
    /// write a line per call. Store, route (no query), status and elapsed time only.</summary>
    private void Note(string store, string route, int status, string why, long startedTimestamp)
    {
        var now = _time.GetUtcNow();
        var key = store + "|" + status;
        if (_lastLogged.TryGetValue(key, out var last) && now - last < LogEvery)
        {
            return;
        }

        _lastLogged[key] = now;
        _logger.LogWarning(
            "Front door web: store {Store} {Route} answered {Status} after {Elapsed} ms ({Why})",
            store, route, status, (long)Stopwatch.GetElapsedTime(startedTimestamp).TotalMilliseconds, why);
    }
}
