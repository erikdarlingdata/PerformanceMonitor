/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

public static partial class DarlingWebEndpoints
{
    /// <summary>#6390 S2c: what the service gives the composed-panel run core, which lives in Storage: its own server-name resolver
    /// (with the refusals and peer lookups), registry lookup, store-setting deadline read and the web data-start fields.</summary>
    internal static readonly ComposeRunHost ComposeHost = new(
        ComposeServerScope.ResolveNamesAsync,
        ComposeServerScope.FindUnregisteredAsync,
        McpCommandDeadlines.ResolveComposedQuerySecondsAsync,
        WebDataStartNote.AddComposedPanelInstants);

    /// <summary>Seconds of client-deadline headroom the web compose path adds; the value and its reasoning live with the run core (#6390 S2c).</summary>
    internal const int ComposeClientDeadlineHeadroomSeconds = ComposePanelRunner.ComposeClientDeadlineHeadroomSeconds;

    /// <summary>The schema-version probe the run core reads; the text lives with the run core (#6390 S2c).</summary>
    internal const string QueryStoreWideSchemaVersionSql = ComposePanelRunner.QueryStoreWideSchemaVersionSql;

    /// <summary>Default compose-run window (hours) when the request omits one; the value lives with the run core (#6390 S2c).</summary>
    internal const int DefaultComposeHours = ComposePanelRunner.DefaultComposeHours;

    /// <summary>
    /// Compile-and-run a single composed panel spec (Custom Views v2, #1563) against <paramref name="postgres"/>
    /// — the ONE runner shared by the web <c>/api/compose/run</c> endpoint and the MCP <c>run_custom_view_panel</c>
    /// tool, so the two surfaces can never diverge on validation, compilation, or query shape. Takes an
    /// already-parsed request body <c>{panel, variables?, values?, server?, hours?}</c> and returns a
    /// <see cref="ComposeRunOutcome"/> the caller maps to its surface. The panel is validated by the SAME
    /// <see cref="ComposeSpec.TryParsePanel"/> authority the write path uses (so a panel that runs here is a
    /// panel that stores), the compiler emits only catalog identifiers — schema-qualified <c>collect.*</c>, every
    /// value bound — and the query runs under the pool's role <c>statement_timeout</c> backstop. A cancellation
    /// (<see cref="OperationCanceledException"/>) is deliberately NOT caught: it propagates to the caller as a
    /// client-abort, exactly as the endpoint has always done.
    ///
    /// <para><paramref name="readLatency"/> (#4782) is the read-latency seat this run is recorded into: the web
    /// route passes the one its own <see cref="MapAll"/> call built, the MCP tool the one its host registered.
    /// Null records nothing (a test calling the runner directly).</para>
    ///
    /// <para><paramref name="includeDataStartFields"/> adds the data-start sentence and the instants it names beside
    /// <c>notice</c> (<see cref="WebDataStartNote.AddComposedPanelInstants"/>), so the page can write the sentence again
    /// in the browser's zone. Only the web route sets it; the MCP tool leaves it off, so its client reads the sentence
    /// once, in <c>notice</c>.</para>
    /// </summary>
    internal static async Task<ComposeRunOutcome> RunComposedPanelAsync(
        NpgsqlDataSource postgres, JsonObject body, System.Threading.CancellationToken cancellationToken,
        ReadLatencyRecorder? readLatency = null, int clientDeadlineHeadroomSeconds = 0, bool remapClientTimeout = false, bool includeDataStartFields = false,
        Action<Exception>? onRunException = null, DateTime? nowUtc = null, string? caller = null, ReadSurface? cacheHost = null)
    {
        /* #5506 part 3 (round 1, C1): a heavy panel is answered from the read cache, joins an identical run in flight, or
           runs once and is stored, as /api/read and the MCP reads are. The host that runs it (cacheHost: web or mcp) is
           part of the key (D4): the two serialize the payload differently and the web adds its data-start fields, so
           their entries are never shared. What is stored is the exact text the host answers (PayloadText), so a hit is
           byte-identical to the run that stored it. A hit skips admission. */
        var admission = readLatency?.Admission;
        if (cacheHost is { } host && admission is not null
            && admission.IsCached(ReadSurface.Compose, ComposeRouteLabel(body), ComposeWindowBand(body)))
        {
            return await RunComposedPanelCachedAsync(postgres, body, readLatency!, admission, host,
                clientDeadlineHeadroomSeconds, remapClientTimeout, includeDataStartFields, onRunException, nowUtc, caller, cancellationToken);
        }

        return await RunComposedPanelAdmittedAsync(postgres, body, readLatency, clientDeadlineHeadroomSeconds,
            remapClientTimeout, includeDataStartFields, onRunException, nowUtc, caller, cacheHost, cancellationToken);
    }

    private static async Task<ComposeRunOutcome> RunComposedPanelCachedAsync(
        NpgsqlDataSource postgres, JsonObject body, ReadLatencyRecorder readLatency,
        ReadAdmission admission, ReadSurface host, int clientDeadlineHeadroomSeconds, bool remapClientTimeout, bool includeDataStartFields,
        Action<Exception>? onRunException, DateTime? nowUtc, string? caller, System.Threading.CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        using var readScope = ReadScope.Open(readLatency.Logger);
        var composeRoute = ComposeRouteLabel(body);
        var composeBand = ComposeWindowBand(body);
        var recordCaller = caller ?? ReadCaller.AmbientMcpCaller;
        var cache = admission.Cache;
        var ttl = ReadResultCache.TtlOf(composeBand);
        var key = ReadCacheKey.ForCompose(host, composeRoute, body.ToJsonString(), cache.Generation, cache, ttl);
        if (cache.TryGet(key, out var stored))
        {
            /* Round 2, L3: a hit runs no read, so it does not count as one in flight. */
            readScope.Scope.Uncount();
            admission.NoteCacheHit(ReadSurface.Compose, composeRoute);
            RecordAdmissionSample(readLatency, composeRoute, ReadOutcome.CacheHit, stopwatch.ElapsedMilliseconds, composeBand, readScope.Scope, recordCaller);
            return ComposeRunOutcome.FromCache(stored);
        }

        /* D6: the run is shared; the caller that starts it records its real outcome inside the run. Round 2, M1: a caller
           that joined records only its own outcome with its own elapsed time (busy, cancelled, or cache_hit for the shared
           answer), never the run's, so one run counts once. */
        var flight = cache.Join(key, ttl,
            token => RunComposedPanelAdmittedAsync(postgres, body, readLatency, clientDeadlineHeadroomSeconds, remapClientTimeout,
                includeDataStartFields, onRunException, nowUtc, caller, host, token),
            run => run.PayloadText is { } text && ReadResultCache.IsCacheableText(text, gcf: false) ? text : null);
        if (flight.IsLead)
        {
            admission.NoteCacheMiss(ReadSurface.Compose, composeRoute);
            return await flight.WaitAsync(cancellationToken);
        }

        /* Round 2, L3: a joined caller runs no read while it waits for the shared one. */
        readScope.Scope.Uncount();
        try
        {
            var shared = await flight.WaitAsync(cancellationToken);
            var joinedOutcome = ReadResultCache.JoinedOutcome(shared.Busy is not null ? ReadOutcome.Busy : null, cancellationToken);
            if (joinedOutcome == ReadOutcome.CacheHit)
            {
                admission.NoteCacheHit(ReadSurface.Compose, composeRoute);
            }

            RecordAdmissionSample(readLatency, composeRoute, joinedOutcome, stopwatch.ElapsedMilliseconds, composeBand, readScope.Scope, recordCaller);
            return shared;
        }
        catch (Exception ex)
        {
            var thrown = ReadResultCache.JoinedOutcome(
                ReadOutcomeClassifier.Classify(ex, cancellationToken) == ReadOutcome.Cancelled ? ReadOutcome.Cancelled : null, cancellationToken);
            if (thrown == ReadOutcome.CacheHit)
            {
                admission.NoteCacheHit(ReadSurface.Compose, composeRoute);
            }

            RecordAdmissionSample(readLatency, composeRoute, thrown, stopwatch.ElapsedMilliseconds, composeBand, readScope.Scope, recordCaller);
            throw;
        }
    }

    private static async Task<ComposeRunOutcome> RunComposedPanelAdmittedAsync(
        NpgsqlDataSource postgres, JsonObject body,
        ReadLatencyRecorder? readLatency, int clientDeadlineHeadroomSeconds, bool remapClientTimeout, bool includeDataStartFields,
        Action<Exception>? onRunException, DateTime? nowUtc, string? caller, ReadSurface? cacheHost, System.Threading.CancellationToken cancellationToken)
    {
        /* #4442 scope 2: recorded ONCE per call, here, so the web /api/compose/run route and the MCP
           run_custom_view_panel tool -- both of which call this ONE runner -- contribute exactly one
           'compose' sample each, never two. The route label is the panel's source measure key when the body
           parses far enough to have one, else the fixed label below; both are bounded-cardinality (the
           catalog's own measure keys), never the caller's raw JSON. Recording never throws into the caller:
           a bucket update is the only work in the try, and any failure there is swallowed and logged at
           Debug, exactly like the web loop's own recording. */
        var stopwatch = Stopwatch.StartNew();
        using var readScope = ReadScope.Open(readLatency?.Logger);
        readScope.Scope.CaptureStatements = true;

        /* #5506 part 3: admission, after the scope and the band, under the same route label and band the run records.
           A refusal records busy, a caller that leaves the queue records cancelled, and a queued run records its wait as
           one queue_wait sample; the recorded elapsed time starts after admission (D1). */
        var admission = readLatency?.Admission;
        var composeRoute = ComposeRouteLabel(body);
        var composeBand = ComposeWindowBand(body);
        using var ticket = admission is null
            ? ReadAdmissionTicket.Passed
            : await ReadScope.AdmitAsync(admission, ReadSurface.Compose, composeRoute, composeBand, cancellationToken);
        if (ticket.Refusal is { } refusal)
        {
            RecordAdmissionSample(readLatency, composeRoute, ReadOutcome.Busy, ticket.QueueMs ?? 0, composeBand, readScope.Scope, caller ?? ReadCaller.AmbientMcpCaller);
            return ComposeRunOutcome.Refused(refusal);
        }

        if (ticket.CallerLeft)
        {
            RecordAdmissionSample(readLatency, composeRoute, ReadOutcome.Cancelled, ticket.QueueMs ?? 0, composeBand, readScope.Scope, caller ?? ReadCaller.AmbientMcpCaller);
            cancellationToken.ThrowIfCancellationRequested();
        }

        if (ticket.QueueMs is { } queueMs)
        {
            RecordAdmissionSample(readLatency, composeRoute, ReadOutcome.QueueWait, queueMs, composeBand, readScope.Scope, caller ?? ReadCaller.AmbientMcpCaller);
        }

        stopwatch.Restart();
        var outcome = await ComposePanelRunner.RunComposedPanelCoreAsync(ComposeHost, postgres, body, clientDeadlineHeadroomSeconds, remapClientTimeout, includeDataStartFields, cancellationToken, readLatency?.Logger, onRunException, nowUtc);
        /* Round 1, C1: the text the host answers, taken once here so a cached copy of it is the same bytes. */
        if (cacheHost is { } host && outcome.Payload is { } payload)
        {
            outcome = outcome with { PayloadText = host == ReadSurface.Mcp ? payload.ToJsonString(McpHelpers.JsonOptions) : payload.ToJsonString() };
        }

        /* #5793: the web route passes its session hash; the MCP tool passes none, and the tool filter's ambient caller
           names the client instead. */
        RecordComposeLatency(readLatency, body, outcome, stopwatch.ElapsedMilliseconds, cancellationToken, readScope.Fallback, readScope.Scope,
            caller ?? ReadCaller.AmbientMcpCaller);
        return outcome;
    }

    /// <summary>The composed-panel route label recorded against <see cref="ReadLatencyAccumulator"/>: the
    /// panel's own measure key when the body parsed that far, else a fixed catch-all -- bounded cardinality
    /// either way (the compose catalog's own measure keys, never the caller's free text).</summary>
    private const string ComposeUnknownRouteLabel = "compose:unknown";

    /// <summary>#5506 part 3 (round 2, L5): the MCP twin <c>/api/catalog</c> is admitted and recorded under.</summary>
    internal const string CatalogRoute = "describe_custom_view_catalog";

    /// <summary>The route name the live Active Queries result read is admitted and recorded under (#6386).</summary>
    internal const string ActiveQueriesLiveResultRoute = "active_queries_live_result";

    /// <summary>#5506 part 3 (round 1, M3): the name <c>/api/ag</c> is admitted and recorded under, its MCP twin's.</summary>
    internal const string AgHealthRoute = "get_ag_health";

    /// <summary>#5506 part 3: the web answer to a refused read: 429, a <c>Retry-After</c> header, and <c>{"error": sentence}</c>,
    /// which the page already shows (<c>wwwroot/js/util.js</c> reads <c>body.error</c>).</summary>
    internal static IResult BusyResult(ReadAdmissionRefusal refusal) =>
        new RetryAfterResult(ErrorResult(refusal.Sentence, StatusCodes.Status429TooManyRequests), refusal.RetryAfterSeconds);

    /// <summary>An answer with a <c>Retry-After</c> header, set on the response it is written to (round 1, C3): a busy
    /// answer made by a shared read reaches every caller of that read with the header, not only the first.</summary>
    private sealed class RetryAfterResult(IResult inner, int retryAfterSeconds) : IResult
    {
        public Task ExecuteAsync(HttpContext httpContext)
        {
            httpContext.Response.Headers.RetryAfter = retryAfterSeconds.ToString(CultureInfo.InvariantCulture);
            return inner.ExecuteAsync(httpContext);
        }
    }

    /// <summary>
    /// #5506 part 3 (round 1, C3): a copy of a read request for a read several callers share. It carries what a read
    /// handler and the recording read (method, path, query string, headers and cookies, user, services, remote address)
    /// and <paramref name="readToken"/> as its <c>RequestAborted</c>, so the shared read runs under the flight's token while
    /// the caller's own request keeps its own, and nothing the read does (its <c>as_of</c> added to the query string)
    /// changes the caller's request.
    /// </summary>
    internal static HttpContext DetachedReadContext(HttpContext source, CancellationToken readToken)
    {
        ArgumentNullException.ThrowIfNull(source);
        var copy = new DefaultHttpContext
        {
            RequestAborted = readToken,
            User = source.User,
            RequestServices = source.RequestServices,
        };
        var from = source.Request;
        var to = copy.Request;
        to.Method = from.Method;
        to.Scheme = from.Scheme;
        to.Host = from.Host;
        to.PathBase = from.PathBase;
        to.Path = from.Path;
        to.QueryString = from.QueryString;
        foreach (var header in from.Headers)
        {
            to.Headers[header.Key] = header.Value;
        }

        copy.Connection.RemoteIpAddress = source.Connection.RemoteIpAddress;
        return copy;
    }

    /// <summary>#5506 part 3: admits a dedicated web read route (<c>/api/fleet</c>, <c>/api/triage</c>,
    /// <c>/api/alerts/test</c>) and, given a recorder and the route's open scope, records its admission sample under the
    /// <c>web</c> surface and <paramref name="route"/> as the read loop does: <c>busy</c> when refused, <c>cancelled</c> when
    /// the browser left the queue, <c>queue_wait</c> when it waited. The run itself is recorded by <see cref="WebRouteRun"/>.
    /// A browser that leaves the queue ends the request, as the read loop's #4276 arm does.</summary>
    internal static Task<ReadAdmissionTicket> AdmitWebRouteAsync(ReadAdmission? admission, string route, string band, HttpContext context,
        ReadLatencyRecorder? recorder = null, ReadScope? scope = null)
    {
        if (admission is null)
        {
            return Task.FromResult(ReadAdmissionTicket.Passed);
        }

        /* Not async (round 1, L4): AdmitAsync sets the nesting marker in the CALLER's flow, and an async method undoes
           every AsyncLocal it sets when it returns, so a read nested under this route would take a second slot (a
           deadlock at a limit of 1). Only the recording below runs in an async continuation; it sets no AsyncLocal. */
        return RecordAsync(ReadScope.AdmitAsync(admission, ReadSurface.Web, route, band, context.RequestAborted));

        async Task<ReadAdmissionTicket> RecordAsync(Task<ReadAdmissionTicket> admitting)
        {
            var ticket = await admitting;
            if (ticket.Refusal is not null)
            {
                RecordWebRouteSample(recorder, route, ReadOutcome.Busy, ticket.QueueMs ?? 0, band, scope, context);
            }
            else if (ticket.CallerLeft)
            {
                RecordWebRouteSample(recorder, route, ReadOutcome.Cancelled, ticket.QueueMs ?? 0, band, scope, context);
                ticket.Dispose();
                context.RequestAborted.ThrowIfCancellationRequested();
            }
            else if (ticket.QueueMs is { } queueMs)
            {
                RecordWebRouteSample(recorder, route, ReadOutcome.QueueWait, queueMs, band, scope, context);
            }

            return ticket;
        }
    }

    /// <summary>#5506 part 3: one sample of a dedicated web read route under the <c>web</c> surface. Never throws into the
    /// request; a missing recorder or scope records nothing.</summary>
    internal static void RecordWebRouteSample(ReadLatencyRecorder? recorder, string route, ReadOutcome outcome, long elapsedMs, string band, ReadScope? scope, HttpContext context)
    {
        if (recorder is null || scope is null)
        {
            return;
        }

        try
        {
            recorder.Accumulator?.Record(ReadSurface.Web, route, outcome, elapsedMs, band, scope.InFlightAtStart, scope.WaitMs, WebReadCaller(context));
        }
        catch (Exception ex)
        {
            recorder.Logger?.LogDebug(ex, "Read-latency recording failed for the web route {Route}.", route);
        }
    }

    /// <summary>The outcome of a tool answer, as the read loop classifies it: a server-error envelope by its sentence
    /// (a caught 57014 is a timeout), anything else <c>ok</c>.</summary>
    internal static ReadOutcome ToolResultOutcome(string result, System.Threading.CancellationToken requestAborted) =>
        ClassifyToolResponse(result) == ToolResponseKind.ServerError
            ? ReadOutcomeClassifier.ClassifySentence(McpHelpers.ErrorMessageOf(result), requestAborted)
            : ReadOutcome.Ok;

    /// <summary>
    /// #5506 part 3: the run sample of a dedicated web read route (<c>/api/fleet</c>, <c>/api/triage</c>,
    /// <c>/api/alerts/test</c>), so the long-read table (<see cref="ReadAdmission"/>) learns these routes from their own
    /// p90 instead of the window-size rule alone. Created once the read may run, so the elapsed time starts after
    /// admission (D1). Disposing records once: the outcome given to <see cref="Complete"/>, a fallback the scope noted on an
    /// <c>ok</c>, or, when the route threw before completing, <c>cancelled</c> for a browser that left and <c>error</c>
    /// otherwise.
    /// </summary>
    internal sealed class WebRouteRun : IDisposable
    {
        private readonly ReadLatencyRecorder? _recorder;
        private readonly string _route;
        private readonly string _band;
        private readonly HttpContext _context;
        private readonly ReadScope _scope;
        private readonly Stopwatch _stopwatch = Stopwatch.StartNew();
        private ReadOutcome? _outcome;
        private int _recorded;

        internal WebRouteRun(ReadLatencyRecorder? recorder, string route, string band, HttpContext context, ReadScope scope)
        {
            _recorder = recorder;
            _route = route;
            _band = band;
            _context = context;
            _scope = scope;
        }

        /// <summary>The outcome the route answered with.</summary>
        internal void Complete(ReadOutcome outcome) => _outcome = outcome;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _recorded, 1) != 0)
            {
                return;
            }

            var outcome = _outcome ?? (_context.RequestAborted.IsCancellationRequested ? ReadOutcome.Cancelled : ReadOutcome.Error);
            RecordWebRouteSample(_recorder, _route, ReadScope.Resolve(outcome, _scope.Fallback), _stopwatch.ElapsedMilliseconds, _band, _scope, _context);
        }
    }

    /// <summary>#5506 part 3: one admission sample of a composed-panel run (busy, cancelled in the queue, or queue_wait).</summary>
    private static void RecordAdmissionSample(ReadLatencyRecorder? recorder, string route, ReadOutcome outcome, long elapsedMs, string band, ReadScope scope, string? caller)
    {
        try
        {
            recorder?.Accumulator?.Record(ReadSurface.Compose, route, outcome, elapsedMs, band, scope.InFlightAtStart, 0, caller);
        }
        catch (Exception ex)
        {
            recorder?.Logger?.LogDebug(ex, "Read-latency recording failed for a composed-panel admission.");
        }
    }

    /// <summary>Records one <c>/api/read/&lt;name&gt;</c> sample under the <c>web</c> surface -- <paramref
    /// name="name"/> is already a bounded-cardinality route label (the dispatch table's own tool name, never
    /// caller-supplied text). Never throws into the request: swallowed and logged at Debug, exactly like the
    /// compose path's own recording.</summary>
    private static void RecordWebReadLatency(ReadLatencyRecorder recorder, string name, ReadOutcome outcome, long elapsedMs, HttpContext context, ReadScope scope)
    {
        try
        {
            /* #5506: the band of the window the page asked for (hours, hours_back or days_back on the query string). A
               request that sends none reads the read's own default window, which the catalog row declares (PHours(def),
               PInt("days_back", def)), so it records that window's band; only a read that takes no window records none. */
            var band = WebWindowBand(name, key => First(context, key));
            recorder.Accumulator?.Record(ReadSurface.Web, name, outcome, elapsedMs, band, scope.InFlightAtStart, scope.WaitMs, WebReadCaller(context));
        }
        catch (Exception ex)
        {
            recorder.Logger?.LogDebug(ex, "Read-latency recording failed for /api/read/{Route}.", name);
        }
    }

    /// <summary>The caller of a web or composed-panel read (#5793): <see cref="ReadCaller.OfWebSession(string?)"/> over the
    /// request's session key, a salted hash. Only the hash leaves this method; the key itself is never recorded.</summary>
    internal static string WebReadCaller(HttpContext context)
        => ReadCaller.OfWebSession(context.Request.Cookies[Mcp.DarlingWebHostService.SessionCookieName]);

    /// <summary>The window band of one <c>/api/read/&lt;name&gt;</c> request (#5506): the window the query string sent, else
    /// the default <see cref="CatalogDescriptors"/> declares for that read, else <c>none</c>. Internal so a test can pin a
    /// request that sends no window against the catalog's default.</summary>
    internal static string WebWindowBand(string name, Func<string, string?> sent) =>
        ReadWindowBand.OrFixed(ReadWindowBand.Of(WebWindowHours(name, sent)), name);

    /// <summary>The window in hours of web read <paramref name="name"/>: the one the query string sent, else the default the catalog
    /// row declares (the number <see cref="WebWindowBand"/> bands); null when there is none.</summary>
    internal static double? WebWindowHours(string name, Func<string, string?> sent) =>
        ReadWindowBand.HoursOf(sent, key => CatalogDefaultText(name, key));

    /// <summary>The default the catalog row of read <paramref name="name"/> declares for parameter <paramref name="key"/>,
    /// as text; null when the read is not in the catalog, does not take that parameter, or declares no default.</summary>
    private static string? CatalogDefaultText(string name, string key)
    {
        if (!CatalogDescriptors.TryGetValue(name, out var read))
        {
            return null;
        }

        foreach (var parameter in read.Params)
        {
            if (string.Equals(parameter.Name, key, StringComparison.Ordinal))
            {
                return ReadWindowBand.DefaultText(parameter.Default);
            }
        }

        return null;
    }

    /// <summary>The query string as slow-read arguments. A key name is caller text: only plain identifier characters
    /// ([A-Za-z0-9_.-]) are stored, capped at 64; any other key is dropped and counted under <c>_dropped_keys</c>. A key that
    /// repeats is stored as a JSON array of its values in the order sent; a key sent once is a plain value.</summary>
    internal static JsonObject WebQueryArguments(IEnumerable<KeyValuePair<string, string?>> query)
    {
        var arguments = new JsonObject();
        var droppedKeys = 0;
        foreach (var (key, value) in query)
        {
            if (!key.All(c => c is (>= 'A' and <= 'Z') or (>= 'a' and <= 'z') or (>= '0' and <= '9') or '_' or '.' or '-'))
            {
                droppedKeys++;
                continue;
            }

            var stored = key.Length > 64 ? key[..64] : key;
            if (!arguments.TryGetPropertyValue(stored, out var existing))
            {
                arguments[stored] = value;
            }
            else if (existing is JsonArray repeated)
            {
                repeated.Add(value);
            }
            else
            {
                /* A key sent more than once records every value, in the order sent, so the record shows the request as it
                   was made (#5245); a key sent once stays a plain value. */
                arguments[stored] = new JsonArray(existing?.DeepClone(), JsonValue.Create(value));
            }
        }

        if (droppedKeys > 0)
        {
            arguments["_dropped_keys"] = droppedKeys;
        }

        return arguments;
    }

    /// <summary>The composed-panel route label: <c>compose:</c> plus the panel's source when the catalog knows it,
    /// else <c>compose:unknown</c>, held to 120 characters.</summary>
    internal static string ComposeRouteLabel(JsonObject body)
    {
        var route = body["panel"] is JsonObject panel && panel["source"] is JsonValue sourceValue
            && sourceValue.TryGetValue<string>(out var source) && MeasureCatalog.IsKnownSource(source)
            ? "compose:" + source
            : ComposeUnknownRouteLabel;
        return route.Length > 120 ? route[..120] : route;
    }

    /// <summary>Offers a slow or failed <c>/api/read/*</c> call to the slow-read record (#5097): the query string, as sent,
    /// becomes the arguments. Never throws into the request.</summary>
    private static void OfferWebSlowRead(ReadLatencyRecorder recorder, ReadScope scope, string name, ReadOutcome outcome, long elapsedMs, HttpContext context, string? errorClass)
    {
        if (recorder.SlowReads is null || !recorder.SlowReads.ShouldRecord(outcome, elapsedMs))
        {
            return;
        }

        try
        {
            /* One pair per value, so a key sent more than once keeps every value (WebQueryArguments folds them into an array);
               a key with no value at all (a bare ?key) is one null pair, as before. */
            var arguments = WebQueryArguments(context.Request.Query.SelectMany(q => q.Value.Count > 0
                ? q.Value.Select(v => new KeyValuePair<string, string?>(q.Key, v))
                : [new KeyValuePair<string, string?>(q.Key, null)]));

            recorder.SlowReads.Offer(scope, ReadSurface.Web, name, outcome, elapsedMs, arguments, errorClass, recorder.Logger, WebReadCaller(context));
        }
        catch (Exception ex)
        {
            recorder.Logger?.LogDebug(ex, "Slow-read recording failed for /api/read/{Route}.", name);
        }
    }

    private static void RecordComposeLatency(ReadLatencyRecorder? recorder, JsonObject body, ComposeRunOutcome outcome, long elapsedMs, System.Threading.CancellationToken cancellationToken, ReadFallback? noted = null, ReadScope? scope = null, string? caller = null)
    {
        try
        {
            var measureKey = ComposeRouteLabel(body);

            /* Payload => Ok. outcome.Fault carries the real PostgresException for a store fault the panel
               author could not have caused -- classified the same way the web loop classifies any exception.
               outcome.AuthorSqlState is set only for an author-actionable PostgresException (#4283 M1/#4293
               R2), and that allow-list includes 57014 -- a panel query hitting the store's own
               statement_timeout is a Timeout sample even though it answers the caller at 400, not 500 -- and
               (#4605) 53400, the viewer/mcp role's temp_file_limit refusing the panel's own spill, which is a
               Limit sample for the same reason. Anything else that did not produce a payload (a validation
               BadRequest with no exception at all, or the generic-Exception ServerError arm) is Error, unless
               the caller's own token already explains it. */
            var readOutcome = outcome.Payload is not null
                ? ReadOutcome.Ok
                : outcome.AuthorSqlState == CollectorFaultCancelOrigin.QueryCanceled
                    ? ReadOutcome.Timeout
                    : outcome.AuthorSqlState == ReadOutcomeClassifier.ConfigurationLimitExceeded
                        ? ReadOutcome.Limit
                        : outcome.Fault is not null
                            ? ReadOutcomeClassifier.Classify(outcome.Fault, cancellationToken)
                            : cancellationToken.IsCancellationRequested
                                ? ReadOutcome.Cancelled
                                : ReadOutcome.Error;

            var resolved = ReadScope.Resolve(readOutcome, noted);
            recorder?.Accumulator?.Record(ReadSurface.Compose, measureKey, resolved, elapsedMs, ComposeWindowBand(body),
                scope?.InFlightAtStart ?? 0, scope?.WaitMs ?? 0, caller);

            /* #5097: a slow or failed run also becomes a slow-read row. The panel body is the arguments. */
            if (scope is not null && recorder?.SlowReads is { } slowReads && slowReads.ShouldRecord(resolved, elapsedMs))
            {
                var errorClass = outcome.Fault is not null ? SlowReadLog.ErrorClassOf(outcome.Fault)
                    : outcome.AuthorSqlState is { Length: > 0 } sqlState ? sqlState
                    : resolved == ReadOutcome.Error ? "compose_error" : null;
                slowReads.Offer(scope, ReadSurface.Compose, measureKey, resolved, elapsedMs, body.DeepClone() as JsonObject, errorClass, recorder.Logger, caller);
            }
        }
        catch (Exception ex)
        {
            recorder?.Logger?.LogDebug(ex, "Read-latency recording failed for a composed-panel run.");
        }
    }

    /// <summary>The window band of a composed-panel run (#5506): the windowStart/windowEnd pair when both parse, else the
    /// body's hours, else the default window the runner reads (<see cref="DefaultComposeHours"/>).</summary>
    internal static string ComposeWindowBand(JsonObject body)
    {
        if (ComposeSpec.TryParseUtcInstant(body["windowStart"], out var start) && ComposeSpec.TryParseUtcInstant(body["windowEnd"], out var end))
        {
            return ReadWindowBand.Of(start, end);
        }

        return ReadWindowBand.Of(body["hours"] is JsonValue hoursValue && hoursValue.TryGetValue<int>(out var hours) ? hours : DefaultComposeHours);
    }

    /// <summary>Maps a failed (non-<see cref="ComposeRunOutcome.Payload"/>) <see cref="ComposeRunOutcome"/> onto
    /// its HTTP answer — factored out of the <c>/api/compose/run</c> route (the <see cref="ToHttpResult"/> /
    /// <see cref="MuteRuleToolResult"/> shape) so the three arms are directly testable without a live Postgres
    /// round trip. <see cref="ComposeRunOutcome.Fault"/> (#4283 M1) is checked first: a STORE fault the panel
    /// author could not have caused answers through the same fixed-body backstop #4276 gives an uncaught
    /// exception, logged once, no role/host text on the wire — even though the outcome that carried it is
    /// itself a BadRequest shape (<see cref="ComposeRunOutcome.IsServerError"/> false), it still needs the
    /// 500-class backstop treatment, not a 400. Absent a Fault, <see cref="ComposeRunOutcome.IsServerError"/>
    /// false is a bad spec or #4283's allow-listed "Query failed: {ex.MessageText}" (a Custom Views author
    /// needs that syntax error, statement_timeout cancel included, to fix their own panel) — that text STAYS;
    /// true is the generic-Exception catch's "Error running query: {ex.Message}", reclassified here exactly as
    /// <see cref="ToHttpResult"/>'s ServerError arm reclassifies a tool's envelope, since by this point the
    /// real Exception is equally gone — only the sentence survived the round trip through
    /// <see cref="ComposeRunOutcome"/>.</summary>
    internal static IResult ComposeRunFailureResult(ComposeRunOutcome outcome, string route, ILogger logger, long elapsedMs, CancellationToken requestAborted)
    {
        /* W13: an unknown server is a plain 404 with its message, never a quiet empty answer. */
        if (outcome.IsNotFound)
        {
            return ErrorResult(outcome.Error!, StatusCodes.Status404NotFound);
        }

        if (outcome.Fault is not null)
        {
            DarlingWebFailureLog.Report(logger, route, elapsedMs, outcome.Fault, requestAborted);
            return Results.Json(DarlingWebFailureLog.Body(outcome.Fault), statusCode: DarlingWebFailureLog.StatusCode(outcome.Fault));
        }

        if (!outcome.IsServerError && outcome.AuthorSqlState is { } sqlState)
        {
            /* #4293 round 2 (R2-L1): a panel's own query error still answers its text at 400 so the author can fix
               the panel, but it is logged once too, so store drift (42P01/42703 after a migration that did not
               finish) reaches the service log, not only one author's browser. */
            logger.LogWarning(
                "{Route} answered a Custom View panel's query error at 400 after {ElapsedMs} ms (SQLSTATE {SqlState}): {Error}",
                route, elapsedMs, sqlState, outcome.Error);
        }

        return outcome.IsServerError
            ? ServerErrorResult(outcome.Error!, route, logger, elapsedMs, requestAborted)
            : ErrorResult(outcome.Error!, StatusCodes.Status400BadRequest);
    }

    internal static JsonValue? DbValueToJson(object value) => ComposePanelRunner.DbValueToJson(value);

    /// <summary>
    /// The <c>updated_by</c> stamp for a web edit made by a seat with NO subject — the shared token and the
    /// tokenless loopback operator. A named OIDC seat stamps its own subject instead
    /// (<see cref="DarlingWebSeat.EditorPrincipal"/> is the one place that choice is made), so this is the
    /// fallback, not the web stamp.
    ///
    /// <para><b>The column is therefore heterogeneous, deliberately.</b> A row reads either as a person or as
    /// the surface that wrote it, and a reader cannot tell which by shape alone — a directory that stamps an
    /// opaque <c>sub</c> GUID produces values no more person-like than this one. The alternative was a second
    /// column separating identity from provenance, rejected because the only consumer is a rendered byline
    /// ("updated by X") that wants exactly one value, and splitting it would make every reader join two columns
    /// to recover the string they already display. Nothing filters, sorts or groups on <c>updated_by</c>, so
    /// heterogeneity costs nothing a query has to cope with; if something ever does, that is the moment to
    /// revisit this, not before.</para>
    /// </summary>
    internal const string WebEditorPrincipal = "web";

    /// <summary>
    /// The <c>updated_by</c> stamp for a custom view created/updated over MCP (the <c>create_custom_view</c> /
    /// <c>update_custom_view</c> tools). Unlike <see cref="WebEditorPrincipal"/> this stays an unconditional
    /// constant, and the asymmetry is chosen rather than left over: MCP authenticates a CLIENT on its own
    /// network block with its own shared token and has no sign-in flow to carry a person through, so there is no
    /// subject to prefer. Stamping <c>mcp</c> always is the honest answer for a surface where per-user identity
    /// does not exist, and MCP is such a surface: it has no OIDC path, so a seat-derived stamp here would have
    /// nothing to derive from. Giving MCP identity is a separate piece of work on a separate credential model,
    /// not a line change here.
    /// </summary>
    internal const string McpEditorPrincipal = "mcp";

    /* ── loopback determination (the web host's tokenless-loopback auth arm) ── */

    /// <summary>
    /// PURE: whether a request's remote address is loopback — the CIDR-exemption arm of the web host's auth
    /// gate (<see cref="DarlingWebHostService.DecideWebAuth"/> calls this). A loopback request skips the CIDR
    /// test, because 127.0.0.1 is not inside a LAN CIDR and would otherwise 403 the operator's own browser; it
    /// still has to present a session cookie or token (#1649). Unwraps an IPv4-mapped-IPv6 address
    /// (<c>::ffff:127.0.0.1</c>) then defers to <see cref="IPAddress.IsLoopback"/>; a null/unverifiable remote
    /// is NOT loopback (fail-closed).
    /// </summary>
    internal static bool IsLoopbackRemote(IPAddress? remoteIp)
    {
        if (remoteIp is null)
        {
            return false;
        }

        var ip = remoteIp.IsIPv4MappedToIPv6 ? remoteIp.MapToIPv4() : remoteIp;
        return IPAddress.IsLoopback(ip);
    }

    /* ── Content-Type gate (the CSRF defense on the mutation routes) ── */

    /// <summary>
    /// PURE: whether a mutation request's Content-Type is <c>application/json</c> (a trailing <c>; charset=…</c>
    /// parameter is ignored; the match is case-insensitive). The POST/PUT/DELETE routes require this and return
    /// 415 otherwise. Requiring a non-simple Content-Type forces the browser to CORS-preflight ANY cross-origin
    /// write, and the dashboard answers no preflight — so a simple-request CSRF (which can only send
    /// <c>text/plain</c> / <c>application/x-www-form-urlencoded</c> / <c>multipart/form-data</c>) can never reach
    /// a write. A null/empty or non-json Content-Type is rejected. The test itself lives in
    /// <c>PerformanceMonitor.Common.JsonContentType</c>, shared with Lite's MCP host.
    /// </summary>
    internal static bool IsJsonContentType(string? contentType)
        => PerformanceMonitor.Common.JsonContentType.IsJson(contentType);

    /* ── definition validation (the authority: a bad doc can never be stored) ── */

    /* ── definition validation: the authority lives in Storage (CustomViewValidation, #6390 S2b-B); these forward ── */

    /// <summary>The visualization vocabulary (see <see cref="CustomViewValidation.KnownVizList"/>).</summary>
    internal static IReadOnlyList<string> KnownVizList => CustomViewValidation.KnownVizList;

    /// <summary>Set form of <see cref="KnownVizList"/>.</summary>
    internal static IReadOnlySet<string> KnownViz => CustomViewValidation.KnownViz;

    internal const int MaxDefinitionBytes = CustomViewValidation.MaxDefinitionBytes;

    internal const int MaxPanelCount = CustomViewValidation.MaxPanelCount;

    internal const int MaxNotebookCells = CustomViewValidation.MaxNotebookCells;

    internal const int MaxMarkdownCellBytes = CustomViewValidation.MaxMarkdownCellBytes;

    internal static string[] RefreshChoices => CustomViewValidation.RefreshChoices;

    /// <summary>The declared parameters of one read from <see cref="CatalogDescriptors"/>, null when it has no entry.</summary>
    internal static IReadOnlyList<ReadPanelParam>? ReadPanelParams(string read) =>
        CatalogDescriptors.TryGetValue(read, out var descriptor)
            ? descriptor.Params.Select(p => new ReadPanelParam(p.Name, p.Required)).ToList()
            : null;

    /// <summary>Validates a stored definition; the body lives in <see cref="CustomViewValidation.ValidateDefinition"/>.</summary>
    internal static CustomViewValidation.DefinitionValidation ValidateDefinition(string? definitionJson) =>
        CustomViewValidation.ValidateDefinition(definitionJson, ReadPanelParams);

    /// <summary>Validates a notebook definition; the body lives in <see cref="CustomViewValidation.ValidateNotebookDefinition"/>.</summary>
    internal static CustomViewValidation.DefinitionValidation ValidateNotebookDefinition(JsonObject rootObject) =>
        CustomViewValidation.ValidateNotebookDefinition(rootObject, ReadPanelParams);

    internal static bool IsHexColor(string? color) => CustomViewValidation.IsHexColor(color);

    private static string? TryGetString(JsonObject obj, string key) =>
        obj[key] is JsonValue value && value.TryGetValue<string>(out var s) ? s : null;

    /* ── the read catalog (input truth): read names from BuildReadDispatch, param metadata hand-authored ── */

    /// <summary>One catalog parameter: its WIRE query-string key (NOT the C# method param name), its
    /// <see cref="Type"/> (server/int/text/bool/double), whether it is <see cref="Required"/>, and its
    /// <see cref="Default"/> (null for text/required).</summary>
    internal sealed record CatalogParam(string Name, string Type, bool Required, object? Default);

    /// <summary>One catalog read: its display <see cref="Category"/>, a short <see cref="Description"/>, and its
    /// <see cref="Params"/> (the exact query keys its dispatch lambda binds).</summary>
    internal sealed record CatalogRead(string Category, string Description, IReadOnlyList<CatalogParam> Params);

    private const string TypeServer = "server";
    private const string TypeInt = "int";
    private const string TypeText = "text";
    private const string TypeBool = "bool";
    private const string TypeDouble = "double";

    private const string CatAnalysis = "Analysis";
    private const string CatSessions = "Sessions";
    private const string CatAlerts = "Alerts";
    private const string CatBlocking = "Blocking & Deadlocks";
    private const string CatConfig = "Configuration";
    private const string CatData = "Data";
    private const string CatTrends = "Trends";
    private const string CatOverview = "Overview";
    private const string CatLatch = "Latch & Spinlock";
    private const string CatMemoryGrants = "Memory Grants";
    private const string CatObjects = "Objects & Indexes";
    private const string CatPlanCache = "Plan Cache & Scheduler";
    private const string CatJobs = "Jobs";
    private const string CatPlans = "Plans";
    private const string CatDefaultTrace = "Default Trace";
    private const string CatSystemHealth = "System Health";
    private const string CatFinOps = "FinOps";

    private static CatalogParam PServer() => new("server", TypeServer, false, null);
    private static CatalogParam PHours(int def) => new("hours", TypeInt, false, def);

    /// <summary>
    /// The window ANCHOR (#2495): <c>?as_of=</c> moves the END of a <see cref="PHours"/> window off "now" so a
    /// caller can ask about a past incident. Typed <c>text</c> rather than a new catalog type because it IS a
    /// string on the wire — an ISO-8601 instant the tool parses and REFUSES if it cannot, which is where the
    /// error message belongs; a new type would only teach the picker to render a box it already renders.
    /// </summary>
    private static CatalogParam PAsOf() => new("as_of", TypeText, false, null);
    private static CatalogParam PLimit(int def) => new("limit", TypeInt, false, def);
    private static CatalogParam PTop(int def) => new("top", TypeInt, false, def);
    private static CatalogParam PText(string name) => new(name, TypeText, false, null);

    /// <summary>
    /// #5245: the catalog row of a read that takes the picker's database list. It is the same
    /// <see cref="CatalogParam"/> as <c>PText("database_name")</c> (text, optional, no default), so the served
    /// catalog keeps its shape and a client that sends one <c>database_name</c> is unchanged. The helper exists so
    /// a census can tell a read that takes a LIST (a <c>PDatabases(</c> row, whose dispatch entry calls
    /// <see cref="DatabaseNames"/>) from a read that takes one name.
    /// </summary>
    internal static CatalogParam PDatabases() => PText("database_name");

    /// <summary>A text param with a real default, unlike <see cref="PText(string)"/>'s always-null one — e.g.
    /// get_fleet_overview's detail (#4198), whose default "summary" is part of the contract, not an absence.</summary>
    private static CatalogParam PTextDefault(string name, string def) => new(name, TypeText, false, def);
    private static CatalogParam PReqText(string name) => new(name, TypeText, true, null);

    /// <summary>A REQUIRED integer with no default — a point read's key (a Query Store <c>query_id</c>, a snapshot
    /// <c>session_id</c>), where there is no sensible value to fall back to.</summary>
    private static CatalogParam PReqInt(string name) => new(name, TypeInt, true, null);
    private static CatalogParam PInt(string name, int def) => new(name, TypeInt, false, def);

    /// <summary>
    /// An optional integer with NO default — <see cref="PDouble(string)"/>'s shape for an int. Absent is a third
    /// state, not a number: <c>bucket_minutes</c> absent means the read sizes its points to the window (#3897),
    /// and advertising a default would tell a catalog consumer that sending nothing and sending that width are
    /// the same request.
    /// </summary>
    private static CatalogParam PInt(string name) => new(name, TypeInt, false, null);
    private static CatalogParam PBool(string name, bool def) => new(name, TypeBool, false, def);
    private static CatalogParam PDouble(string name, double def) => new(name, TypeDouble, false, def);

    /// <summary>
    /// A numeric FILTER, which carries no default — the <see cref="PText"/> shape rather than
    /// <see cref="PDouble(string, double)"/>'s.
    ///
    /// <para>The distinction is load-bearing for <c>min_duration_ms</c>: <c>0</c> is a real value there (it
    /// admits every row AND ranks the page by duration), so advertising <c>0</c> as the default would tell a
    /// catalog consumer that sending nothing and sending zero are the same request. Absent means no filter,
    /// which is a third state and not a number.</para>
    /// </summary>
    private static CatalogParam PDouble(string name) => new(name, TypeDouble, false, null);

    private static CatalogRead R(string category, string description, params CatalogParam[] parameters) =>
        new(category, description, parameters);

    /// <summary>
    /// The hand-authored per-read param + category metadata, keyed by read name. There is NO declarative param
    /// source (the dispatch lambdas bind STRING-LITERAL query keys imperatively, and those keys ≠ the C# method
    /// param names — e.g. <c>hours_back</c> ≠ <c>hoursBack</c> — so reflection would send params under the wrong
    /// keys). Each entry's <see cref="CatalogParam.Name"/> is therefore the ACTUAL query-string key its
    /// <see cref="BuildReadDispatch"/> lambda reads. A test pins <c>CatalogDescriptors.Keys ==
    /// BuildReadDispatch().Keys</c>, so a new read forces a catalog entry (it cannot silently ship key-less).
    /// </summary>
    internal static readonly IReadOnlyDictionary<string, CatalogRead> CatalogDescriptors =
        new Dictionary<string, CatalogRead>(StringComparer.Ordinal)
        {
            /* ── analysis reads (AuditConfig/CompareAnalysis/GetAnalysisFacts/GetAnalysisFindings) ── */
            ["audit_config"] = R(CatAnalysis, "Configuration-audit findings for a server.", PServer()),
            ["compare_analysis"] = R(CatAnalysis, "Compare a window's analysis facts against an earlier baseline.", PServer(), PHours(4), PInt("baseline_hours_back"), PAsOf()),
            ["get_analysis_facts"] = R(CatAnalysis, "Raw analysis facts for a window, filtered by source and minimum severity.", PServer(), PHours(4), PText("source"), PDouble("min_severity", 0), PAsOf()),
            ["get_analysis_findings"] = R(CatAnalysis, "Persisted analysis findings for a server.", PServer(), PHours(24), PAsOf(), PLimit(MaxRowLimit), PBool("include_drilldown", false), PBool("full_text", true)),

            /* ── sessions (DarlingMcpSessionTools) ── */
            ["get_active_queries"] = R(CatSessions, "Currently-active queries, optionally blocking-only.", PServer(), PHours(1), PDatabases(), PBool("blocking_only", false), PText("wait_type"), PLimit(50), PAsOf(), PText("tag"), PText("collection_time"), PBool("snapshots_only", false)),
            ["get_session_stats"] = R(CatSessions, "Session-level summary counters for a server.", PServer()),
            ["get_waiting_tasks"] = R(CatSessions, "Tasks currently waiting, with wait type and duration.", PServer(), PHours(1), PLimit(30), PDatabases(), PAsOf()),

            /* ── alerts / mute rules (DarlingMcpAlertTools) ── */
            ["get_alert_history"] = R(CatAlerts, "Recent fired-alert history for a server, newest first and bounded by limit. Excludes operator-dismissed alerts unless include_dismissed is true (dismissed_excluded_count says how many the default hid). include_streak adds streak_days to each row: days in a row this metric fired at this time of day before the alert, muted fires counting.", PServer(), PHours(24), PLimit(50), PAsOf(), PBool("include_dismissed", false), PText("metric_name"), PText("family"), PBool("include_streak", false)),
            ["get_alert_settings"] = R(CatAlerts, "The current alert-settings configuration."),
            ["get_daily_health_report"] = R(CatAlerts, "The Daily Health Report for one day (#7041), built from the store and sent nowhere. date is a local date in the report's time zone, default the latest finished period.", PText("date")),
            /* #5241: a web-only read (no MCP tool behind it, see WebOnlyReadNames): one alert's advice and fix script,
               fetched when its Analysis row is first expanded. The key is the page's own row identity. */
            ["get_alert_details"] = R(CatAlerts, "One fired alert's advice: the heading, fields, advice text and fix script the desktop's Alert Detail shows (never the Apply payload), found by server_id, metric_name and alert_time exactly as get_alert_history reports them. An alert with no advice, or no match, answers an empty details list. For High CPU, Blocking Detected, Blocking Wait Time and Deadlocks Detected it also answers attached_findings: the analysis findings held back from paging because that alert was open.", new CatalogParam("server_id", TypeInt, true, null), PReqText("metric_name"), PReqText("alert_time")),
            ["list_investigation_requests"] = R(CatAlerts, "Level 1 investigation requests alerts opened for an agent, newest first, with the state computed from the lease and expiry.",
                PText("state"), PServer(), PText("family"), PLimit(20)),
            ["get_investigation_reports"] = R(CatAlerts, "Investigation requests with their agent reports and journal: one request by id (with its brief), or those opened in the window.",
                PServer(), PText("request_id"), PHours(24)),
            ["get_mute_rules"] = R(CatAlerts, "The alert mute rules (enabled-only by default).", PBool("enabled_only", true)),
            /* #3598: a read the web host's viewer role can serve — the tool selects only the non-secret carve
               (route_id, metric_match, the GENERATED configured_channels presence column, smtp_recipients,
               enabled), never a destination URL. */
            ["get_notification_routes"] = R(CatAlerts, "The alert-family taxonomy (self-monitor / reports / agent-jobs / performance, with every metric each owns) and the notification routes layered over the parent channels — which channels each route sets, never the destination values."),

            /* ── blocking / deadlocks (DarlingMcpBlockingTools) ── */
            ["get_blocked_process_xml"] = R(CatBlocking, "Blocked-process-report XML captures.", PServer(), PHours(24), PLimit(5), PDatabases(), PAsOf()),
            /* #6292: a web-only read (no MCP tool behind it, see WebOnlyReadNames): the lead-blocker tree of one blocked-process row,
               fetched when the row's Block chain action is used. The key is the page's own row identity. */
            ["get_block_chain"] = R(CatBlocking, "The lead-blocker tree of one blocked-process row: the lead blocker at the root and the sessions it blocks under it, built from the blocked-process reports and DMV blocking snapshots within five minutes of the row, exactly as the desktop viewer builds it.", PServer(), PReqText("event_time"), PReqInt("blocked_spid"), PInt("blocked_ecid", 0)),
            ["get_blocking"] = R(CatBlocking, "Blocking chains observed in the window.", PServer(), PHours(24), PLimit(30), PDatabases(), PAsOf()),
            ["get_blocking_trend"] = R(CatBlocking, "Blocking-event counts over time.", PServer(), PHours(24), PDatabases(), PAsOf()),
            ["get_deadlock_detail"] = R(CatBlocking, "Deadlock graph detail for recent deadlocks.", PServer(), PHours(24), PLimit(5), PBool("full_graph", true), PAsOf()),
            ["get_deadlock_trend"] = R(CatBlocking, "Deadlock counts over time.", PServer(), PHours(24), PAsOf()),
            ["get_deadlocks"] = R(CatBlocking, "Recent deadlocks with victim/resource summary.", PServer(), PHours(24), PLimit(20), PAsOf()),
            ["get_lock_wait_trend"] = R(CatBlocking, "The LCK% family's summed wait ms/sec over time - the aggregate lock-wait lane - with a legend of the types that waited.", PServer(), PHours(24), PAsOf(), PInt("bucket_minutes")),

            /* ── automatic plan correction (DarlingMcpPlanCorrectionTools, #2028) ── */
            ["get_plan_corrections"] = R(CatAnalysis, "Automatic plan correction activity + per-database FORCE_LAST_GOOD_PLAN state.", PServer(), PHours(24), PLimit(50), PAsOf(), PBool("full_text", true), PDatabases()),

            /* ── config: current + history (DarlingMcpConfigTools / DarlingMcpConfigHistoryTools) ── */
            ["get_database_config"] = R(CatConfig, "Database-level configuration for a server.", PServer(), PDatabases()),
            ["get_server_config"] = R(CatConfig, "Server-level configuration (sp_configure) for a server.", PServer()),
            ["get_trace_flags"] = R(CatConfig, "Active trace flags for a server.", PServer()),
            ["get_database_protection"] = R(CatConfig, "Per database, the last good CHECKDB, the last full backup and the last log backup with their ages, and which databases are overdue (full backup 7 days, log backup 60 minutes, CHECKDB 14 days). Azure SQL Database and Managed Instance show the age and are never overdue. RDS shows the backup ages and is never overdue on them, but its CHECKDB is judged.", PServer(), PHours(24), PAsOf(), PBool("overdue_only", false)),
            ["get_identity_capacity"] = R(CatConfig, "How full each identity column is and how long until it runs out, per scanned database (the fullest columns above 10 percent used).", PServer(), PHours(168), PAsOf(), PDatabases(), PLimit(200)),
            ["get_database_config_changes"] = R(CatConfig, "Database-configuration changes over time.", PServer(), PHours(168), PAsOf(), PDatabases()),
            ["get_database_scoped_config"] = R(CatConfig, "Database-scoped configuration for a database.", PServer(), PDatabases()),
            ["get_query_store_health"] = R(CatConfig, "Per-database Query Store health: actual vs desired state, readonly_reason, storage vs cap, and the two capture modes (query_capture_mode ALL / AUTO / CUSTOM / NONE, wait_stats_capture_mode ON / OFF; null on a pre-rung row or a pre-2017 engine).", PServer(), PDatabases()),
            ["get_server_config_changes"] = R(CatConfig, "Server-configuration changes over time.", PServer(), PHours(168), PAsOf()),
            ["get_trace_flag_changes"] = R(CatConfig, "Trace-flag changes over time.", PServer(), PHours(168), PAsOf()),
            ["get_known_patterns"] = R(CatConfig, "The operator-written known-pattern entries (a note on a recurring alert pattern); get_alert_history rows carry the matches.", PServer(), PText("metric_name")),
            ["get_change_watches"] = R(CatConfig, "A server's change watches, open and closed: each recorded change opens a 60 minute watch, with its changes, measures and result. watch_id reads one watch in any state.", PServer(), PText("state"), PLimit(20), PText("watch_id")),

            /* ── core data reads (DarlingMcpDataTools + long-query / fleet tools) ── */
            ["get_collection_health"] = R(CatData, "Per-collector collection health for a server. server=* with collector_name answers for that one collector on every enabled server (hours: default 24).", PServer(), PText("collector_name")),
            ["get_collection_log"] = R(CatData, "Raw per-run collector log for a server, newest first — or slowest first when min_duration_ms is supplied.", PServer(), PHours(24), PLimit(200), PAsOf(), PText("collector_name"), PDouble("min_duration_ms"), PText("status")),
            ["get_current_waits_trend"] = R(CatData, "Waiting-task and blocked-session series over time.", PServer(), PHours(4), PDatabases(), PAsOf()),
            ["get_client_timeouts"] = R(CatData, "Client-timeout (attention) counts, rate and top databases, applications and statements over the window, with the newest sampled events (#5640).", PServer(), PHours(1), PLimit(20), PAsOf()),
            ["get_blocking_stats"] = R(CatData, "Blocking duration and deadlock severity per minute.", PServer(), PHours(24), PDatabases(), PAsOf()),
            ["get_cpu_utilization"] = R(CatData, "CPU utilization over time.", PServer(), PHours(4), PAsOf(), PInt("bucket_minutes")),
            ["get_file_io_stats"] = R(CatData, "Per-file IO stall/throughput stats.", PServer(), PDatabases()),
            ["get_memory_clerks"] = R(CatData, "Top memory clerks by allocation.", PServer()),
            ["get_memory_stats"] = R(CatData, "Server memory summary counters.", PServer()),
            ["get_perfmon_stats"] = R(CatData, "Perfmon counter values, filtered by counter/instance.", PServer(), PText("counter_name"), PText("instance_name")),
            ["get_query_heatmap"] = R(CatData, "Query counts per (time bin x log-magnitude bucket) - the viewer's Query Heatmap as a table.", PServer(), PHours(24), PText("metric"), PDatabases(), PInt("bucket_minutes", 5), PLimit(500), PAsOf()),
            ["get_query_store_regressions"] = R(CatData, "Queries whose Query Store performance got WORSE vs their baseline.", PServer(), PHours(24), PDatabases(), PLimit(50), PBool("full_text", true), PAsOf()),
            ["get_query_store_clutter"] = R(CatData, "The Query Store CLUTTER view: per database the collector's read cost (how often and by how much it was the slowest fan-out item), plan churn (plans per query, arrivals per day, one-shot plans) and the options row, each with raw numbers and a decomposed verdict; ONE per-server overhead block (non-sleep QDS_* wait deltas with the excluded sleep waits named, and the Query Store memory clerk). Replicas excluded by architecture with the reason on the row; query_capture_mode (ALL / AUTO / CUSTOM / NONE) carried with capture_mode_known beside it, null meaning a capture older than the V137 rung rather than NONE. window_truncated says the raw tier did not hold the whole window. Composed from collected rows - no new query against the server.", PServer(), PHours(24), PLimit(DarlingMcpQueryStoreClutterTools.DefaultLimit), PBool("include_fleet_median", false), PAsOf(), PDatabases()),
            ["get_resource_use_by_tag"] = R(CatData, "Resource use rolled up by query tag, one section per source (plan cache, Query Store, procedures, blocking, deadlocks); sections are never summed. Omit the server for the fleet.", PServer(), PHours(24), PText("tag"), PLimit(DarlingMcpQueryTagRollupTools.DefaultLimit), PAsOf()),
            ["get_query_store_query_history"] = R(CatData, "One Query Store query's executions, duration and CPU per plan over the window (requires database_name, query_id).", PReqText("database_name"), PReqInt("query_id"), PServer(), PHours(24), PAsOf()),
            ["get_query_store_hints"] = R(CatData, "Which Query Store hints are in place: one entry per hint in its database's newest capture, with the query, the hint text, its source, whether the engine records a failure (FAILING or NO_FAILURE) and the query text (database_name optional). SQL Server 2022 and later.", PServer(), PHours(24), PAsOf(), PDatabases()),
            ["get_forced_plans"] = R(CatData, "Whether each Query Store force works: one entry per plan forced in its database's newest capture, with a state (WORKING, NOT_EXERCISED, IGNORED, FAILING, UNKNOWN_PLAN_NOT_COLLECTED or REMOVED) and the evidence (database_name optional).", PServer(), PHours(24), PAsOf(), PDatabases()),
            ["get_plan_force_precheck"] = R(CatData, "Reads one Query Store plan before anyone forces it: verdict blocked (plan_not_found or plan_already_forced, a force that cannot work) or not_blocked (never safe), the warnings with stable codes for the agent's own instructions to weigh, what a person should read first, and the checks the store could not answer (requires database_name, query_id, plan_id; compare_plan_id optional).", PReqText("database_name"), PReqInt("query_id"), PReqInt("plan_id"), PInt("compare_plan_id"), PServer(), PHours(24), PAsOf()),
            ["get_query_store_aborts"] = R(CatData, "Queries ranked by Query Store runs that ended aborted (a client timeout or cancel) or with an exception, with each query's share of its own runs, the average aborted duration and the newest failure time; window_truncated says the store did not hold the whole window.", PServer(), PHours(24), PTop(20), PText("database_name"), PAsOf(), PBool("full_text", false)),
            ["get_cpu_by_database"] = R(CatData, "Databases ranked by CPU over the window with each one's share of the server total, its heaviest statement and the procedure stats' CPU beside it (never added to it).", PServer(), PHours(24), PTop(20), PAsOf()),
            ["get_load_by_caller"] = R(CatSessions, "Active-query snapshots grouped by host, program and login, ranked by CPU, with each group's share and top statements.", PServer(), PHours(1), PTop(20), PInt("statements", 3), PAsOf()),
            ["get_query_pileups"] = R(CatSessions, "Capture times when min_sessions or more sessions ran the same query_hash at once, with the main waits and the head blocker from the nearest blocking snapshot.", PServer(), PHours(24), PInt("min_sessions", 5), PTop(10), PAsOf()),
            ["get_query_store_top"] = R(CatData, "Top Query Store queries in the window, optionally filtered by execution outcome or to one exact module before ranking; window_truncated says the raw tier did not hold the whole window (effective_hours_back how far it reached).", PServer(), PHours(24), PTop(20), PDatabases(), PAsOf(), PText("execution_type"), PText("module_name"), PBool("full_text", false), PText("tag")),
            ["get_long_query_completions"] = R(CatData, "Completed long-running queries captured by the XE trace.", PServer(), PHours(24), PLimit(30), PAsOf(), PDatabases(), PText("tag")),
            ["get_server_properties"] = R(CatData, "Server properties/inventory for a server.", PServer()),
            ["get_tempdb_trend"] = R(CatData, "tempdb space usage over time.", PServer(), PHours(24), PAsOf(), PInt("bucket_minutes")),
            ["get_top_procedures_by_cpu"] = R(CatData, "Top stored procedures, ranked by CPU (the default), duration, reads or executions (order_by).", PServer(), PHours(24), PTop(20), PDatabases(), PTextDefault("order_by", "cpu"), PAsOf(), PTextDefault("detail", "summary"), PText("tag")),
            ["get_top_queries_by_cpu"] = R(CatData, "Top queries, ranked by CPU (the default), duration, reads or executions (order_by), optionally parallel-only / min-DOP.", PServer(), PHours(24), PTop(20), PDatabases(), PBool("parallel_only", false), PInt("min_dop", 0), PTextDefault("order_by", "cpu"), PAsOf(), PTextDefault("detail", "summary"), PText("tag")),
            ["get_pg_top_queries"] = R(CatData, "Top PostgreSQL query shapes by total execution time (Aurora targets).", PServer(), PHours(24), PLimit(20), PBool("full_text", true), PAsOf()),
            ["get_pg_plans"] = R(CatData, "Captured PostgreSQL execution plans, grouped by shape. Plans are redacted at collection.", PServer(), PHours(24), PLimit(10), PText("query_id"), PAsOf()),
            ["get_pg_plan_capture_readiness"] = R(CatData, "Whether a PostgreSQL target can capture execution plans at all, facet by facet, with the remedy for each step that is not in place. Read this when a plan or target-log read is empty.", PServer(), PHours(24), PLimit(25), PAsOf()),
            ["get_pg_logging_audit"] = R(CatData, "Whether a PostgreSQL target's logging settings (log_min_duration_statement, log_lock_waits, log_temp_files, log_autovacuum_min_duration, log_checkpoints, log_connections, log_disconnections) are producing the lines they could, per setting: verdict, what it unlocks, the recommended value with its cost, and the remedy in the hosting flavour's syntax. Judged from the newest stored pg_server_config snapshot, not the live server; plan capture's own settings are listed and pointed at get_pg_plan_capture_readiness.", PServer()),
            ["get_pg_wraparound_risk"] = R(CatData, "PostgreSQL XID/MultiXact freeze headroom per database.", PServer(), PHours(24), PAsOf()),
            ["get_pg_xmin_horizon"] = R(CatData, "What is holding back the PostgreSQL xmin horizon, by cause.", PServer(), PHours(24), PAsOf()),
            ["get_pg_replication_slots"] = R(CatData, "PostgreSQL replication slot health, including whether retained WAL is still growing.", PServer(), PHours(24), PAsOf()),
            ["get_pg_autovacuum_health"] = R(CatData, "PostgreSQL tables behind on vacuum or analyze, ranked by how far past each table's own threshold.", PServer(), PHours(24), PLimit(20), PAsOf()),
            ["get_pg_io_stats"] = R(CatData, "PostgreSQL I/O by backend type, object and context, differenced across the window.", PServer(), PHours(24), PLimit(20), PAsOf()),
            ["get_pg_wait_stats"] = R(CatData, "Top PostgreSQL wait events in the window (Aurora targets).", PServer(), PHours(24), PLimit(20), PAsOf()),
            ["get_pg_cpu_utilization"] = R(CatData, "Instance CPU utilization over time from AWS Performance Insights (Aurora/RDS targets).", PServer(), PHours(4), PAsOf(), PInt("bucket_minutes")),
            ["get_pg_wait_sampling"] = R(CatData, "Sampled PostgreSQL waits by query shape, from pg_wait_sampling - the stock-PostgreSQL counterpart of get_pg_wait_stats. Sample counts, not measured durations; event_type CPU means running rather than waiting.", PServer(), PHours(24), PLimit(20), PAsOf()),
            ["get_pg_kernel_stats"] = R(CatData, "Per-query OS CPU (user and system), device bytes and major faults, from pg_stat_kcache. The CPU half of the elapsed time get_pg_top_queries reports.", PServer(), PHours(24), PLimit(20), PAsOf()),
            ["get_pg_predicate_stats"] = R(CatData, "Which columns queries actually filter on and how selectively, from pg_qualstats. SAMPLED counts - the evidence behind an index recommendation.", PServer(), PHours(24), PLimit(25), PAsOf()),
            ["get_pg_index_bloat"] = R(CatData, "ESTIMATED PostgreSQL index bloat from catalog statistics: reclaimable bytes, the modelled inputs behind them, and a reason instead of an answer where the statistics cannot support one. Answerless rows sort FIRST by design, so pass answered_only for the ranking on a server whose answerless population exceeds the row cap — read the reach field, which says whether raising the limit would help at all. The complete btree census, no size floor, so it counts MORE indexes than get_pg_index_usage (which floors at 64 kB).", PServer(), PHours(168), PLimit(25), PBool("answered_only", false), PAsOf()),
            ["get_pg_column_stats"] = R(CatData, "Per-column distribution statistics the PLANNER uses: n_distinct, null fraction, correlation and top-value frequency.", PServer(), PHours(168), PLimit(25), PAsOf()),
            ["get_pg_buffer_usage"] = R(CatData, "What is resident in the shared buffer pool per relation, from pg_buffercache. Residency, not read volume.", PServer(), PHours(24), PLimit(25), PAsOf()),
            ["get_pg_extensions"] = R(CatData, "Which PostgreSQL extensions are installed, outdated, available or absent, per database. Usually the reason another read is empty. Rows are databases x extension names, so a multi-database host outgrows any row cap: read install_census for what is installed where (aggregated, limit-independent), or pass database_name to complete one database at a time.", PServer(), PHours(168), PLimit(50), PText("database_name"), PAsOf()),
            ["get_pg_lock_stats"] = R(CatData, "Sampled PostgreSQL lock activity by type, mode and relation. A sample of pg_locks, not an event log; for who blocks whom use get_pg_blocking.", PServer(), PHours(24), PLimit(25), PAsOf()),
            ["get_pg_write_stats"] = R(CatData, "Checkpoint and WAL write activity across the window: timed versus requested checkpoints, buffers written by whom, and WAL volume.", PServer(), PHours(24), PAsOf()),
            ["get_pg_server_config"] = R(CatData, "The PostgreSQL server's configuration from pg_settings, non-default first, saying where each value came from and whether changing it needs a restart. Reports pending_restart, where the file and the running server disagree. Per-database and per-role overrides (ALTER DATABASE / ALTER ROLE ... SET) are a separate section, present only when the cluster has any: those are what sessions in that database or as that role actually run with.", PServer(), PLimit(100), PBool("include_defaults", false)),
            ["get_pg_server_config_changes"] = R(CatData, "PostgreSQL configuration parameters whose value CHANGED in the window, old beside new - per-database and per-role overrides included, as changed, set or reset. Nothing else can reconstruct this after the fact.", PServer(), PHours(168), PLimit(100), PAsOf()),
            ["get_pg_deadlocks"] = R(CatData, "PostgreSQL deadlocks reported in the window, with the victim, the lock modes and resources, and the victim's statement. Needs nothing configured on the target.", PServer(), PHours(24), PLimit(25), PAsOf()),
            ["get_pg_deadlock_detail"] = R(CatData, "PostgreSQL deadlock graphs in full: the whole wait graph as the server wrote it, and every participant's statement with its literals normalized. Newest first, or one by deadlock_hash.", PServer(), PText("deadlock_hash"), PLimit(5)),
            ["get_pg_log_events"] = R(CatData, "PostgreSQL server-log events in the window, classified by family (error, connection, lock_wait, temp_file, autovacuum, checkpoint), newest first, SQL in them normalized. The 'check the error log' read. Filter by family and min_severity; the page says what bounded it.", PServer(), PHours(24), PText("family"), PText("min_severity"), PLimit(50), PAsOf()),
            ["get_pg_wait_trend"] = R(CatTrends, "One PostgreSQL wait event over time, per second. Omit wait_event to follow whichever dominates. Estimates from a sampling profiler, so the shape is the finding.", PServer(), PText("wait_event"), PHours(24), PAsOf()),
            ["get_pg_query_duration_trend"] = R(CatTrends, "One PostgreSQL statement over time by queryid: what a single execution cost in each interval. Omit queryid for the busiest statement. The regression read.", PServer(), PText("queryid"), PHours(24), PAsOf(), PInt("bucket_minutes")),
            ["get_pg_io_trend"] = R(CatTrends, "One PostgreSQL (backend_type, context) pair over time: I/O rates per second, the hit ratio per point, and latency where the server measures it. Omit both to follow whichever pair moved the most I/O.", PServer(), PText("backend_type"), PText("context"), PHours(24), PAsOf(), PInt("bucket_minutes")),
            ["get_pg_database_trend"] = R(CatTrends, "One PostgreSQL database over time: temp-file spills, the cache hit ratio per point with its worst interval, deadlocks and the rollback share. Omit database for the biggest spiller. The cumulative ratio is a lifetime average that hides a cliff.", PServer(), PText("database"), PHours(24), PAsOf(), PInt("bucket_minutes")),
            ["get_pg_replication_stats"] = R(CatData, "Health of CONNECTED replicas from pg_stat_replication, with the worst lag in the window beside the latest. Counterpart of get_pg_replication_slots.", PServer(), PHours(24), PLimit(25), PAsOf()),
            ["get_pg_blocking"] = R(CatData, "PostgreSQL blocking chains that were sampled, with the root blocker attributed. A sample, not an event log.", PServer(), PHours(24), PLimit(50), PAsOf()),
            ["get_pg_database_stats"] = R(CatData, "PostgreSQL per-database temp-file spills, cache hit ratio, deadlocks and commit/rollback split, differenced across the window.", PServer(), PHours(24), PLimit(20), PAsOf()),
            ["get_pg_index_usage"] = R(CatData, "PostgreSQL per-index scan counts and size, with the constraint, replica-identity and validity facts that decide whether an unused index can actually be dropped. Reports indexes of at least 64 kB plus any invalid index at any size, so it counts FEWER indexes than get_pg_index_bloat (the complete btree census) — that floor is the whole difference.", PServer(), PHours(168), PLimit(25), PAsOf()),
            ["get_pg_table_bloat"] = R(CatData, "PostgreSQL per-table bloat ESTIMATE with its measured sizes and dead-tuple counts. The estimate is suppressed, not captioned, when its statistics cannot be trusted.", PServer(), PHours(168), PLimit(25), PAsOf()),
            ["get_pg_session_states"] = R(CatData, "PostgreSQL sessions holding a transaction open, and whether each one actually pins the xmin horizon - which is not the same question as how long it has been idle in transaction.", PServer(), PHours(24), PLimit(25), PAsOf()),
            ["get_wait_stats"] = R(CatData, "Top wait statistics in the window.", PServer(), PHours(24), PLimit(20), PAsOf()),
            ["get_wait_trend"] = R(CatData, "One wait type's totals over time (requires wait_type).", PReqText("wait_type"), PServer(), PHours(24), PAsOf(), PInt("bucket_minutes")),
            ["get_wait_types"] = R(CatData, "The wait types observed in the window.", PServer(), PHours(24), PAsOf()),
            ["find_database"] = R(CatData, "Which monitored server holds a database: exact, case-insensitive name match against each server's newest database list, with the registry and display names, whether the server is still monitored and when the list was last seen (requires database_name).", PReqText("database_name")),
            ["list_servers"] = R(CatData, "The monitored servers known to the store."),
            ["list_query_tags"] = R(CatData, "The query tag tree with each tag's hand assignments, their precedence over rules, and whether the tag map has caught up."),

            /* ── trends (DarlingMcpTrendTools) ── */
            ["get_file_io_trend"] = R(CatTrends, "File I/O read and write latency over time per database and file type, heaviest stall first; database_name charts one database per file.", PServer(), PHours(24), PAsOf(), PInt("bucket_minutes"), PDatabases()),
            ["get_memory_trend"] = R(CatTrends, "Memory usage over time.", PServer(), PHours(24), PAsOf(), PInt("bucket_minutes")),
            ["get_server_trend"] = R(CatTrends, "One instance trend over time, picked by metric: total_waits, cpu_scheduler, memory_clerks, plan_cache, latch, spinlock, session_stats, collector_duration, tempdb_file_io, tempdb_size or file_io_throughput.", PReqText("metric"), PServer(), PHours(24), PAsOf(), PInt("bucket_minutes"), PText("clerk_types"), PText("names")),
            ["get_perfmon_trend"] = R(CatTrends, "One perfmon counter over time (requires counter_name).", PReqText("counter_name"), PServer(), PHours(24), PAsOf(), PInt("bucket_minutes")),
            /* #3653 item 17: the four reads below disclose the WINDOW floor as window_truncated (beside
               effective_start / effective_hours_back) — not the page dialect's truncated, which they never had. */
            ["get_procedure_duration_trend"] = R(CatTrends, "Stored-procedure elapsed ms/sec + executions/sec over time, or one procedure's with procedure_name (its full_name from the Top Procedures grid, plus one database_name); window_truncated says the tier did not hold the whole window.", PServer(), PHours(24), PAsOf(), PInt("bucket_minutes"), PDatabases(), PText("procedure_name")),
            ["get_query_duration_trend"] = R(CatTrends, "Query elapsed ms/sec + executions/sec over time (per-interval rates, not percentiles); window_truncated says the tier did not hold the whole window.", PServer(), PHours(24), PAsOf(), PInt("bucket_minutes"), PDatabases()),
            ["get_query_store_duration_trend"] = R(CatTrends, "Query Store duration ms/sec + executions/sec over time; window_truncated says the tier did not hold the whole window.", PServer(), PHours(24), PAsOf(), PDatabases()),
            ["get_query_trend"] = R(CatTrends, "One query's metrics over time (requires query_hash + database_name); window_truncated says the tier did not hold the whole window.", PReqText("query_hash"), PReqText("database_name"), PServer(), PHours(24), PAsOf()),

            /* ── health / overview (DarlingMcpHealthTools / DarlingMcpFleetTools) ── */
            ["get_server_summary"] = R(CatOverview, "A one-shot health summary for a server.", PServer()),
            /* #6291: a web-only read (no MCP tool behind it, see WebOnlyReadNames): the four baselines (CPU, waits per second, I/O latency, blocking)
               at the window start that the Overview strips draw their bands and anomaly markers from. Keyed by the window start the page draws. */
            ["get_overview_baselines"] = R(CatOverview, "The four baselines the Overview strips draw their band and anomaly markers from (CPU, waits per second, I/O latency, blocking): each bucket's mean and effective standard deviation at the window start, null where there is not enough history.", PServer(), PReqText("window_start")),
            ["get_daily_summary"] = R(CatOverview, "The daily health summary (optionally for a specific date).", PServer(), PText("summary_date")),
            ["get_daily_summary_range"] = R(CatOverview, "One daily health summary per collected day over a span of days - the Performance Calendar's month grid.", PServer(), PInt("days_back", 30), PAsOf()),
            ["get_fleet_overview"] = R(CatOverview, "The banded cross-server fleet roll-up.", PHours(DefaultFleetHours), PTextDefault("detail", "summary"), PBool("worst_only", false), PText("band")),
            ["get_sweep_reports"] = R(CatOverview, "The scheduled Fleet Sweep Reports: the sweep timeline for the window, the newest sweep in full, and the watch-item worklist - or one sweep by sweep_id (a string; the ids do not survive a JSON number round trip).", PHours(1), PAsOf(), PText("sweep_id"), PText("watch_state")),
            ["get_ag_health"] = R(CatOverview, "Availability Group topology: replicas and per-database secondary state.", PServer(), PLimit(DarlingMcpAgTools.DefaultGroupLimit)),
            ["get_store_metrics"] = R(CatOverview, "The monitoring store's own size/compression/growth (self-metrics): a summary by default, object_kind to list one kind, an exact object_name for one object's daily series.", PInt("days_back", 30), PText("object_kind"), PText("object_name"), PLimit(DarlingMcpStoreMetricsTools.DefaultLimit)),
            ["get_store_log"] = R(CatOverview, "What the monitoring store's OWN PostgreSQL server log recorded - a per-class census with the capture denominator beside it, not the lines. Deliberately unbanded.", PHours(24), PLimit(DarlingMcpStoreLogTools.DefaultRetainedLimit), PAsOf()),
            ["get_store_query_history"] = R(CatOverview, "The monitoring store's OWN SQL statements hour by hour (pg_stat_statements deltas the service snapshots every hour): the top statements over the window by time spent, or one statement's hourly series with query_id.", PText("query_id"), PText("role"), PHours(DarlingMcpStoreQueryHistoryTools.DefaultHours), PTop(DarlingMcpStoreQueryHistoryTools.DefaultTop)),
            ["get_store_query_stats"] = R(CatOverview, "The monitoring store's OWN SQL statements ranked by server-side cost (pg_stat_statements), split by the role that ran them (on a managed store: the web viewer, MCP tools, the Darling Viewer, or the service itself).", PText("role"), PText("order_by"), PTop(DarlingMcpStoreQueryStatsTools.DefaultTop), PBool("full_text", false)),
            ["get_store_host"] = R(CatOverview, "The monitoring store's own HOST profile: platform/RAM/data volume, PostgreSQL/TimescaleDB facts, and a verdict per sizing-relevant setting against what this host would derive today - is the store sized right. No parameters; a snapshot."),
            ["get_collector_cost"] = R(CatOverview, "The monitoring tool's OWN per-collector cost on the monitored servers (self-monitoring) - which of our collectors is the most expensive to run. Pass collector_name for that one collector's daily trend instead of the ranked list.", PInt("days_back", 7), PText("collector_name")),
            ["get_collector_stall_probes"] = R(CatOverview, "The out-of-band server-wide wait samples taken while one of OUR collectors was stalled mid-read - what the monitored instance was doing inside the window the sequential sweep records nothing in. Carries the outcome census beside the samples, deliberately unbanded.", PServer(), PInt("days_back", 7), PLimit(DarlingMcpStallProbeTools.DefaultLimit)),
            ["get_oversized_plan_backlog"] = R(CatOverview, "The cached plans this tool measured as too large to capture inline, and what the out-of-band sweep has done about each one: per server the three verdict buckets (pending/captured/expired, a strict partition), the attempt figures on still-pending rows, the newest capture and expiry instants, and observed_bytes min/median/max, with the per-collector census beside them. Takes no window - a worklist updated in place, not a series. Pass server_name with include_rows for the claim keys.", PServer(), PBool("include_rows", false), PLimit(DarlingMcpOversizedPlanBacklogTools.DefaultLimit)),
            ["get_slow_reads"] = R(CatOverview, "The monitoring tool's OWN slow or failed reads, newest first - each with the arguments it was asked with, the source that answered and its slowest statements. Recorded when a read took 5 seconds or more or ended in a timeout, error or limit. Optional server, surface (web/compose/mcp) and route filter.", PHours(DarlingMcpSlowReadTools.DefaultHours), PServer(), PText("surface"), PText("route"), PLimit(DarlingMcpSlowReadTools.DefaultLimit)),
            ["get_read_latency"] = R(CatOverview, "The monitoring tool's OWN read-latency history (self-monitoring) - which of the web dashboard's or MCP server's own reads is really slow, and how often it times out. p50/p95/p99 are bucket upper-bound estimates, per (surface, route, window band: 1h, 24h, 7d, 7d+ or none), so one route can return a row per band; sorted p95 desc. window_band is the width of the window the read asked for (its default window when it sent none). max_in_flight is the most other reads running when one started; wait_total_ms is the summed wait for a store connection. Optional surface (web/compose/mcp) and route filter. fallbacks and gate_failures count reads that fell back to raw.", PHours(DarlingMcpReadLatencyTools.DefaultHours), PText("surface"), PText("route"), PLimit(DarlingMcpReadLatencyTools.DefaultLimit)),

            /* ── latch / spinlock (DarlingMcpLatchSpinlockTools) ── */
            ["get_latch_stats"] = R(CatLatch, "Top latch waits in the window.", PServer(), PHours(24), PTop(10), PAsOf()),
            ["get_spinlock_stats"] = R(CatLatch, "Top spinlock activity in the window.", PServer(), PHours(24), PTop(10), PAsOf()),

            /* ── memory grants (DarlingMcpMemoryGrantTools) ── */
            ["get_memory_grants"] = R(CatMemoryGrants, "Active/recent memory grants. chart=true answers the whole window as one point per bucket per resource pool instead of grants[] and window[]; bucket_minutes picks the width.", PServer(), PHours(1), PAsOf(), PBool("chart", false), PInt("bucket_minutes")),
            ["get_memory_pressure_events"] = R(CatMemoryGrants, "Memory-pressure events in the window.", PServer(), PHours(24), PAsOf()),
            ["get_resource_semaphore"] = R(CatMemoryGrants, "Resource-semaphore state over time.", PServer(), PHours(24), PAsOf()),

            /* ── object / index stats (DarlingMcpObjectStatsTools) ── */
            ["get_database_sizes"] = R(CatObjects, "Per-database size breakdown.", PServer(), PDatabases()),
            ["get_pvs_stats"] = R(CatObjects, "ADR persistent version store state per database, with an optional top-5 size trend.", PServer(), PInt("trend_hours_back", 0), PDatabases()),
            ["get_index_usage"] = R(CatObjects, "Index usage (seeks/scans/updates) per index. Unused-first, so pass database_name unless you want a server-wide sweep; the answer carries matching_index_count and truncated.", PServer(), PDatabases(), PLimit(200)),
            ["get_object_locking"] = R(CatObjects, "Per-object locking/contention stats.", PServer(), PLimit(200), PDatabases(), PText("detail_database"), PText("detail_schema"), PText("detail_table"), PText("detail_index")),
            ["get_table_index_sizes"] = R(CatObjects, "Per-table/index size breakdown.", PServer(), PDatabases()),

            /* ── plan cache / scheduler (DarlingMcpPlanCacheSchedulerTools) ── */
            ["get_cpu_scheduler_pressure"] = R(CatPlanCache, "CPU scheduler pressure indicators from the newest snapshot within the window.", PServer(), PHours(24), PAsOf()),
            ["get_plan_cache_bloat"] = R(CatPlanCache, "Plan-cache bloat / single-use plan indicators.", PServer(), PHours(24), PAsOf()),

            /* ── jobs (DarlingMcpJobTools) ── */
            ["get_running_jobs"] = R(CatJobs, "Currently-running SQL Agent jobs.", PServer()),
            ["get_agent_alerts"] = R(CatJobs, "The SQL Server Agent alerts defined on an instance, and the gaps in them (no alert for severities 19 to 25 or errors 823 to 825, an alert that notifies no one, a disabled alert, an alert that fired and told no one).", PServer(), PHours(24), PAsOf()),
            ["get_job_history"] = R(CatJobs, "Retained SQL Agent job runs, newest first; omit server for every server.", PServer(), PHours(24), PText("job_name"), PText("status"), PText("category"), PLimit(100), PInt("min_duration_seconds"), PAsOf()),

            /* ── stored plan XML (DarlingMcpPlanTools; the analyze_*_plan compute family stays excluded) ── */
            ["get_plan_xml"] = R(CatPlans, "The stored execution-plan XML for a query (requires query_hash).", PReqText("query_hash"), PServer(), PText("database_name")),
            /* #5228: the three grid-row plan reads — point reads by a row's own key, so no window (no PHours/PAsOf). */
            ["get_query_store_plan_xml"] = R(CatPlans, "The stored Query Store plan XML for a query (requires database_name, query_id).", PReqText("database_name"), PReqInt("query_id"), PServer(), PInt("plan_id")),
            ["get_procedure_plan_xml"] = R(CatPlans, "The stored plan XML for a procedure (requires sql_handle).", PReqText("sql_handle"), PServer()),
            ["get_active_query_plan_xml"] = R(CatPlans, "The plan captured with one Active Queries row (requires collection_time, session_id).", PReqText("collection_time"), PReqInt("session_id"), PServer(), PInt("request_id", 0), PBool("live", false)),
            /* #5236: the Blocking and Deadlocks grids' plan reads, the same point-read shape: no window (no PHours/PAsOf). */
            ["get_blocking_plan_xml"] = R(CatPlans, "The plan stored with one Blocking row (requires event_time, blocked_spid, blocking_spid); side=blocking for the blocker's plan.", PReqText("event_time"), PReqInt("blocked_spid"), PReqInt("blocking_spid"), PServer(), PInt("blocked_ecid", 0), PInt("blocking_ecid", 0), PTextDefault("side", "blocked"), PText("database_name")),
            ["get_deadlock_plan_xml"] = R(CatPlans, "The victim's plan stored with one Deadlocks row (requires collection_time, deadlock_time).", PReqText("collection_time"), PReqText("deadlock_time"), PServer(), PText("victim_process_id"), PText("database_name")),
            /* #5233: a repro script built from the stored text and plan — store-only, nothing runs. */
            ["get_query_repro_script"] = R(CatPlans, "A T-SQL repro script built from a stored query's text and plan (requires kind and its key).", PReqText("kind"), PServer(), PText("database_name"), PText("query_hash"), PInt("query_id"), PInt("plan_id"), PText("collection_time"), PInt("session_id"), PInt("request_id", 0)),

            /* ── default trace (DarlingMcpDefaultTraceTools) ── */
            ["get_default_trace_events"] = R(CatDefaultTrace, "Default-trace events (file growth, DDL, security).", PServer(), PHours(24), PLimit(100), PAsOf(), PDatabases()),

            /* ── system_health parse-on-read family (DarlingMcpHealthParserTools) ── */
            ["get_health_parser_cpu_tasks"] = R(CatSystemHealth, "system_health: CPU-bound task snapshots.", PServer(), PHours(24), PLimit(50), PAsOf()),
            ["get_health_parser_io_issues"] = R(CatSystemHealth, "system_health: IO-latency issues.", PServer(), PHours(24), PLimit(50), PAsOf()),
            ["get_health_parser_memory_broker"] = R(CatSystemHealth, "system_health: memory-broker ring-buffer entries.", PServer(), PHours(24), PLimit(50), PAsOf()),
            ["get_health_parser_memory_conditions"] = R(CatSystemHealth, "system_health: resource-monitor memory conditions.", PServer(), PHours(24), PLimit(50), PAsOf()),
            ["get_health_parser_memory_node_oom"] = R(CatSystemHealth, "system_health: per-node out-of-memory events.", PServer(), PHours(24), PLimit(50), PAsOf()),
            ["get_health_parser_scheduler_issues"] = R(CatSystemHealth, "system_health: non-yielding scheduler issues.", PServer(), PHours(24), PLimit(50), PAsOf()),
            ["get_health_parser_severe_errors"] = R(CatSystemHealth, "system_health: severe (sev >= 19) errors.", PServer(), PHours(24), PLimit(50), PAsOf(), PDatabases()),
            ["get_health_parser_significant_waits"] = R(CatSystemHealth, "system_health: individual 500 ms+ waits with their statement.", PServer(), PHours(24), PLimit(50), PAsOf()),
            ["get_health_parser_system_health"] = R(CatSystemHealth, "system_health: the raw parsed session records. chart=true answers one row per time bucket (mean CPU) across the whole window instead of the newest limit rows; bucket_minutes picks the width.", PServer(), PHours(24), PLimit(50), PAsOf(), PBool("chart", false), PInt("bucket_minutes")),
            // FinOps web parity (#4843), set A: append new FinOps entries below this line only.
            ["get_finops_inventory"] = R(CatFinOps, "FinOps Server Inventory for the whole fleet, one row per configured server. view is server_inventory; limit caps servers returned; include_removed adds servers removed from the configuration.", PText("view"), PInt("limit", DarlingMcpFinOpsInventoryTools.DefaultLimit), PBool("include_removed", false)),
            ["get_finops_recommendations"] = R(CatFinOps, "FinOps recommendations for one server, High severity first; est_savings_usd_month is null without a monthly cost.", PServer()),
            // FinOps web parity (#4843), set A ends.
            // Each set belongs to one series of changes. Append to your own set only,
            // so the two series never edit the same lines of this catalog.
            // Entries keep the catalog's existing order and form.
            // Set A and set B are separated on purpose: keep this gap.
            //
            //
            //
            // FinOps web parity (#4843), set B: append new FinOps entries below this line only.
            ["get_finops"] = R(CatFinOps, "FinOps views for one server, picked by view. view is one of a closed set (today: " + DarlingMcpFinOpsTools.SetAValid + DarlingMcpFinOpsTools.SetBValid + "); an unknown view is refused with the valid list. Windowed over hours; limit caps rows per list.", PServer(), PText("view"), PHours(24), PInt("limit", 10), PText("database_name"), PBool("full_text", false), PText("object_name")),
            // FinOps web parity (#4843), set B ends.
        };

    /// <summary>Builds the <c>/api/catalog</c> body: the reads (names taken from <see cref="BuildReadDispatch"/>,
    /// each enriched with its <see cref="CatalogDescriptors"/> metadata) and the viz vocabulary. Iterating the
    /// dispatch keys guarantees the catalog only advertises actually-dispatchable reads.</summary>
    internal static JsonObject BuildCatalogNode() => BuildCatalogNode(null, Array.Empty<ScheduleOverride>());

    /// <summary>
    /// The catalog with each windowed read's reach (#5562): its <c>hours</c> param carries <c>max_hours</c> (the longest
    /// window the read's validator accepts, from <see cref="WebReadReach"/>), <c>shape</c>, and the main collector's
    /// <c>collector</c> id, <c>collector_interval_minutes</c> (the schedule in force: <paramref name="schedules"/> over
    /// the shipped default) and <c>collector_default_interval_minutes</c>, so the picker can grey out a longer period
    /// with its reason and say "collected every N minutes" under a range too short to hold three samples.
    /// </summary>
    internal static JsonObject BuildCatalogNode(int? serverId, IReadOnlyList<ScheduleOverride> schedules)
    {
        var reads = new JsonArray();
        foreach (var name in BuildReadDispatch().Keys)
        {
            var descriptor = CatalogDescriptors[name];
            var parameters = new JsonArray();
            foreach (var p in descriptor.Params)
            {
                var param = new JsonObject
                {
                    ["name"] = p.Name,
                    ["type"] = p.Type,
                    ["required"] = p.Required,
                    ["default"] = ToJsonValue(p.Default),
                };
                if (p.Name == "hours")
                {
                    AddReach(param, name, serverId, schedules);
                }

                parameters.Add(param);
            }

            reads.Add(new JsonObject
            {
                ["name"] = name,
                ["category"] = descriptor.Category,
                ["description"] = descriptor.Description,
                ["params"] = parameters,
            });
        }

        var viz = new JsonArray();
        foreach (var v in KnownVizList)
        {
            viz.Add(v);
        }

        /* v1 keys (reads/viz) unchanged; the v2 composed-view catalog rides alongside under "compose". */
        return new JsonObject { ["reads"] = reads, ["viz"] = viz, ["compose"] = BuildComposeCatalogNode() };
    }

    /// <summary>Adds a windowed read's reach to its <c>hours</c> catalog param (#5562). A read with no row in
    /// <see cref="WebReadReach.All"/> is advertised at the default reach with no collector; a census keeps that case
    /// from shipping.</summary>
    private static void AddReach(JsonObject param, string read, int? serverId, IReadOnlyList<ScheduleOverride> schedules)
    {
        WebReadReach.All.TryGetValue(read, out var reach);
        var shape = reach?.Shape switch
        {
            ReadShape.BucketedTrend => "bucketed_trend",
            ReadShape.LatestSnapshot => "latest_snapshot",
            _ => "list",
        };
        var collector = reach?.Collector;
        param["max_hours"] = reach?.MaxHours ?? WebReadReach.DefaultHours;
        if (WebReadReach.ViewMaxHours.TryGetValue(read, out var views))
        {
            var viewReach = new JsonObject();
            foreach (var (view, hours) in views)
            {
                viewReach[view] = hours;
            }

            param["view_max_hours"] = viewReach;
        }

        param["shape"] = shape;
        param["collector"] = collector;
        param["collector_interval_minutes"] = WebReadReach.CollectorIntervalMinutes(collector, serverId, schedules);
        param["collector_default_interval_minutes"] = collector is not null && CollectorScheduleDefaults.All.TryGetValue(collector, out var def)
            ? CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes(def.FrequencyMinutes)
            : null;
    }

    /// <summary>
    /// The v2 (Custom Views composer) catalog: the measure/dimension/unit-family/aggregate/time-bucket/filter-op
    /// vocabularies the composer picks from, straight off <see cref="MeasureCatalog"/> (the SOLE identifier
    /// source the compiler emits). Served under <c>/api/catalog</c>'s <c>compose</c> key so a single fetch backs
    /// both the v1 read picker and the v2 composer.
    /// </summary>
    internal static JsonObject BuildComposeCatalogNode()
    {
        var measures = new JsonArray();
        foreach (var m in MeasureCatalog.Measures)
        {
            var validAggregates = new JsonArray();
            foreach (var a in m.ValidAggs)
            {
                validAggregates.Add(MeasureCatalog.WireName(a));
            }

            var allowedDimensions = new JsonArray();
            foreach (var d in m.AllowedDimensions)
            {
                allowedDimensions.Add(d);
            }

            string? defaultAggregate = m.Kind == MeasureKind.Ratio ? null : MeasureCatalog.WireName(m.DefaultTimeAgg);

            var node = new JsonObject
            {
                ["key"] = m.Key,
                ["displayName"] = m.DisplayName,
                ["category"] = m.Category,
                ["source"] = m.SourceTable,
                ["kind"] = m.Kind == MeasureKind.Ratio ? "ratio" : "scalar",
                ["archetype"] = m.Archetype.ToString(),
                ["nativeUnit"] = m.NativeUnit,
                ["defaultUnit"] = m.DefaultUnit,
                ["unitFamily"] = m.UnitFamily,
                ["defaultAggregate"] = defaultAggregate,
                ["validAggregates"] = validAggregates,
                ["allowedDimensions"] = allowedDimensions,
                /* Per-server-type availability (design D4), off the owning collector's AppliesTo gate — so the
                   composer can grey a measure a given target can't collect (e.g. Agent measures need msdb). */
                ["appliesTo"] = BuildAppliesToNode(m.SourceTable),
            };

            /* #4653: only a measure that really divides carries a suffix; omitted otherwise, so the payload grows
               by the ratio measures alone (editor.js reads `labelSuffix || ""`, so an absent field renders the same). */
            var labelSuffix = MeasureCatalog.LabelSuffix(m);
            if (labelSuffix.Length > 0)
            {
                node["labelSuffix"] = labelSuffix;
            }

            measures.Add(node);
        }

        var dimensions = new JsonArray();
        foreach (var d in MeasureCatalog.Dimensions)
        {
            dimensions.Add(new JsonObject
            {
                ["source"] = d.SourceTable,
                ["name"] = d.Name,
                ["likeable"] = d.Likeable,
                ["viaModuleJoin"] = d.ViaModuleJoin,
            });
        }

        var unitFamilies = new JsonArray();
        foreach (var family in MeasureCatalog.UnitFamilies)
        {
            var units = new JsonArray();
            foreach (var unit in family.Units)
            {
                units.Add(new JsonObject { ["name"] = unit.Name, ["baseFactor"] = unit.BaseFactor });
            }

            unitFamilies.Add(new JsonObject { ["name"] = family.Name, ["units"] = units });
        }

        /* Event-annotation sources (design D5): the marker overlays the composer offers on a time-series panel.
           Carries appliesTo off the owning collector's gate (like measures, D4) so the composer can grey a source
           a given target can't collect — e.g. default_trace_events / system_health differ by platform. */
        var annotationSources = new JsonArray();
        foreach (var a in MeasureCatalog.AnnotationSources)
        {
            annotationSources.Add(new JsonObject
            {
                ["key"] = a.Key,
                ["displayName"] = a.DisplayName,
                ["category"] = a.Category,
                ["appliesTo"] = BuildAppliesToNode(a.SourceTable),
            });
        }

        return new JsonObject
        {
            ["measures"] = measures,
            ["dimensions"] = dimensions,
            ["annotationSources"] = annotationSources,
            /* server is a virtual dimension present on every source (the fleet axis), not in the per-source
               dimensions list — surfaced here so the composer offers it on every measure. */
            ["universalDimensions"] = ToJsonStringArray(new[] { MeasureCatalog.ServerDimensionName }),
            ["unitFamilies"] = unitFamilies,
            ["aggregates"] = ToJsonStringArray(MeasureCatalog.AggregateWireNames),
            ["timeBuckets"] = ToJsonStringArray(MeasureCatalog.TimeBucketWireNames),
            ["filterOps"] = ToJsonStringArray(MeasureCatalog.FilterOpWireNames),
            /* the v2 composed-panel viz vocabulary (bar/pie/stacked/stacked-bar/area added over v1's KnownViz). */
            ["viz"] = ToJsonStringArray(ComposeSpec.ComposeVizList),
        };
    }

    /// <summary>
    /// The MCP <c>describe_custom_view_catalog</c> tool's DEFAULT shape (#4198 — the full catalog measured 98,173
    /// bytes at default arguments, three times the tool's 32 KB response budget). Re-groups
    /// <see cref="BuildComposeCatalogNode"/>'s flat <c>measures</c> by <c>source</c> (collector table) and keeps
    /// only the fields an author needs to pick a measure and know its legal vocabulary: <c>key</c>,
    /// <c>displayName</c> (the one-line purpose), <c>kind</c>, <c>unitFamily</c>, <c>validAggregates</c> (this one
    /// VARIES within a source — e.g. a ratio measure's is empty where its source's scalars allow sum/avg/min/max —
    /// so, unlike <c>allowedDimensions</c>, it cannot be hoisted to the source level without hiding a real
    /// restriction). Left out: <c>category</c>/<c>archetype</c> (neither is part of the vocabulary a panel spec
    /// names — <see cref="ComposeSpec"/>'s required panel fields are measure/aggregate/unit, never these),
    /// <c>nativeUnit</c>/<c>defaultUnit</c>/<c>defaultAggregate</c> (suggestions, not requirements — any unit in
    /// the measure's family and any aggregate in its validAggregates is legal), <c>allowedDimensions</c> (uniform
    /// across every measure of a source — pinned by <c>DarlingComposeTests</c> — so it is the source's dimension
    /// list, one <see cref="FilterComposeCatalogNodeBySource"/> call away), and <c>appliesTo</c> (server-type
    /// availability is a UI greying hint for the web composer, per design D4 — it gates nothing at compose time).
    /// Every dropped field is reachable per-source via <c>source=&lt;name&gt;</c>
    /// (<see cref="FilterComposeCatalogNodeBySource"/>) or for the whole catalog via <c>full_detail=true</c>
    /// (this method's own return, unfiltered).
    ///
    /// <para>Takes the ALREADY-BUILT full node so the compact view is a pure re-shape of it, never a second read
    /// of <see cref="MeasureCatalog"/> — it can't drift from what <c>full_detail=true</c> and <c>/api/catalog</c>
    /// (which never compacts; the web Custom Views editor needs the fields this trims) both serve.</para>
    /// </summary>
    internal static JsonObject BuildComposeCatalogCompactNode(JsonObject full)
    {
        var sources = new JsonArray();
        foreach (var sourceGroup in full["measures"]!.AsArray()
                     .Select(m => m!.AsObject())
                     .GroupBy(m => m["source"]!.GetValue<string>(), StringComparer.Ordinal))
        {
            var measures = new JsonArray();
            foreach (var m in sourceGroup)
            {
                measures.Add(new JsonObject
                {
                    ["key"] = m["key"]!.GetValue<string>(),
                    ["displayName"] = m["displayName"]!.GetValue<string>(),
                    ["kind"] = m["kind"]!.GetValue<string>(),
                    ["unitFamily"] = m["unitFamily"]!.GetValue<string>(),
                    ["validAggregates"] = JsonNode.Parse(m["validAggregates"]!.ToJsonString()),
                });
            }

            sources.Add(new JsonObject { ["source"] = sourceGroup.Key, ["measures"] = measures });
        }

        var annotationSources = new JsonArray();
        foreach (var a in full["annotationSources"]!.AsArray())
        {
            var ao = a!.AsObject();
            annotationSources.Add(new JsonObject
            {
                ["key"] = ao["key"]!.GetValue<string>(),
                ["displayName"] = ao["displayName"]!.GetValue<string>(),
                ["category"] = ao["category"]!.GetValue<string>(),
            });
        }

        return new JsonObject
        {
            ["sources"] = sources,
            ["annotationSources"] = annotationSources,
            ["universalDimensions"] = JsonNode.Parse(full["universalDimensions"]!.ToJsonString()),
            ["unitFamilies"] = JsonNode.Parse(full["unitFamilies"]!.ToJsonString()),
            ["aggregates"] = JsonNode.Parse(full["aggregates"]!.ToJsonString()),
            ["timeBuckets"] = JsonNode.Parse(full["timeBuckets"]!.ToJsonString()),
            ["filterOps"] = JsonNode.Parse(full["filterOps"]!.ToJsonString()),
            ["viz"] = JsonNode.Parse(full["viz"]!.ToJsonString()),
            ["compact"] = true,
            ["note"] = "Compact by default (#4198): each source lists its measures' key/displayName/kind/unitFamily/validAggregates " +
                "only. Call describe_custom_view_catalog(source=\"<name>\") for that source's FULL per-measure detail " +
                "(category, archetype, nativeUnit, defaultUnit, defaultAggregate, allowedDimensions, appliesTo) plus its own " +
                "dimensions, or full_detail=true for the complete catalog (measures/dimensions/annotationSources as flat arrays, " +
                "every field, exactly like this tool returned before #4198).",
        };
    }

    /// <summary>
    /// The MCP <c>describe_custom_view_catalog</c> tool's <c>source=&lt;name&gt;</c> drill-down (#4198): the FULL
    /// per-entry detail <see cref="BuildComposeCatalogNode"/> serves, filtered to one source (collector table)'s
    /// measures and dimensions. <c>annotationSources</c> rides along whole either way — five entries total, cheap
    /// regardless of the filter, and its underlying source table is not itself a served field to filter on. An
    /// unmatched <paramref name="source"/> comes back with empty <c>measures</c>/<c>dimensions</c> and a
    /// <c>note</c> pointing at the default (unfiltered) call for the real source names, rather than an error
    /// envelope — this tool has never returned one, and a typo should not change the shape the caller parses.
    /// </summary>
    internal static JsonObject FilterComposeCatalogNodeBySource(JsonObject full, string source)
    {
        var measures = new JsonArray(full["measures"]!.AsArray()
            .Where(m => string.Equals(m!["source"]!.GetValue<string>(), source, StringComparison.Ordinal))
            .Select(m => JsonNode.Parse(m!.ToJsonString())!)
            .ToArray());

        var dimensions = new JsonArray(full["dimensions"]!.AsArray()
            .Where(d => string.Equals(d!["source"]!.GetValue<string>(), source, StringComparison.Ordinal))
            .Select(d => JsonNode.Parse(d!.ToJsonString())!)
            .ToArray());

        var result = new JsonObject
        {
            ["source"] = source,
            ["measures"] = measures,
            ["dimensions"] = dimensions,
            ["annotationSources"] = JsonNode.Parse(full["annotationSources"]!.ToJsonString()),
            ["universalDimensions"] = JsonNode.Parse(full["universalDimensions"]!.ToJsonString()),
            ["unitFamilies"] = JsonNode.Parse(full["unitFamilies"]!.ToJsonString()),
            ["aggregates"] = JsonNode.Parse(full["aggregates"]!.ToJsonString()),
            ["timeBuckets"] = JsonNode.Parse(full["timeBuckets"]!.ToJsonString()),
            ["filterOps"] = JsonNode.Parse(full["filterOps"]!.ToJsonString()),
            ["viz"] = JsonNode.Parse(full["viz"]!.ToJsonString()),
        };

        if (measures.Count == 0)
        {
            result["note"] = $"No measures found for source '{source}'. Call describe_custom_view_catalog() with no arguments to list the valid source names.";
        }

        return result;
    }

    /// <summary>Collector definition by its destination table, for the per-measure availability lookup (a
    /// measure's <c>SourceTable</c> is a collector <c>TargetTable</c>, pinned by <c>DarlingComposeTests</c>).</summary>
    private static readonly Dictionary<string, ICollectorSchemaInfo> s_collectorByTable =
        CollectorCatalog.All.ToDictionary(c => c.TargetTable, StringComparer.Ordinal);

    /* Representative target profiles a measure's collector AppliesTo gate is evaluated against (design D4).
       SqlMajorVersion is pinned to a supported major (16 = SQL 2022) so the version-gated collectors
       (query_stats/query_store, 2016+) read as available. */
    private static readonly CollectorTargetInfo s_onPremTarget = new() { SqlMajorVersion = 16, HasMsdbAccess = true };

    /* The SQL-Agent collectors, NAMED rather than probed. This used to be detected by evaluating the gate
       against an msdb-less profile - "available with msdb, gated without it" - which stopped working when
       #2559 removed HasMsdbAccess from those gates. The dependency itself did not go away: these three read
       msdb.dbo.sysjobs and friends, so a panel built on them still needs the grant. What changed is what its
       absence produces - a PERMISSIONS row rather than no dispatch - which arguably makes the badge MORE
       useful, since the data starts flowing the moment the grant lands with no reconnect.

       Honest limit: this cannot notice a NEW collector that reads msdb. DarlingComposeTests pins that the set
       is exactly these three and that each one's query really does reference msdb, which catches a rename or
       a collector that stopped reading it - not an addition. */
    private static readonly HashSet<string> s_msdbBackedTables =
        new(StringComparer.Ordinal) { "running_jobs", "job_history", "agent_status" };
    private static readonly CollectorTargetInfo s_azureSqlDbTarget = new() { IsAzureSqlDb = true, SqlMajorVersion = 16, HasMsdbAccess = false };
    private static readonly CollectorTargetInfo s_azureMiTarget = new() { IsAzureManagedInstance = true, SqlMajorVersion = 16, HasMsdbAccess = true };
    private static readonly CollectorTargetInfo s_awsRdsTarget = new() { IsAwsRds = true, SqlMajorVersion = 16, HasMsdbAccess = true };

    /* PostgreSQL target profiles (#3285). Engine = PostgreSql so the dispatch gate's engine half
       (CollectorCatalog.EngineMatches) selects the PostgreSQL collectors and excludes every SQL Server one -
       and excludes these PG collectors from the four SQL profiles above. The major/version are pinned modern
       so the version-gated PG collectors (pg_stat_io PG16+, pg_write_stats PG14+) read as available, and
       IsInRecovery defaults false so the writer-only collectors (autovacuum, index/table stats) do too.
       s_postgresTarget is stock PostgreSQL: IsAurora is false, so the Aurora-only collectors (pg_wait_stats,
       pg_cpu_utilization) correctly read unavailable there; s_auroraTarget flips IsAurora on. */
    private static readonly CollectorTargetInfo s_postgresTarget =
        new() { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = 17, PostgresVersionNum = 170_005 };
    private static readonly CollectorTargetInfo s_auroraTarget =
        new() { Engine = CollectorTargetEngine.PostgreSql, IsAurora = true, PostgresMajorVersion = 17, PostgresVersionNum = 170_005 };

    /// <summary>The per-server-type availability of a measure (design D4), derived from its owning collector's
    /// target gate so the composer can label/grey a measure a given server type can't collect. Each flag is
    /// the FULL dispatch gate <see cref="CollectorCatalog.AppliesTo(ICollectorSchemaInfo, CollectorTargetInfo)"/>
    /// (<c>EngineMatches AND definition.AppliesTo</c>), NOT the raw <see cref="ICollectorDefinition{TRow}.AppliesTo"/>
    /// override: the engine half is what makes a PostgreSQL measure read unavailable on the SQL Server profiles
    /// and a SQL Server measure unavailable on the PostgreSQL profiles. <c>postgres</c>/<c>aurora</c> were added
    /// with the #3285 PostgreSQL measures; <c>needsMsdb</c> is the SQL-Agent dependency (job/agent measures),
    /// taken from the named SQL-Agent set rather than probed from the gate - see the note there on why #2559
    /// made probing impossible. Returns null only if a measure's source has no collector (impossible, pinned by test).</summary>
    private static JsonObject? BuildAppliesToNode(string sourceTable)
    {
        if (!s_collectorByTable.TryGetValue(sourceTable, out var collector))
        {
            return null;
        }

        return new JsonObject
        {
            /* CollectorCatalog.AppliesTo, NOT the raw collector.AppliesTo: the former is
               EngineMatches && definition.AppliesTo, so a PostgreSQL collector (including one gated `=> true`)
               correctly reads FALSE on the SQL Server profiles and a SQL Server collector reads FALSE on the
               PostgreSQL profiles. The raw override skips the engine half and reported a `=> true` PG collector
               as available on every SQL Server target (#3285). */
            ["onPrem"] = CollectorCatalog.AppliesTo(collector, s_onPremTarget),
            ["azureSqlDb"] = CollectorCatalog.AppliesTo(collector, s_azureSqlDbTarget),
            ["azureMi"] = CollectorCatalog.AppliesTo(collector, s_azureMiTarget),
            ["awsRds"] = CollectorCatalog.AppliesTo(collector, s_awsRdsTarget),
            ["postgres"] = CollectorCatalog.AppliesTo(collector, s_postgresTarget),
            ["aurora"] = CollectorCatalog.AppliesTo(collector, s_auroraTarget),
            ["needsMsdb"] = s_msdbBackedTables.Contains(sourceTable),
        };
    }

    private static JsonArray ToJsonStringArray(IReadOnlyList<string> values)
    {
        var array = new JsonArray();
        foreach (var value in values)
        {
            array.Add(value);
        }

        return array;
    }

    private static JsonValue? ToJsonValue(object? value) => value switch
    {
        null => null,
        int i => JsonValue.Create(i),
        bool b => JsonValue.Create(b),
        double d => JsonValue.Create(d),
        _ => JsonValue.Create(value.ToString()),
    };

    /* ── request body parsing + response building ── */

    /// <summary>A parsed create/update body: name (required, trimmed), description (optional), the definition as
    /// raw JSON text (re-serialized from the parsed node), version (present only on updates), and (#6214) the folder
    /// (<see cref="FolderGiven"/> tells an omitted key from an explicit null, which means the top level) and the personal flag
    /// (null when omitted).</summary>
    private sealed record ViewWriteRequest(
        string Name, string? Description, string DefinitionJson, int? Version, bool FolderGiven = false, string? Folder = null, bool? Personal = null);

    /// <summary>Parses the request body into a <see cref="ViewWriteRequest"/>, or returns a caller-facing error
    /// (the definition's STRUCTURE is validated separately by <see cref="ValidateDefinition"/>).</summary>
    private static async Task<(ViewWriteRequest? Request, string? Error)> ParseViewBodyAsync(HttpContext context)
    {
        JsonNode? root;
        try
        {
            root = await JsonNode.ParseAsync(context.Request.Body, cancellationToken: context.RequestAborted);
        }
        catch (JsonException)
        {
            return (null, "Request body is not valid JSON.");
        }

        if (root is not JsonObject obj)
        {
            return (null, "Request body must be a JSON object.");
        }

        var name = TryGetString(obj, "name");
        if (string.IsNullOrWhiteSpace(name))
        {
            return (null, "'name' is required.");
        }

        var description = TryGetString(obj, "description");

        var definitionNode = obj["definition"];
        if (definitionNode is null)
        {
            return (null, "'definition' is required.");
        }

        var definitionJson = definitionNode.ToJsonString();

        int? version = null;
        if (obj["version"] is JsonValue versionValue && versionValue.TryGetValue<int>(out var v))
        {
            version = v;
        }

        var folderGiven = obj.ContainsKey("folder");
        string? folder = null;
        if (folderGiven && obj["folder"] is { } folderNode)
        {
            if (folderNode is not JsonValue folderValue || !folderValue.TryGetValue<string>(out var folderText))
            {
                return (null, "'folder' must be a string, or null for the top level.");
            }

            folder = CustomViewStore.NormalizeFolder(folderText, out var folderError);
            if (folderError is not null)
            {
                return (null, folderError);
            }
        }

        bool? personal = null;
        if (obj["personal"] is { } personalNode)
        {
            if (personalNode is not JsonValue personalValue || !personalValue.TryGetValue<bool>(out var personalFlag))
            {
                return (null, "'personal' must be true or false.");
            }

            personal = personalFlag;
        }

        return (new ViewWriteRequest(name.Trim(), description, definitionJson, version, folderGiven, folder, personal), null);
    }

    /// <summary>The full single-view wire shape (definition embedded as JSON, NOT an escaped string). Shared by
    /// the web <c>GET/POST/PUT /api/views</c> responses and the MCP <c>get_custom_view</c> / <c>create_custom_view</c>
    /// / <c>update_custom_view</c> tools, so the two surfaces return an identical view shape.</summary>
    internal static JsonObject BuildFullViewNode(CustomView view) => new()
    {
        ["id"] = view.Id,
        ["name"] = view.Name,
        ["description"] = view.Description,
        ["definition"] = JsonNode.Parse(view.DefinitionJson),
        ["version"] = view.Version,
        ["created_at"] = view.CreatedAt,
        ["updated_at"] = view.UpdatedAt,
        ["updated_by"] = view.UpdatedBy,
        ["folder"] = view.Folder,
        ["personal"] = view.Personal,
        ["created_by"] = view.CreatedBy,
        ["mine"] = view.Mine,
    };

    /// <summary>The 400 sentence for a personal view a session cannot own (#6214): a session that names a person (a cookie from before
    /// owner keys existed) signs in again; one with no person at all has none to give.</summary>
    internal static string NoOwnerKeyMessage(DarlingWebSeat seat) =>
        seat.Subject is null ? CustomViewStore.NoIdentityMessage : CustomViewStore.SignInAgainMessage;

    /// <summary>The bare-array list wire shape (no definition body). Carries the view <c>kind</c> — always
    /// concrete (<c>"dashboard"</c> when the stored definition declares none) so the list page/sidebar can badge
    /// and route a notebook vs a dashboard without fetching each full definition.</summary>
    internal static JsonArray BuildSummariesNode(IReadOnlyList<CustomViewSummary> views)
    {
        var array = new JsonArray();
        foreach (var view in views)
        {
            array.Add(new JsonObject
            {
                ["id"] = view.Id,
                ["name"] = view.Name,
                ["description"] = view.Description,
                ["version"] = view.Version,
                ["updated_at"] = view.UpdatedAt,
                ["updated_by"] = view.UpdatedBy,
                ["kind"] = view.Kind ?? "dashboard",
                ["folder"] = view.Folder,
                ["personal"] = view.Personal,
                ["created_by"] = view.CreatedBy,
                ["mine"] = view.Mine,
            });
        }

        return array;
    }

    /* ── custom-alert-rule body parsing + response building (#3285) ── */

    /// <summary>A parsed alert create/update body: name (required, trimmed), description (optional), the
    /// definition as raw JSON text (re-serialized from the parsed node), <c>enabled</c> (defaults true when the
    /// key is absent, matching create's default and a full-replace PUT), and version (present only on updates).
    /// The definition's STRUCTURE is validated separately by <see cref="CustomAlertRuleDefinition.TryParse"/>.</summary>
    private sealed record AlertWriteRequest(string Name, string? Description, string DefinitionJson, bool Enabled, int? Version, string? ExpiresAtText = null);

    /// <summary>Parses an alert create/update body into an <see cref="AlertWriteRequest"/>, or a caller-facing
    /// error. Mirrors <see cref="ParseViewBodyAsync"/>, adding the <c>enabled</c> field alert rules carry.</summary>
    private static async Task<(AlertWriteRequest? Request, string? Error)> ParseAlertBodyAsync(HttpContext context)
    {
        JsonNode? root;
        try
        {
            root = await JsonNode.ParseAsync(context.Request.Body, cancellationToken: context.RequestAborted);
        }
        catch (JsonException)
        {
            return (null, "Request body is not valid JSON.");
        }

        if (root is not JsonObject obj)
        {
            return (null, "Request body must be a JSON object.");
        }

        var name = TryGetString(obj, "name");
        if (string.IsNullOrWhiteSpace(name))
        {
            return (null, "'name' is required.");
        }

        var description = TryGetString(obj, "description");

        var definitionNode = obj["definition"];
        if (definitionNode is null)
        {
            return (null, "'definition' is required.");
        }

        var definitionJson = definitionNode.ToJsonString();

        /* Absent 'enabled' defaults true: create's default, and a full-replace PUT of a rule that names no
           enabled state is an enabled rule (the real full-document editor always sends the field). */
        var enabled = true;
        if (obj["enabled"] is JsonValue enabledValue && enabledValue.TryGetValue<bool>(out var e))
        {
            enabled = e;
        }

        int? version = null;
        if (obj["version"] is JsonValue versionValue && versionValue.TryGetValue<int>(out var v))
        {
            version = v;
        }

        /* #6235: the optional expiry. Absent, null or empty means the rule does not expire (a full-replace PUT clears it, like every
           other field the editor sends); anything else is parsed by the store call below, so a bad value is a 400, not a throw. */
        string? expiresText = null;
        if (obj["expires_at_utc"] is JsonValue expiresValue)
        {
            if (!expiresValue.TryGetValue<string>(out var expiresString))
            {
                return (null, $"'expires_at_utc' must be {CustomAlertExpiry.Spelling}, or null for a rule that does not expire.");
            }

            expiresText = string.IsNullOrWhiteSpace(expiresString) ? null : expiresString;
        }

        return (new AlertWriteRequest(name.Trim(), description, definitionJson, enabled, version, expiresText), null);
    }

    /// <summary>Extracts just the embedded <c>definition</c> object from a body as raw JSON text — the
    /// <c>POST /api/alerts/validate</c> shape — or a caller-facing error. The definition rides as a JSON object
    /// (not an escaped string), the same way the create/update bodies carry it.</summary>
    private static async Task<(string? DefinitionJson, string? Error)> ParseDefinitionBodyAsync(HttpContext context)
    {
        JsonNode? root;
        try
        {
            root = await JsonNode.ParseAsync(context.Request.Body, cancellationToken: context.RequestAborted);
        }
        catch (JsonException)
        {
            return (null, "Request body is not valid JSON.");
        }

        if (root is not JsonObject obj)
        {
            return (null, "Request body must be a JSON object.");
        }

        var definitionNode = obj["definition"];
        return definitionNode is null
            ? (null, "'definition' is required.")
            : (definitionNode.ToJsonString(), null);
    }

    /// <summary>Parses a <c>POST /api/alerts/test</c> body into <c>(rule_id, definition)</c> — both optional here
    /// (the wrapped <see cref="Mcp.DarlingMcpCustomAlertTools.TestCustomAlertRule"/> enforces the rule_id XOR
    /// definition rule and reports a violation in its envelope). Errors only on a body that is not a JSON object.
    /// The definition rides as an embedded JSON object, re-serialized to the text the tool parses.</summary>
    private static async Task<(long? RuleId, string? DefinitionJson, string? Error)> ParseAlertTestBodyAsync(HttpContext context)
    {
        JsonNode? root;
        try
        {
            root = await JsonNode.ParseAsync(context.Request.Body, cancellationToken: context.RequestAborted);
        }
        catch (JsonException)
        {
            return (null, null, "Request body is not valid JSON.");
        }

        if (root is not JsonObject obj)
        {
            return (null, null, "Request body must be a JSON object.");
        }

        long? ruleId = null;
        if (obj["rule_id"] is JsonValue ruleIdValue && ruleIdValue.TryGetValue<long>(out var r))
        {
            ruleId = r;
        }

        var definitionNode = obj["definition"];
        var definitionJson = definitionNode?.ToJsonString();

        return (ruleId, definitionJson, null);
    }

    /// <summary>The full single-rule wire shape — IDENTICAL to
    /// <see cref="Mcp.DarlingMcpCustomAlertTools"/>'s (definition embedded as JSON, plus the <c>enabled</c>
    /// column alert rules carry), so the web GET/POST/PUT and the MCP get/create/update return the same rule
    /// shape.</summary>
    internal static JsonObject BuildFullRuleNode(CustomAlertRule rule) =>
        JsonSerializer.SerializeToNode(new CustomAlertRuleImage(rule.Id, rule.Name, rule.Description, JsonNode.Parse(rule.DefinitionJson),
            rule.Enabled, rule.Version, rule.CreatedAt, rule.UpdatedAt, rule.UpdatedBy,
            CustomAlertExpiry.Format(rule.ExpiresAtUtc), rule.IsExpiredAt(DateTime.UtcNow)))!.AsObject();

    /// <summary>The bare-array list wire shape (no definition body), carrying <c>enabled</c> so the list can
    /// badge a paused rule without fetching each full definition — plus <c>last_fired</c> (#3360), the naive-UTC
    /// ISO-8601 instant the rule most recently fired (JSON <c>null</c> when it never has) so a card can show
    /// "last fired …" / "never fired" without a per-rule read. Mirrors the MCP list surface (they share this
    /// builder AND <see cref="CustomAlertRuleSummary"/>, so the field reaches both surfaces identically).</summary>
    internal static JsonArray BuildRuleSummariesNode(IReadOnlyList<CustomAlertRuleSummary> rules)
    {
        var array = new JsonArray();
        foreach (var rule in rules)
        {
            array.Add(new JsonObject
            {
                ["id"] = rule.Id,
                ["name"] = rule.Name,
                ["description"] = rule.Description,
                ["enabled"] = rule.Enabled,
                ["version"] = rule.Version,
                ["updated_at"] = rule.UpdatedAt,
                ["updated_by"] = rule.UpdatedBy,
                ["last_fired"] = rule.LastFired,
                ["expires_at_utc"] = CustomAlertExpiry.Format(rule.ExpiresAtUtc),
                ["expired"] = rule.IsExpiredAt(DateTime.UtcNow),
            });
        }

        return array;
    }

    /* ── result helpers (JSON body written verbatim, bypassing any serializer naming policy) ── */

    private static IResult JsonNodeResult(JsonNode node, int statusCode = StatusCodes.Status200OK) =>
        Results.Text(node.ToJsonString(), "application/json", statusCode: statusCode);

    private static IResult CreatedResult(HttpContext context, string location, JsonNode node)
    {
        context.Response.Headers.Location = location;
        return Results.Text(node.ToJsonString(), "application/json", statusCode: StatusCodes.Status201Created);
    }

    private static IResult ErrorResult(string message, int statusCode) =>
        JsonNodeResult(new JsonObject { ["error"] = message }, statusCode);

    private static IResult NotFoundResult() =>
        ErrorResult("View not found.", StatusCodes.Status404NotFound);

    private static IResult AlertNotFoundResult() =>
        ErrorResult("Alert rule not found.", StatusCodes.Status404NotFound);

    private static IResult UnsupportedMediaTypeResult() =>
        ErrorResult("Content-Type must be application/json.", StatusCodes.Status415UnsupportedMediaType);
}
