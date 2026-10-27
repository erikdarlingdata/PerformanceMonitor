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
using System.Runtime.CompilerServices;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// Times every <c>tools/call</c> and records it into the shared <see cref="ReadLatencyAccumulator"/> under
/// <see cref="ReadSurface.Mcp"/> (#4442 scope 2), the same accumulator the <c>/api/read/*</c> dispatch loop and
/// <c>RunComposedPanelAsync</c> already feed. Registered ONCE (<c>DarlingMcpHostService.ConfigureMcpServices</c>),
/// beside the unknown-argument guard and the GCF filter, so it covers every tool with no per-tool change,
/// including a <c>/core</c> call: a call-tool filter runs from inside the MCP transport for every path the
/// server maps, before AND after <c>ConfigureSessionOptions</c> narrows <c>/core</c>'s <c>ToolCollection</c> —
/// the narrowing only changes WHICH names dispatch, never whether a dispatched call passes through the
/// registered filters. Darling registers no fallback <c>CallToolHandler</c>, so a <c>/core</c> call for a name
/// outside the closure never reaches <paramref name="next"/> at all (the SDK's own "unknown tool" answer, from
/// <c>tools/list</c>'s narrowed set) and records nothing here — which is correct: no tool ran, so there is
/// nothing to time.
///
/// <para><b>run_custom_view_panel is skipped.</b> That tool's body calls <c>RunComposedPanelAsync</c>, the ONE
/// runner shared with the web composer, which already records one <c>Compose</c> sample per call. Recording
/// here too would double-count every MCP custom-view run as both <c>Compose</c> and <c>Mcp</c>.</para>
///
/// <para><b>The tools in <see cref="UntimedTools"/> pass straight through</b>, each for the reason it lists there.</para>
///
/// <para><b>Outcome.</b> A thrown exception (a binding-layer throw the tool's own try/catch did not
/// catch) classifies through <see cref="ReadOutcomeClassifier.Classify"/>, the same as the web loop's
/// backstop arm. A normal return classifies its result: <see cref="CallToolResult.IsError"/> set true, or a
/// lone text block carrying the tool's own caught-and-formatted error sentence (<c>McpHelpers.FormatError</c>'s
/// envelope — the shape every tool catch returns), classifies through
/// <see cref="ReadOutcomeClassifier.ClassifySentence"/> so a 57014 statement-timeout envelope counts as
/// <see cref="ReadOutcome.Timeout"/> exactly as it would on the web surface; anything else is
/// <see cref="ReadOutcome.Ok"/>.</para>
///
/// <para>Recording never throws into the call: swallowed and logged at Debug, the same posture
/// <c>RecordWebReadLatency</c>/<c>RecordComposeLatency</c> already take.</para>
/// </summary>
public sealed class McpToolLatencyFilter
{
    /// <summary>The tool name this filter never records against <see cref="ReadSurface.Mcp"/> — it is already
    /// counted once as <c>compose</c> by <c>RunComposedPanelAsync</c>.</summary>
    internal const string SkippedToolName = "run_custom_view_panel";

    /// <summary>
    /// The tools this filter passes straight through: not timed, not admitted, no read scope (so no slow-read record), each
    /// with why (#5822; #5856 review round 1 made the exemption a roster). Not <see cref="ReadAdmission.SkippedRoutes"/>:
    /// that roster holds health reads, which are still timed, and this tool must not be timed at all.
    /// </summary>
    internal static readonly IReadOnlyDictionary<string, string> UntimedTools = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        [DarlingMcpInvestigationTools.WaitToolName] =
            "long poll: it waits up to 25 seconds by design, so timing it would fill the read-latency percentiles and the " +
            "slow-read record with its waits, and admitting it would hold a read slot for the whole wait; its catch-up " +
            "reads are small and keyed on opened_at",
    };

    private readonly ReadLatencyAccumulator _readLatency;
    private readonly ILogger? _logger;

    private readonly SlowReadLog? _slowReads;

    private readonly ReadAdmission? _admission;

    public McpToolLatencyFilter(ReadLatencyAccumulator readLatency, ILogger? logger)
        : this(readLatency, logger, null)
    {
    }

    internal McpToolLatencyFilter(ReadLatencyAccumulator readLatency, ILogger? logger, SlowReadLog? slowReads, ReadAdmission? admission = null)
    {
        _readLatency = readLatency ?? throw new ArgumentNullException(nameof(readLatency));
        _logger = logger;
        _slowReads = slowReads;
        _admission = admission;
    }

    /// <summary>The filter: run the tool, time it, record the outcome. A filter is <c>next =&gt; handler</c>.</summary>
    public McpRequestFilter<CallToolRequestParams, CallToolResult> AsFilter() =>
        next => async (request, cancellationToken) =>
        {
            var toolName = request.Params?.Name;
            if (string.IsNullOrEmpty(toolName))
            {
                return await next(request, cancellationToken);
            }

            if (UntimedTools.ContainsKey(toolName))
            {
                return await next(request, cancellationToken);
            }

            /* #5793: who made the call, worked out once here so the recorder only ever sees the finished string. The
               composed-panel tool records itself through the shared runner, which reads it from the ambient value. */
            var caller = CallerOf(request);
            /* #6230/#6227: set for every tool, not just the composed panel: a write tool names its caller in its own log line (it takes no request of its own). */
            ReadCaller.SetAmbientMcpCaller(caller);
            /* #6231: and the server-side identity of the call (the token's fingerprint), which a write tool logs ahead of the client's own name (ReadCaller.WriteIdentity). */
            ReadCaller.SetAmbientMcpToken(TokenIdentityOf(request));
            if (string.Equals(toolName, SkippedToolName, StringComparison.Ordinal))
            {
                ReadCaller.SetAmbientMcpCaller(caller);
                var busyAnswer = new StrongBox<bool>();
                s_busyAnswer.Value = busyAnswer;
                var panelResult = await next(request, cancellationToken);

                /* #5506 part 3: the shared runner admits the panel itself and answers a refusal with the busy envelope;
                   the tool returns text, so the error flag is set here. The tool flags the refusal (round 1, L6): with GCF
                   on, the text that comes back is the re-encoded wire, not the JSON envelope. */
                if (busyAnswer.Value || IsBusyEnvelope(panelResult))
                {
                    panelResult.IsError = true;
                }

                return panelResult;
            }

            using var readScope = ReadScope.Open(_logger);
            readScope.Scope.CaptureStatements = true;
            var windowBand = WindowBandOf(request);

            /* #5506 part 3: a heavy cacheable read is answered from the read cache, joins an identical read running now,
               or runs once and is stored. A hit skips admission. */
            /* #6800: a whole-fleet read of get_resource_use_by_tag (no server_name) over a window of 24 hours or more is cached for its own TTL. */
            var fleetRead = IsFleetRead(request);
            if (_admission is { } admission && admission.IsCached(ReadSurface.Mcp, toolName, windowBand, fleetRead))
            {
                return await ServeCachedAsync(admission, next, request, toolName, windowBand, fleetRead, readScope.Scope, caller, cancellationToken);
            }

            CallToolResult answer;
            try
            {
                answer = await RunReadAsync(next, request, toolName, windowBand, readScope.Scope, caller, cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException && DarlingMcpWritePolicy.IsWrite(toolName))
            {
                LogWriteCall(toolName, "failed");
                throw;
            }

            if (DarlingMcpWritePolicy.IsWrite(toolName))
            {
                LogWriteCall(toolName, WriteOutcomeOf(answer));
            }

            /* A successful write, or a successful unanchored analyze_server (it stores findings, D5), makes every cached
               answer stored before it unreachable. */
            if (_admission is not null && MovesGeneration(toolName, request.Params?.Arguments, answer))
            {
                _admission.Cache.BumpGeneration();
            }

            return answer;
        };

    /// <summary>One Information line for every call of a write tool: the tool, who called it and the outcome, and never the arguments (a
    /// write tool can take a webhook URL or a credential). Who called is <see cref="ReadCaller.WriteIdentity"/>, the same text every write
    /// tool's own log line carries: the token's fingerprint, then the name the client gave itself. A write tool that changes a collector
    /// schedule also logs what it changed, from inside the tool.</summary>
    private void LogWriteCall(string toolName, string outcome) =>
        _logger?.LogInformation("MCP write tool {Tool} called by {Caller}: {Outcome}", toolName, ReadCaller.WriteIdentity(), outcome);

    /// <summary>The outcome word a write call logs, read from the answer the way <see cref="ClassifyResult"/> reads a read's: a write
    /// tool answers a refusal and a caught error as text, never as a thrown exception, so the error flag alone would call every one
    /// of them completed. A <c>refused</c> or <c>invalid</c> status is <c>refused</c> (nothing was changed because the request as
    /// given cannot be served), the caught-error envelope or an error flag is <c>failed</c>, anything else is <c>completed</c>.
    /// Internal so a unit test can call it without a running server.</summary>
    internal static string WriteOutcomeOf(CallToolResult result)
    {
        if (result.Content is { Count: 1 } content && content[0] is TextContentBlock block)
        {
            var text = EnvelopeText(block.Text);
            if (McpHelpers.IsErrorEnvelope(text))
            {
                return "failed";
            }

            if (McpHelpers.IsRefusalEnvelope(text) || StatusWordOf(text) == "refused")
            {
                return "refused";
            }
        }

        return result.IsError == true ? "failed" : "completed";
    }

    /// <summary>The <c>status</c> word of a JSON object answer, or null when the text is not one.</summary>
    private static string? StatusWordOf(string text)
    {
        if (!text.AsSpan().TrimStart().StartsWith("{".AsSpan(), StringComparison.Ordinal))
        {
            return null;
        }

        try
        {
            return JsonNode.Parse(text) is JsonObject body && body["status"] is JsonValue status && status.TryGetValue<string>(out var word)
                ? word
                : null;
        }
        catch (System.Text.Json.JsonException)
        {
            return null;
        }
    }

    /// <summary>The read itself: admission, then the tool, recording the outcome. A cached read runs it once for every
    /// caller waiting on it, under the flight's token.</summary>
    private async Task<CallToolResult> RunReadAsync(McpRequestHandler<CallToolRequestParams, CallToolResult> next,
        RequestContext<CallToolRequestParams> request, string toolName, string windowBand, ReadScope scope, string caller,
        CancellationToken cancellationToken)
    {
        /* #5506 part 3: admission, after the scope and the band. A refused read answers busy and records busy; a caller
           that leaves the queue records cancelled; a queued read records its wait as one queue_wait sample. The
           recorded elapsed time starts after admission (D1), so the read's own percentiles never hold queue time. */
        using var ticket = _admission is null || !ReadAdmission.IsAdmitted(toolName)
            ? ReadAdmissionTicket.Passed
            : await ReadScope.AdmitAsync(_admission, ReadSurface.Mcp, toolName, windowBand, cancellationToken);
        if (ticket.Refusal is { } refusal)
        {
            Record(toolName, ReadOutcome.Busy, ticket.QueueMs ?? 0, windowBand, scope, caller);
            return BusyResult(refusal);
        }

        if (ticket.CallerLeft)
        {
            Record(toolName, ReadOutcome.Cancelled, ticket.QueueMs ?? 0, windowBand, scope, caller);
            throw new OperationCanceledException(cancellationToken);
        }

        if (ticket.QueueMs is { } queueMs)
        {
            Record(toolName, ReadOutcome.QueueWait, queueMs, windowBand, scope, caller);
        }

        var stopwatch = Stopwatch.StartNew();
        try
        {
            var result = await next(request, cancellationToken);
            var outcome = ReadScope.Resolve(ClassifyResult(result, cancellationToken), scope.Fallback);
            Record(toolName, outcome, stopwatch.ElapsedMilliseconds, windowBand, scope, caller);
            OfferSlow(scope, toolName, outcome, stopwatch.ElapsedMilliseconds, request, outcome == ReadOutcome.Error ? "tool_error" : SlowReadLog.ErrorClassOf(outcome), caller);
            return result;
        }
        catch (Exception ex)
        {
            var outcome = ReadScope.Resolve(ReadOutcomeClassifier.Classify(ex, cancellationToken), scope.Fallback);
            Record(toolName, outcome, stopwatch.ElapsedMilliseconds, windowBand, scope, caller);
            OfferSlow(scope, toolName, outcome, stopwatch.ElapsedMilliseconds, request, SlowReadLog.ErrorClassOf(ex), caller);
            throw;
        }
    }

    /// <summary>#6800: <see cref="IsFleetRead(string, IDictionary{string, JsonElement}?, McpToolParameterTypes?)"/> for a request. Never
    /// throws; a call whose window it cannot read is not a fleet read.</summary>
    private bool IsFleetRead(ModelContextProtocol.Server.RequestContext<CallToolRequestParams> request)
    {
        try
        {
            var types = request.Services?.GetService(typeof(McpToolParameterTypes)) as McpToolParameterTypes;
            return IsFleetRead(request.Params?.Name ?? string.Empty, request.Params?.Arguments, types);
        }
        catch (Exception ex)
        {
            _logger?.LogDebug(ex, "Whether MCP tool {Tool} read the fleet was not read.", request.Params?.Name);
            return false;
        }
    }

    /// <summary>#6800: whether a call names no server (<c>server_name</c> absent, null or blank, on a route in
    /// <see cref="ReadAdmission.FleetCachedRoutes"/>) over a window of 24 hours or more (<see cref="WindowHoursOf"/>, the sent
    /// <c>hours_back</c> or the tool's declared default). A <c>server_name</c> of another kind is not a server name, so it is not read
    /// as the fleet.</summary>
    internal static bool IsFleetRead(string toolName, System.Collections.Generic.IDictionary<string, System.Text.Json.JsonElement>? arguments, McpToolParameterTypes? types)
    {
        var hours = WindowHoursOf(toolName, arguments, types);
        if (arguments is null || !arguments.TryGetValue("server_name", out var value))
        {
            return ReadAdmission.IsFleetRead(toolName, null, hours);
        }

        return value.ValueKind switch
        {
            System.Text.Json.JsonValueKind.Null or System.Text.Json.JsonValueKind.Undefined => ReadAdmission.IsFleetRead(toolName, null, hours),
            System.Text.Json.JsonValueKind.String => ReadAdmission.IsFleetRead(toolName, value.GetString(), hours),
            _ => false,
        };
    }

    /// <summary>
    /// #5506 part 3: a heavy read through the read cache. A stored answer is returned at once and records
    /// <c>cache_hit</c>. Otherwise the caller joins the identical read running now, or starts it (D6): the caller that
    /// starts it records the read's real outcome through <see cref="RunReadAsync"/>; a caller that joined records only its
    /// own outcome with its own elapsed time (round 2, M1: <see cref="ReadResultCache.JoinedOutcome"/>), so one read counts
    /// once. Neither a hit nor a joined caller counts as a read in flight (round 2, L3).
    /// </summary>
    private async Task<CallToolResult> ServeCachedAsync(ReadAdmission admission, McpRequestHandler<CallToolRequestParams, CallToolResult> next,
        RequestContext<CallToolRequestParams> request, string toolName, string windowBand, bool fleetRead, ReadScope scope, string caller,
        CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        var cache = admission.Cache;
        var gcf = (request.Services?.GetService(typeof(IGcfOutputFormat)) as IGcfOutputFormat)?.Enabled ?? GcfOutput.Enabled;
        var types = request.Services?.GetService(typeof(McpToolParameterTypes)) as McpToolParameterTypes;
        var ttl = ReadResultCache.TtlFor(toolName, windowBand, fleetRead);
        var key = ReadCacheKey.ForMcp(toolName, request.Params?.Arguments, types, gcf, cache.Generation, cache, ttl,
            heldFromStore: ReadAdmission.FleetTtlApplies(toolName, fleetRead));
        if (cache.TryGet(key, out var stored))
        {
            scope.Uncount();
            admission.NoteCacheHit(ReadSurface.Mcp, toolName);
            Record(toolName, ReadOutcome.CacheHit, stopwatch.ElapsedMilliseconds, windowBand, scope, caller);
            return TextResult(stored);
        }

        var flight = cache.Join(key, ttl, token => RunReadAsync(next, request, toolName, windowBand, scope, caller, token),
            result => CacheableText(result, gcf));
        if (flight.IsLead)
        {
            admission.NoteCacheMiss(ReadSurface.Mcp, toolName);
            return await flight.WaitAsync(cancellationToken);
        }

        scope.Uncount();
        try
        {
            var shared = await flight.WaitAsync(cancellationToken);
            var outcome = ReadResultCache.JoinedOutcome(IsBusyEnvelope(shared) ? ReadOutcome.Busy : null, cancellationToken);
            if (outcome == ReadOutcome.CacheHit)
            {
                admission.NoteCacheHit(ReadSurface.Mcp, toolName);
            }

            Record(toolName, outcome, stopwatch.ElapsedMilliseconds, windowBand, scope, caller);
            return CacheableText(shared, gcf) is { } text ? TextResult(text) : shared;
        }
        catch (Exception ex)
        {
            var outcome = ReadResultCache.JoinedOutcome(
                ReadOutcomeClassifier.Classify(ex, cancellationToken) == ReadOutcome.Cancelled ? ReadOutcome.Cancelled : null, cancellationToken);
            if (outcome == ReadOutcome.CacheHit)
            {
                admission.NoteCacheHit(ReadSurface.Mcp, toolName);
            }

            Record(toolName, outcome, stopwatch.ElapsedMilliseconds, windowBand, scope, caller);
            throw;
        }
    }

    /// <summary>The text of a result the cache may store: one plain text block, no error flag, no structured content or
    /// metadata, and text <see cref="ReadResultCache.IsCacheableText"/> accepts. Null for anything else.</summary>
    internal static string? CacheableText(CallToolResult result, bool gcf) =>
        result.IsError != true && result.StructuredContent is null && result.Meta is null
        && result.Content is { Count: 1 } content && content[0] is TextContentBlock { Meta: null, Annotations: null } text
        && ReadResultCache.IsCacheableText(text.Text, gcf)
            ? text.Text
            : null;

    private static CallToolResult TextResult(string text) => new()
    {
        Content = [new TextContentBlock { Text = text }],
    };

    /// <summary>Whether a finished call moves the cache's write generation: a successful write tool, or a successful
    /// <c>analyze_server</c> with no <c>as_of</c> (D5: it stores findings).</summary>
    internal static bool MovesGeneration(string toolName, System.Collections.Generic.IDictionary<string, System.Text.Json.JsonElement>? arguments, CallToolResult result)
    {
        var writes = DarlingMcpWritePolicy.IsWrite(toolName)
            || (string.Equals(toolName, ReadResultCache.AnalyzeServer, StringComparison.Ordinal)
                && (arguments is null || !arguments.TryGetValue("as_of", out var asOf)
                    || asOf.ValueKind != System.Text.Json.JsonValueKind.String || string.IsNullOrEmpty(asOf.GetString())));
        return writes && ClassifyResult(result, CancellationToken.None) == ReadOutcome.Ok;
    }

    /// <summary>#5506 part 3: the MCP answer to a refused read, <c>busy</c> with the error flag set.</summary>
    internal static CallToolResult BusyResult(ReadAdmissionRefusal refusal) => new()
    {
        IsError = true,
        Content = [new TextContentBlock { Text = ReadAdmission.McpBusyText(refusal) }],
    };

    /* #5506 part 3 (round 1, L6): set by the run_custom_view_panel arm before the tool runs; the tool sets it when it
       answers busy, so the arm needs no parse of a text GCF may have re-encoded. */
    private static readonly AsyncLocal<StrongBox<bool>?> s_busyAnswer = new();

    /// <summary>#5506 part 3 (round 1, L6): a tool notes that the text it returns is the busy envelope, so this filter sets
    /// the error flag whatever form the text reaches it in. A no-op outside a call this filter wraps.</summary>
    internal static void NoteBusyAnswer()
    {
        if (s_busyAnswer.Value is { } busy)
        {
            busy.Value = true;
        }
    }

    /// <summary>The JSON form of a status envelope that GCF re-encoded (round 1, L6): <c>{"status", "message"}</c> from the
    /// wire's top level, so the envelope checks below read the same words with GCF on or off. Text that is already JSON,
    /// or that does not decode to an object with a <c>status</c> word, is returned as it was.</summary>
    internal static string EnvelopeText(string text)
    {
        var first = text.AsSpan().TrimStart();
        if (first.Length == 0 || first[0] == '{' || first[0] == '[')
        {
            return text;
        }

        try
        {
            if (BlackwellSystems.Gcf.Gcf.DecodeGeneric(text) is BlackwellSystems.Gcf.OrderedMap map
                && map.TryGetValue("status", out var status) && status is string word)
            {
                var envelope = new JsonObject { ["status"] = word };
                if (map.TryGetValue("message", out var message) && message is string sentence)
                {
                    envelope["message"] = sentence;
                }

                return envelope.ToJsonString();
            }
        }
#pragma warning disable CA1031 // text that is not a GCF wire is simply not an envelope
        catch (Exception)
#pragma warning restore CA1031
        {
        }

        return text;
    }

    /// <summary>Whether a result is the busy envelope (a composed panel the shared runner refused), as JSON or as the GCF
    /// wire of one.</summary>
    internal static bool IsBusyEnvelope(CallToolResult result)
    {
        if (result.Content is not { Count: 1 } content || content[0] is not TextContentBlock block)
        {
            return false;
        }

        var text = EnvelopeText(block.Text);
        if (!text.StartsWith('{') || !text.Contains("\"busy\"", StringComparison.Ordinal))
        {
            return false;
        }

        try
        {
            return JsonNode.Parse(text) is JsonObject body
                && body["status"] is JsonValue status && status.TryGetValue<string>(out var word) && word == ReadAdmission.BusyStatus;
        }
        catch (System.Text.Json.JsonException)
        {
            return false;
        }
    }

    /// <summary>Classifies a tool's normal (non-throwing) return: an explicit <see cref="CallToolResult.IsError"/>
    /// or a lone text block carrying the caught-error envelope (<c>McpHelpers.FormatError</c>'s shape) goes
    /// through <see cref="ReadOutcomeClassifier.ClassifySentence"/>, so a 57014 statement-timeout sentence
    /// classifies as <see cref="ReadOutcome.Timeout"/> exactly as the web surface's twin arm does. Everything
    /// else is <see cref="ReadOutcome.Ok"/>. Internal (not private) so a unit test can pin the mapping directly,
    /// without a running server.</summary>
    internal static ReadOutcome ClassifyResult(CallToolResult result, System.Threading.CancellationToken cancellationToken)
    {
        /* Round 1, L6: an envelope GCF re-encoded is read through its JSON form, so it is not counted as ok. */
        if (result.Content is { Count: 1 } content && content[0] is TextContentBlock block
            && EnvelopeText(block.Text) is var text
            && (result.IsError == true || McpHelpers.IsErrorEnvelope(text)))
        {
            return ReadOutcomeClassifier.ClassifySentence(McpHelpers.ErrorMessageOf(text), cancellationToken);
        }

        return result.IsError == true ? ReadOutcome.Error : ReadOutcome.Ok;
    }

    /// <summary>The arguments a slow-read record keeps for one MCP call. A write tool's arguments are the new values it
    /// sets (URLs, rule text, notes), so only their names are kept and every value is the redaction marker; a read keeps
    /// its values, which the slow-read log then redacts by key and sweeps for statements. Internal so a test can pin it.</summary>
    internal static JsonObject? SlowReadArguments(string toolName, IDictionary<string, System.Text.Json.JsonElement>? supplied)
    {
        if (supplied is null)
        {
            return null;
        }

        var isWrite = DarlingMcpWritePolicy.IsWrite(toolName);
        var arguments = new JsonObject();
        foreach (var (key, value) in supplied)
        {
            arguments[key] = isWrite
                ? JsonValue.Create(SecretTextGuard.RedactedMarker)
                : JsonNode.Parse(value.GetRawText());
        }

        return arguments;
    }

    /// <summary>Offers a slow or failed call to the slow-read record (#5097). The arguments are built only when the
    /// call qualifies, and nothing here can reach the request.</summary>
    private void OfferSlow(ReadScope scope, string toolName, ReadOutcome outcome, long elapsedMs,
        ModelContextProtocol.Server.RequestContext<CallToolRequestParams> request, string? errorClass, string caller)
    {
        if (_slowReads is null || !_slowReads.ShouldRecord(outcome, elapsedMs))
        {
            return;
        }

        JsonObject? arguments = null;
        try
        {
            arguments = SlowReadArguments(toolName, request.Params?.Arguments);
        }
        catch (Exception ex)
        {
            _logger?.LogDebug(ex, "Slow-read arguments for MCP tool {Tool} were not read.", toolName);
        }

        _slowReads.Offer(scope, ReadSurface.Mcp, toolName, outcome, elapsedMs, arguments, errorClass, _logger, caller);
    }

    /// <summary>The window band of the call (#5506): the window arguments the caller sent, else the defaults the tool
    /// method declares for them. Never throws; a call it cannot read records <c>none</c>.</summary>
    private string WindowBandOf(ModelContextProtocol.Server.RequestContext<CallToolRequestParams> request)
    {
        try
        {
            var types = request.Services?.GetService(typeof(McpToolParameterTypes)) as McpToolParameterTypes;
            return WindowBandOf(request.Params?.Name, request.Params?.Arguments, types);
        }
        catch (Exception ex)
        {
            _logger?.LogDebug(ex, "The window band of MCP tool {Tool} was not read.", request.Params?.Name);
            return ReadWindowBand.None;
        }
    }

    /// <summary>The window band of a call to <paramref name="toolName"/>. An argument the caller left out takes the default
    /// the tool method declares, read from <paramref name="types"/> (which <c>McpSchemaCompat.WithGeminiCompatibleTools</c>
    /// fills from the method as it creates each tool), never from the served input schema: that schema has every
    /// <c>default</c> keyword stripped for Gemini, so a band read from it would be <c>none</c> for the commonest call, the
    /// one that leaves <c>hours_back</c> out. A tool whose window is fixed in code takes its <see cref="ReadWindowBand.FixedHours"/>
    /// band (#5814). Internal so a test can pin it against a really registered tool.</summary>
    internal static string WindowBandOf(string? toolName, System.Collections.Generic.IDictionary<string, System.Text.Json.JsonElement>? arguments, McpToolParameterTypes? types) =>
        ReadWindowBand.OrFixed(ReadWindowBand.Of(WindowHoursOf(toolName, arguments, types)), toolName);

    /// <summary>The window in hours of a call to <paramref name="toolName"/>, the number <see cref="WindowBandOf(string?, IDictionary{string, JsonElement}?, McpToolParameterTypes?)"/> bands:
    /// the argument the caller sent, else the default the tool method declares; null when there is none.</summary>
    internal static double? WindowHoursOf(string? toolName, System.Collections.Generic.IDictionary<string, System.Text.Json.JsonElement>? arguments, McpToolParameterTypes? types) =>
        ReadWindowBand.HoursOfToolCall(arguments, key =>
            types is not null && !string.IsNullOrEmpty(toolName) && types.TryGetDefault(toolName, key, out var value)
                ? ReadWindowBand.DefaultText(value)
                : null);

    /// <summary>The server-side identity of the call (#6231): the bearer token the HTTP request carried, as a short fingerprint
    /// (<see cref="ReadCaller.OfMcpToken"/>), never the token. The client's own name is not part of it. Never throws.</summary>
    private string TokenIdentityOf(ModelContextProtocol.Server.RequestContext<CallToolRequestParams> request)
    {
        try
        {
            if (request.Services?.GetService(typeof(IHttpContextAccessor)) is IHttpContextAccessor accessor)
            {
                return ReadCaller.OfMcpIdentity(
                    DarlingMcpTokenSet.CallerOf(accessor)?.Name,
                    DarlingMcpHostService.ExtractBearerToken(accessor.HttpContext?.Request.Headers.Authorization.ToString()));
            }
        }
        catch (Exception ex)
        {
            _logger?.LogDebug(ex, "The token identity of MCP tool {Tool} was not read.", request.Params?.Name);
        }

        return "token unknown";
    }

    /// <summary>The caller of the call (#5793): the client info the request's server holds (sent per request from protocol
    /// 2026-07-28; the stateless host has none for an older client's tool call), else the first product token of the HTTP
    /// request's User-Agent, else <c>unknown</c>. The User-Agent is the only part of the request read. Never throws.</summary>
    private string CallerOf(ModelContextProtocol.Server.RequestContext<CallToolRequestParams> request)
    {
        try
        {
            var clientInfo = request.Server?.ClientInfo;
            string? userAgent = null;
            if (string.IsNullOrWhiteSpace(clientInfo?.Name)
                && request.Services?.GetService(typeof(IHttpContextAccessor)) is IHttpContextAccessor accessor)
            {
                userAgent = accessor.HttpContext?.Request.Headers.UserAgent.ToString();
            }

            return ReadCaller.OfMcpClient(clientInfo?.Name, clientInfo?.Version, userAgent);
        }
        catch (Exception ex)
        {
            _logger?.LogDebug(ex, "The caller of MCP tool {Tool} was not read.", request.Params?.Name);
            return ReadCaller.Unknown;
        }
    }

    private void Record(string toolName, ReadOutcome outcome, long elapsedMs, string windowBand, ReadScope scope, string caller)
    {
        try
        {
            _readLatency.Record(ReadSurface.Mcp, toolName, outcome, elapsedMs, windowBand, scope.InFlightAtStart, scope.WaitMs, caller);
        }
        catch (Exception ex)
        {
            _logger?.LogDebug(ex, "Read-latency recording failed for MCP tool {Tool}.", toolName);
        }
    }
}
