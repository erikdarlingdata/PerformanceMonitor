/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;

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

    private readonly ReadLatencyAccumulator _readLatency;
    private readonly ILogger? _logger;

    public McpToolLatencyFilter(ReadLatencyAccumulator readLatency, ILogger? logger)
    {
        _readLatency = readLatency ?? throw new ArgumentNullException(nameof(readLatency));
        _logger = logger;
    }

    /// <summary>The filter: run the tool, time it, record the outcome. A filter is <c>next =&gt; handler</c>.</summary>
    public McpRequestFilter<CallToolRequestParams, CallToolResult> AsFilter() =>
        next => async (request, cancellationToken) =>
        {
            var toolName = request.Params?.Name;
            if (string.IsNullOrEmpty(toolName) || string.Equals(toolName, SkippedToolName, StringComparison.Ordinal))
            {
                return await next(request, cancellationToken);
            }

            var stopwatch = Stopwatch.StartNew();
            using var readScope = ReadScope.Open(_logger);
            readScope.Scope.CaptureStatements = true;
            try
            {
                var result = await next(request, cancellationToken);
                Record(toolName, ReadScope.Resolve(ClassifyResult(result, cancellationToken), readScope.Fallback), stopwatch.ElapsedMilliseconds);
                return result;
            }
            catch (Exception ex)
            {
                Record(toolName, ReadScope.Resolve(ReadOutcomeClassifier.Classify(ex, cancellationToken), readScope.Fallback), stopwatch.ElapsedMilliseconds);
                throw;
            }
        };

    /// <summary>Classifies a tool's normal (non-throwing) return: an explicit <see cref="CallToolResult.IsError"/>
    /// or a lone text block carrying the caught-error envelope (<c>McpHelpers.FormatError</c>'s shape) goes
    /// through <see cref="ReadOutcomeClassifier.ClassifySentence"/>, so a 57014 statement-timeout sentence
    /// classifies as <see cref="ReadOutcome.Timeout"/> exactly as the web surface's twin arm does. Everything
    /// else is <see cref="ReadOutcome.Ok"/>. Internal (not private) so a unit test can pin the mapping directly,
    /// without a running server.</summary>
    internal static ReadOutcome ClassifyResult(CallToolResult result, System.Threading.CancellationToken cancellationToken)
    {
        if (result.Content is { Count: 1 } content && content[0] is TextContentBlock text
            && (result.IsError == true || McpHelpers.IsErrorEnvelope(text.Text)))
        {
            return ReadOutcomeClassifier.ClassifySentence(McpHelpers.ErrorMessageOf(text.Text), cancellationToken);
        }

        return result.IsError == true ? ReadOutcome.Error : ReadOutcome.Ok;
    }

    private void Record(string toolName, ReadOutcome outcome, long elapsedMs)
    {
        try
        {
            _readLatency.Record(ReadSurface.Mcp, toolName, outcome, elapsedMs);
        }
        catch (Exception ex)
        {
            _logger?.LogDebug(ex, "Read-latency recording failed for MCP tool {Tool}.", toolName);
        }
    }
}
