using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;

namespace PerformanceMonitor.Darling.Service.Mcp;

// A single call-tool filter that offers each tool's result as GCF instead of JSON when
// DARLING_OUTPUT_FORMAT=gcf. Registered once in DarlingMcpHostService, so it covers every
// tool with no per-tool changes. The re-encode is conservative (never larger, never lossy,
// see GcfOutput); anything it cannot faithfully shrink is returned as the original JSON.
public static class GcfCallToolFilter
{
    // The filter: run the tool, then transform its result. A filter is `next => handler`.
    public static McpRequestFilter<CallToolRequestParams, CallToolResult> Instance =>
        next => async (request, cancellationToken) =>
        {
            var result = await next(request, cancellationToken);
            // #5320: a host that registers an IGcfOutputFormat pins the format; the real service
            // registers none and keeps reading DARLING_OUTPUT_FORMAT on each call.
            bool enabled = request.Services?.GetService<IGcfOutputFormat>()?.Enabled ?? GcfOutput.Enabled;
            return Transform(result, enabled);
        };

    // Replaces a single JSON text-content block with its GCF wire when GCF is enabled and
    // the wire is smaller and lossless; otherwise returns the result unchanged. Exposed for
    // testing. StructuredContent (if a tool sets it) is left untouched.
    public static CallToolResult Transform(CallToolResult result) => Transform(result, GcfOutput.Enabled);

    // The same, with the format decision handed in (the filter resolves it per request).
    public static CallToolResult Transform(CallToolResult result, bool enabled)
    {
        if (!enabled || result.Content == null || result.IsError == true)
            return result;

        // Only a lone text block is re-encoded, so an image or other block sent alongside
        // it is never dropped.
        if (result.Content.Count != 1 || result.Content[0] is not TextContentBlock text)
            return result;

        var wire = GcfOutput.TryEncode(text.Text);
        if (wire == null)
            return result;

        // Return a new result with only the text block replaced, rather than mutating the
        // one the tool produced; the other fields are carried over unchanged.
        return new CallToolResult
        {
            Content = new List<ContentBlock> { new TextContentBlock { Text = wire } },
            StructuredContent = result.StructuredContent,
            IsError = result.IsError,
            Meta = result.Meta,
        };
    }
}
