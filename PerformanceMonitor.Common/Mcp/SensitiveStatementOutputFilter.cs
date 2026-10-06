/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;

namespace PerformanceMonitor.Common;

/// <summary>
/// #4348: the statement filter's one choke point for MCP output. A call-tool filter that runs each tool's result
/// through <see cref="SensitiveStatements"/> before it leaves the host, so every tool, current and future, is
/// covered without a per-tool change. Error results are swept too: an error sentence can echo a statement.
///
/// <para><b>Order.</b> The first filter added is the outermost, so a host registers this one LAST: it then sits next
/// to the tool and reads the tool's own JSON, before any output re-encoder (GCF) reshapes it. Registered earlier,
/// a re-encoder would turn a line feed between two tokens into two characters and the judge would no longer see
/// the statement.</para>
///
/// <para><b>Budget.</b> One 1.5 s budget covers every block of one result. Past it, every later value comes back as
/// the marker. A result the sweep cannot read is replaced by <see cref="SensitiveStatements.JsonRefusal"/> as an
/// error result, never passed through.</para>
/// </summary>
public static class SensitiveStatementOutputFilter
{
    /// <summary>The filter: run the tool, then sweep its result. A filter is <c>next =&gt; handler</c>.</summary>
    public static McpRequestFilter<CallToolRequestParams, CallToolResult> Instance =>
        next => async (request, cancellationToken) => Sweep(await next(request, cancellationToken));

    /// <summary>
    /// Returns <paramref name="result"/> itself when nothing in it is named, otherwise a copy with each named
    /// string replaced. Never throws and never returns the input after a failure.
    /// </summary>
    public static CallToolResult Sweep(CallToolResult result)
    {
        if (result is null) return result!;

        try
        {
            var budget = new SensitiveStatements.JudgeBudget(SensitiveStatements.ReadBudget);
            bool changed = false;

            List<ContentBlock>? content = null;
            if (result.Content is { Count: > 0 } blocks)
            {
                for (int i = 0; i < blocks.Count; i++)
                {
                    ContentBlock? swept = SweepBlock(blocks[i], budget);
                    if (ReferenceEquals(swept, blocks[i])) continue;
                    content ??= new List<ContentBlock>(blocks);
                    content[i] = swept!;
                    changed = true;
                }
            }

            JsonElement? structured = result.StructuredContent;
            if (structured is not null)
            {
                string json = structured.Value.GetRawText();
                string judged = Judge(json, budget);
                if (!ReferenceEquals(judged, json))
                {
                    using JsonDocument parsed = JsonDocument.Parse(judged);
                    structured = parsed.RootElement.Clone();
                    changed = true;
                }
            }

            if (!changed) return result;

            return new CallToolResult
            {
                Content = content ?? result.Content,
                StructuredContent = structured,
                IsError = result.IsError,
                Meta = result.Meta,
            };
        }
#pragma warning disable CA1031 // fail closed: the sweep never lets the input through after a failure
        catch (Exception)
#pragma warning restore CA1031
        {
            return Refusal();
        }
    }

    private static ContentBlock? SweepBlock(ContentBlock block, SensitiveStatements.JudgeBudget budget)
    {
        switch (block)
        {
            case TextContentBlock text:
            {
                if (string.IsNullOrEmpty(text.Text)) return block;
                string judged = Judge(text.Text, budget);
                if (ReferenceEquals(judged, text.Text)) return block;
                return new TextContentBlock { Text = judged, Annotations = text.Annotations, Meta = text.Meta };
            }
            case EmbeddedResourceBlock { Resource: TextResourceContents resource } embedded:
            {
                if (string.IsNullOrEmpty(resource.Text)) return block;
                string judged = Judge(resource.Text, budget);
                if (ReferenceEquals(judged, resource.Text)) return block;
                return new EmbeddedResourceBlock
                {
                    Resource = new TextResourceContents
                    {
                        Uri = resource.Uri,
                        MimeType = resource.MimeType,
                        Text = judged,
                        Meta = resource.Meta,
                    },
                    Annotations = embedded.Annotations,
                    Meta = embedded.Meta,
                };
            }
            default:
                return block;
        }
    }

    /// <summary>Sweeps one string. A refusal from the walk is thrown, so the whole result becomes the refusal
    /// (as an error result) instead of one block of it.</summary>
    private static string Judge(string output, SensitiveStatements.JudgeBudget budget)
    {
        string judged = SensitiveStatements.Json(output, budget);
        if (string.Equals(judged, SensitiveStatements.JsonRefusal, StringComparison.Ordinal)
            && !string.Equals(output, SensitiveStatements.JsonRefusal, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(SensitiveStatements.JsonRefusal);
        }

        return judged;
    }

    private static CallToolResult Refusal() => new()
    {
        IsError = true,
        Content = new List<ContentBlock> { new TextContentBlock { Text = SensitiveStatements.JsonRefusal } },
    };
}
