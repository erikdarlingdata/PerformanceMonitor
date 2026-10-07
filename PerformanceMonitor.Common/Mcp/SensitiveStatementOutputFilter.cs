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
using System.Text.Json.Nodes;
using ModelContextProtocol;
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
/// <para><b>Budget.</b> One budget covers every block of one result: 1.5 s, plus 0.5 s per 1,048,576 characters of each
/// distinct block, up to 10 s (#5477). Past it, every later value comes back as
/// the marker. A result the sweep cannot read is replaced by <see cref="SensitiveStatements.JsonRefusal"/> as an
/// error result, never passed through.</para>
/// </summary>
public static class SensitiveStatementOutputFilter
{
    /// <summary>The filter: run the tool, then sweep its result. A filter is <c>next =&gt; handler</c>.</summary>
    public static McpRequestFilter<CallToolRequestParams, CallToolResult> Instance =>
        next => async (request, cancellationToken) =>
        {
            try
            {
                return Sweep(await next(request, cancellationToken));
            }
            catch (McpException ex)
            {
                // #4348: the SDK turns a thrown McpException into an error result of its own, from the exception's
                // message, OUTSIDE this filter's result sweep. A tool's message can echo a statement, so the message
                // is swept here and the exception rethrown with the same type, error code and inner exception.
                throw WithSweptMessage(ex);
            }
        };

    /// <summary>
    /// A copy of <paramref name="ex"/> whose message is swept (the same object when nothing in it is named). A
    /// message the sweep cannot read becomes <see cref="SensitiveStatements.JsonRefusal"/>, never the raw text.
    /// </summary>
    private static McpException WithSweptMessage(McpException ex)
    {
        string swept;
        try
        {
            swept = Judge(ex.Message, new SensitiveStatements.JudgeBudget(SensitiveStatements.ReadBudget));
        }
#pragma warning disable CA1031 // fail closed: the sweep never lets the message through after a failure
        catch (Exception)
#pragma warning restore CA1031
        {
            swept = SensitiveStatements.JsonRefusal;
        }

        if (ReferenceEquals(swept, ex.Message)) return ex;
        return ex is McpProtocolException protocol
            ? new McpProtocolException(swept, ex.InnerException, protocol.ErrorCode)
            : new McpException(swept, ex.InnerException);
    }

    /// <summary>
    /// Returns <paramref name="result"/> itself when nothing in it is named, otherwise a copy with each named
    /// string replaced. Never throws and never returns the input after a failure.
    /// </summary>
    public static CallToolResult Sweep(CallToolResult result)
    {
        if (result is null) return result!;

        try
        {
            /* #5478 / #5484: a large read in the first seconds of the process would walk cold code and be withheld, so
               a result of 256 KB or more first waits for the start-up warm-up, up to 3 s after it started. The sweep
               runs on a pool thread, so the blocking wait is fine. */
            SensitiveStatements.WaitForWarmUp(SweptChars(result));
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

            JsonObject? meta = SweepMeta(result.Meta, budget);
            if (!ReferenceEquals(meta, result.Meta)) changed = true;

            if (!changed) return result;

            return new CallToolResult
            {
                Content = content ?? result.Content,
                StructuredContent = structured,
                IsError = result.IsError,
                Meta = meta,
            };
        }
#pragma warning disable CA1031 // fail closed: the sweep never lets the input through after a failure
        catch (Exception)
#pragma warning restore CA1031
        {
            return Refusal();
        }
    }

    /// <summary>The characters the sweep will judge in <paramref name="result"/>: the text of each text block and text
    /// resource, and the structured content. The size gate of the warm-up wait; a block the sweep cannot read
    /// counts for nothing here because it is refused, not walked.</summary>
    private static long SweptChars(CallToolResult result)
    {
        long total = 0;
        if (result.Content is { Count: > 0 } blocks)
        {
            foreach (ContentBlock block in blocks)
            {
                total += block switch
                {
                    TextContentBlock text => text.Text?.Length ?? 0,
                    EmbeddedResourceBlock { Resource: TextResourceContents resource } => resource.Text?.Length ?? 0,
                    _ => 0,
                };
            }
        }

        if (result.StructuredContent is { } structured)
        {
            total += structured.GetRawText().Length;
        }

        return total;
    }

    /// <summary>Sweeps one block. A block type this filter does not know (an image, audio, a blob resource, a
    /// resource link, anything a later SDK adds) cannot be read, so it is not passed through: it throws, and the
    /// caller turns the whole result into the fixed refusal.</summary>
    private static ContentBlock? SweepBlock(ContentBlock block, SensitiveStatements.JudgeBudget budget)
    {
        switch (block)
        {
            case TextContentBlock text:
            {
                JsonObject? meta = SweepMeta(text.Meta, budget);
                if (string.IsNullOrEmpty(text.Text))
                    return ReferenceEquals(meta, text.Meta)
                        ? block
                        : new TextContentBlock { Text = text.Text, Annotations = text.Annotations, Meta = meta };
                string judged = Judge(text.Text, budget);
                if (ReferenceEquals(judged, text.Text) && ReferenceEquals(meta, text.Meta)) return block;
                return new TextContentBlock { Text = judged, Annotations = text.Annotations, Meta = meta };
            }
            case EmbeddedResourceBlock { Resource: TextResourceContents resource } embedded:
            {
                JsonObject? blockMeta = SweepMeta(embedded.Meta, budget);
                JsonObject? resourceMeta = SweepMeta(resource.Meta, budget);
                // A URI or MIME type with a named statement in it is useless with the statement replaced, so it
                // refuses the whole result (the block keeps a valid shape) instead of rewriting the value.
                RefuseIfNamed(resource.Uri, budget);
                RefuseIfNamed(resource.MimeType, budget);
                string judged = string.IsNullOrEmpty(resource.Text) ? resource.Text : Judge(resource.Text, budget);
                if (ReferenceEquals(judged, resource.Text)
                    && ReferenceEquals(blockMeta, embedded.Meta)
                    && ReferenceEquals(resourceMeta, resource.Meta))
                {
                    return block;
                }

                return new EmbeddedResourceBlock
                {
                    Resource = new TextResourceContents
                    {
                        Uri = resource.Uri,
                        MimeType = resource.MimeType,
                        Text = judged,
                        Meta = resourceMeta,
                    },
                    Annotations = embedded.Annotations,
                    Meta = blockMeta,
                };
            }
            default:
                throw new InvalidOperationException(SensitiveStatements.JsonRefusal);
        }
    }

    /// <summary>Sweeps a <c>_meta</c> object under the shared budget: it is judged as the JSON it is, so a named
    /// string inside it becomes the marker. The same instance comes back when nothing in it is named.</summary>
    private static JsonObject? SweepMeta(JsonObject? meta, SensitiveStatements.JudgeBudget budget)
    {
        if (meta is null) return null;
        string json = meta.ToJsonString();
        string judged = Judge(json, budget);
        if (ReferenceEquals(judged, json)) return meta;
        return JsonNode.Parse(judged) as JsonObject ?? throw new InvalidOperationException(SensitiveStatements.JsonRefusal);
    }

    /// <summary>Judges a value that cannot be rewritten (a resource URI or MIME type) under the shared budget and
    /// throws the refusal when anything in it is named.</summary>
    private static void RefuseIfNamed(string? value, SensitiveStatements.JudgeBudget budget)
    {
        if (string.IsNullOrEmpty(value)) return;
        if (!ReferenceEquals(Judge(value, budget), value))
            throw new InvalidOperationException(SensitiveStatements.JsonRefusal);
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
