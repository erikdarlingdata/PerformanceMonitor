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
using ModelContextProtocol;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;

namespace PerformanceMonitor.Common;

/// <summary>
/// The host filter of <c>mcp.dataDetail</c> (#5632): applies <see cref="McpDataDetailMasker"/> to every tool answer
/// on the listener, the investigation tools included, at the level the host registered as a
/// <see cref="McpDataDetailSetting"/>. No setting, or <see cref="McpDataDetail.Full"/>, passes every answer through
/// untouched. It sits beside the statement filter (<see cref="SensitiveStatementOutputFilter"/>), registered just
/// before it so it reads the statement filter's output as plain JSON; the readers never mask.
/// <list type="bullet">
/// <item>Text blocks, embedded text resources and structured content are masked; a block of any other kind refuses
/// the whole answer, like the statement filter.</item>
/// <item>A thrown <see cref="McpException"/> has its message masked as error text and is rethrown with the same type
/// and error code.</item>
/// <item>Any failure refuses the whole answer, never passes it through.</item>
/// </list>
/// </summary>
public static class McpDataDetailFilter
{
    /// <summary>The call-tool filter.</summary>
    public static McpRequestFilter<CallToolRequestParams, CallToolResult> Instance =>
        next => async (request, cancellationToken) =>
        {
            McpDataDetail mode = (request.Services?.GetService(typeof(McpDataDetailSetting)) as McpDataDetailSetting)?.Mode
                ?? McpDataDetail.Full;
            if (mode == McpDataDetail.Full) return await next(request, cancellationToken).ConfigureAwait(false);

            try
            {
                return Mask(await next(request, cancellationToken).ConfigureAwait(false), mode);
            }
            catch (McpException ex)
            {
                throw WithMaskedMessage(ex, mode);
            }
        };

    /// <summary><paramref name="result"/> masked at <paramref name="mode"/>; the SAME instance when nothing
    /// changed.</summary>
    public static CallToolResult Mask(CallToolResult result, McpDataDetail mode)
    {
        if (result is null || mode == McpDataDetail.Full) return result!;

        try
        {
            // One masker for the whole answer (#5632): every block and the structured content share one login#n and
            // host#n numbering, so one login is one number throughout the answer.
            McpDataDetailMasker masker = McpDataDetailMasker.ForAnswer(mode);
            bool changed = false;
            List<ContentBlock>? content = null;
            if (result.Content is { Count: > 0 } blocks)
            {
                for (int i = 0; i < blocks.Count; i++)
                {
                    ContentBlock masked = MaskBlock(blocks[i], masker);
                    if (ReferenceEquals(masked, blocks[i])) continue;
                    content ??= new List<ContentBlock>(blocks);
                    content[i] = masked;
                    changed = true;
                }
            }

            JsonElement? structured = result.StructuredContent;
            if (structured is not null)
            {
                string json = structured.Value.GetRawText();
                string masked = Checked(masker.Answer(json), json);
                if (!ReferenceEquals(masked, json))
                {
                    using JsonDocument parsed = JsonDocument.Parse(masked);
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
#pragma warning disable CA1031 // fail closed: the masker never lets the input through after a failure
        catch (Exception)
#pragma warning restore CA1031
        {
            return new CallToolResult
            {
                IsError = true,
                Content = new List<ContentBlock> { new TextContentBlock { Text = McpDataDetailMasker.Refusal } },
            };
        }
    }

    private static ContentBlock MaskBlock(ContentBlock block, McpDataDetailMasker masker)
    {
        switch (block)
        {
            case TextContentBlock text:
            {
                if (string.IsNullOrEmpty(text.Text)) return block;
                string masked = Checked(masker.Answer(text.Text), text.Text);
                return ReferenceEquals(masked, text.Text)
                    ? block
                    : new TextContentBlock { Text = masked, Annotations = text.Annotations, Meta = text.Meta };
            }
            case EmbeddedResourceBlock { Resource: TextResourceContents resource } embedded:
            {
                if (string.IsNullOrEmpty(resource.Text)) return block;
                string masked = Checked(masker.Answer(resource.Text), resource.Text);
                if (ReferenceEquals(masked, resource.Text)) return block;
                return new EmbeddedResourceBlock
                {
                    Resource = new TextResourceContents
                    {
                        Uri = resource.Uri,
                        MimeType = resource.MimeType,
                        Text = masked,
                        Meta = resource.Meta,
                    },
                    Annotations = embedded.Annotations,
                    Meta = embedded.Meta,
                };
            }
            default:
                throw new InvalidOperationException(McpDataDetailMasker.Refusal);
        }
    }

    /// <summary>A refusal from the masker refuses the whole answer, unless the input was itself the refusal.</summary>
    private static string Checked(string masked, string input) =>
        string.Equals(masked, McpDataDetailMasker.Refusal, StringComparison.Ordinal)
        && !string.Equals(input, McpDataDetailMasker.Refusal, StringComparison.Ordinal)
            ? throw new InvalidOperationException(McpDataDetailMasker.Refusal)
            : masked;

    private static McpException WithMaskedMessage(McpException ex, McpDataDetail mode)
    {
        string masked;
        try
        {
            masked = McpDataDetailMasker.MaskErrorText(ex.Message, mode);
        }
#pragma warning disable CA1031 // fail closed
        catch (Exception)
#pragma warning restore CA1031
        {
            masked = McpDataDetailMasker.Refusal;
        }

        if (ReferenceEquals(masked, ex.Message)) return ex;
        return ex is McpProtocolException protocol
            ? new McpProtocolException(masked, ex.InnerException, protocol.ErrorCode)
            : new McpException(masked, ex.InnerException);
    }
}
