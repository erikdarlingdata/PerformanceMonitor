/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Lite's twins of #4487's job_history fixes, checked without a live DuckDB or a project reference to
/// Lite: a source-text scan of the two runner files, confirming (a) the numeric watermark read is scoped
/// to the newest batch's <c>collection_time</c> rather than a plain unscoped MAX, and (b) the pre-insert
/// natural-key dedupe runs before the appender opens, by textual order in the method body. Runs on macOS.
/// </summary>
public sealed class LiteJobHistorySourceTests
{
    /// <summary>Extracts one method's body by brace-matching from its signature line, so the anchors below
    /// read only that method rather than matching text anywhere else in the file.</summary>
    private static string ExtractMethodBody(string text, string signatureAnchor)
    {
        var signatureIndex = text.IndexOf(signatureAnchor, StringComparison.Ordinal);
        Assert.True(signatureIndex >= 0, $"signature not found, this pin would read nothing: {signatureAnchor}");

        var openBrace = text.IndexOf('{', signatureIndex);
        Assert.True(openBrace >= 0, $"no opening brace found after signature: {signatureAnchor}");

        var depth = 0;
        var i = openBrace;
        for (; i < text.Length; i++)
        {
            if (text[i] == '{')
            {
                depth++;
            }
            else if (text[i] == '}')
            {
                depth--;
                if (depth == 0)
                {
                    break;
                }
            }
        }

        Assert.True(depth == 0, $"unbalanced braces extracting method body: {signatureAnchor}");
        return text[openBrace..(i + 1)];
    }

    [Fact]
    public void GetLastCollectedInstanceIdAsync_ScopesToTheNewestBatch_NotAPlainUnscopedMax()
    {
        var text = ReadRepoFile("Lite", "Services", "RemoteCollectorService.cs");
        var body = ExtractMethodBody(text, "protected async Task<long?> GetLastCollectedInstanceIdAsync(");

        /* #4487: the newest-batch scope. An identity reseed can leave a lower-valued epoch's rows behind a
           higher-valued OLD epoch's max, so a plain MAX(instance_id) would never notice the new epoch's own
           rows arriving — scoping to the newest collection_time fixes that. */
        Assert.Contains("collection_time = (SELECT MAX(collection_time)", body, StringComparison.Ordinal);

        /* The pre-#4487 shape on dev: a plain MAX with no collection_time scope, as the query's WHOLE text.
           Confirming this is ABSENT as the entire command (not merely that the scoped string differs) is
           what actually distinguishes the fix from a cosmetic rewrite of the same query. */
        Assert.DoesNotContain(
            "cmd.CommandText = $\"SELECT MAX({columnName}) FROM {tableName} WHERE server_id = $1\";",
            body,
            StringComparison.Ordinal);
    }

    [Fact]
    public void WriteBatch_CallsDropAlreadyStoredJobHistoryRows_BeforeCreateAppender()
    {
        var text = ReadRepoFile("Lite", "Services", "RemoteCollectorService.DefinitionRunner.cs");
        var body = ExtractMethodBody(text, "private static int WriteBatch<TRow>(");

        var dedupeIndex = body.IndexOf("DropAlreadyStoredJobHistoryRows(", StringComparison.Ordinal);
        var appenderIndex = body.IndexOf("CreateAppender(", StringComparison.Ordinal);

        Assert.True(dedupeIndex >= 0, "WriteBatch no longer calls DropAlreadyStoredJobHistoryRows");
        Assert.True(appenderIndex >= 0, "WriteBatch no longer calls CreateAppender");
        Assert.True(
            dedupeIndex < appenderIndex,
            "the pre-insert dedupe must run BEFORE the appender opens, by textual order in WriteBatch's body");
    }
}
