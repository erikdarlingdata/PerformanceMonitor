/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #2761: the slice-repair survey must not hold ONE read lock across its whole multi-phase read.
///
/// <para><b>Why this is the property worth pinning.</b> <c>RemoteCollectorService</c> takes the DB WRITE lock
/// for every collection write, and <see cref="System.Threading.ReaderWriterLockSlim"/> blocks a writer while
/// any reader holds the lock — so the duration of one read-lock hold is the duration of a fleet-wide
/// collection stall. The survey reads the hot table AND does a <c>read_parquet</c> of every monthly archive
/// file; holding one lock across all of it means minutes with no data arriving anywhere on a store like the
/// one #2748 reported, which is indistinguishable from the app having failed to start.</para>
///
/// <para><b>Why a source scan rather than a timing test.</b> The honest behavioural version — park a writer
/// and assert it gets in before the survey returns — is a race against how long each phase happens to take on
/// the CI machine, and a flaky guard on a locking model gets muted rather than fixed. The structural property
/// is exact and is what the fix actually changed: the archive loop acquires its own lock per file instead of
/// inheriting one taken before the loop.</para>
/// </summary>
public sealed class SliceRepairSurveyLockScopeTests
{
    private static readonly string ServicePath =
        Path.Combine("Lite", "Services", "QueryStoreSliceRepairService.cs");

    [Fact]
    public void SurveyAcquiresTheReadLockPerPhase_NotOnceAroundEverything()
    {
        var body = MethodBody("SurveyAsync");

        /* At minimum: one for the hot table, one inside the archive loop. A single acquisition is exactly
           the defect — that is what "holds it for its full duration" means in source. */
        var acquisitions = Regex.Matches(body, @"AcquireReadLock\(").Count;
        Assert.True(
            acquisitions >= 2,
            $"SurveyAsync should take the read lock per phase, found {acquisitions} acquisition(s). " +
            "One acquisition means the hot GROUP BY and every archive read_parquet share a single hold, " +
            "which starves every collection write for the whole survey (#2761).");

        /* And the second one must be INSIDE the per-file loop, not merely somewhere in the method — two
           acquisitions both before the loop would satisfy a naive count while changing nothing. */
        var loop = body.IndexOf("foreach (var file in ArchiveFiles())", StringComparison.Ordinal);
        Assert.True(loop >= 0, "SurveyAsync no longer has the per-archive-file loop this pin is about.");
        Assert.Contains("AcquireReadLock(", body[loop..], StringComparison.Ordinal);
    }

    [Fact]
    public void SurveyDoesNotKeepAConnectionOpenAcrossALockBoundary()
    {
        var body = MethodBody("SurveyAsync");

        /* A connection outliving its lock is a connection open across a maintenance operation that
           reorganizes the database file — where "Reached the end of the file" comes from. Every connection
           the survey opens must therefore be created inside a lock scope, which is the idiom
           RewriteArchiveFileAsync already uses. Assert there is no connection created before the first
           acquisition. */
        var firstLock = body.IndexOf("AcquireReadLock(", StringComparison.Ordinal);
        var firstConnection = body.IndexOf("CreateConnection()", StringComparison.Ordinal);

        Assert.True(firstLock >= 0 && firstConnection >= 0, "SurveyAsync should both lock and open a connection.");
        Assert.True(
            firstLock < firstConnection,
            "SurveyAsync opens a connection before taking the read lock, so the connection can outlive the " +
            "lock that protects the file it is reading (#2761).");
    }

    [Fact]
    public void RepairOnStartupReportsCancellationInsteadOfLosingItToAnUnobservedTask()
    {
        /* #2761 handed the repair a real token, which made OperationCanceledException reachable for the
           first time. RepairOnStartupAsync is started with a bare Task.Run and never awaited, so an
           uncaught one becomes an unobserved task exception — a repair that stopped with nothing anywhere
           saying so. That is the silent-failure shape this repo keeps re-finding, so it is pinned. */
        var body = MethodBody("RepairOnStartupAsync");

        Assert.Contains("catch (OperationCanceledException)", body, StringComparison.Ordinal);

        var cancelCatch = body.IndexOf("catch (OperationCanceledException)", StringComparison.Ordinal);
        Assert.Contains("_logger?.Log", body[cancelCatch..], StringComparison.Ordinal);
    }

    /// <summary>
    /// Extracts one method body by brace balance, so the assertions cannot leak into a neighbour.
    ///
    /// <para><b>Matches a DECLARATION, not the first textual occurrence.</b> The naive
    /// <c>IndexOf($" {methodName}(")</c> finds <c>var survey = await SurveyAsync(cancellationToken);</c> —
    /// a CALL inside <c>RepairOnStartupAsync</c> — and then scans that method's braces instead. Caught while
    /// proving this pin red-first: it reported the fixed source as still failing, which would have read as
    /// the fix not working rather than the harness looking in the wrong place.</para>
    /// </summary>
    private static string MethodBody(string methodName)
    {
        var source = File.ReadAllText(FindRepoFile(ServicePath));
        var declaration = Regex.Match(
            source,
            @"^[ \t]*(?:public|private|internal|protected)[^\n(]*\b" + Regex.Escape(methodName) + @"\s*\(",
            RegexOptions.Multiline);
        Assert.True(declaration.Success, $"{methodName} declaration not found in {ServicePath}.");

        var open = source.IndexOf('{', declaration.Index + declaration.Length);
        Assert.True(open >= 0, $"{methodName} has no body.");

        var depth = 0;
        for (var i = open; i < source.Length; i++)
        {
            if (source[i] == '{')
            {
                depth++;
            }
            else if (source[i] == '}')
            {
                depth--;
                if (depth == 0)
                {
                    return source[open..i];
                }
            }
        }

        throw new InvalidOperationException($"Unbalanced braces scanning {methodName}.");
    }

    private static string FindRepoFile(string relativePath)
    {
        var dir = AppContext.BaseDirectory;
        for (var i = 0; i < 8 && dir is not null; i++)
        {
            var candidate = Path.Combine(dir, relativePath);
            if (File.Exists(candidate))
            {
                return candidate;
            }

            dir = Path.GetDirectoryName(dir);
        }

        throw new FileNotFoundException($"Could not locate {relativePath} walking up from {AppContext.BaseDirectory}");
    }
}
