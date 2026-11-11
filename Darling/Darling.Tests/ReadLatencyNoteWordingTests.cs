/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>#8571 item 4: the note every get_read_latency reply carries read "never an interpolation :" and "the true duration only proven to exceed".</summary>
public sealed class ReadLatencyNoteWordingTests
{
    [Fact]
    public void TheBucketEstimateNote_HasNoSpaceBeforeItsColon_AndNoMissingVerb()
    {
        var note = DarlingMcpReadLatencyTools.BucketEstimateNote;

        Assert.DoesNotContain(" :", note, StringComparison.Ordinal);
        Assert.Contains("never an interpolation: the true duration", note, StringComparison.Ordinal);
        Assert.Contains("means the true duration is only proven to exceed the last finite bucket bound", note, StringComparison.Ordinal);
    }
}
