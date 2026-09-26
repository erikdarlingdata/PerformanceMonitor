/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <see cref="ManagedConfFileSettings.Compare"/> (#4336): pins for the design's compare rules, plus the
/// applied-row shape the compare walks.
/// </summary>
public sealed class ManagedConfFileSettingsTests
{
    private static FileSettingRow Applied(string name, string setting, string file = "postgresql.conf", int line = 1)
        => new(SourceFile: file, SourceLine: line, Name: name, Setting: setting, Applied: true, Error: null);

    private static FileSettingRow ErrorRow(string name, string setting, string error, string file = "postgresql.conf", int line = 1)
        => new(SourceFile: file, SourceLine: line, Name: name, Setting: setting, Applied: false, Error: error);

    [Fact]
    public void Equal_Matches()
    {
        var before = new[] { Applied("shared_buffers", "2048MB"), Applied("work_mem", "16MB") };
        var after = new[] { Applied("shared_buffers", "2048MB"), Applied("work_mem", "16MB") };

        var (match, mismatches) = ManagedConfFileSettings.Compare(before, after);

        Assert.True(match);
        Assert.Empty(mismatches);
    }

    [Fact]
    public void ValueChanged_IsAMismatch()
    {
        var before = new[] { Applied("work_mem", "16MB") };
        var after = new[] { Applied("work_mem", "32MB") };

        var (match, mismatches) = ManagedConfFileSettings.Compare(before, after);

        Assert.False(match);
        Assert.Contains("work_mem", mismatches);
    }

    [Fact]
    public void NameAdded_IsAMismatch()
    {
        var before = new[] { Applied("work_mem", "16MB") };
        var after = new[] { Applied("work_mem", "16MB"), Applied("max_connections", "100") };

        var (match, mismatches) = ManagedConfFileSettings.Compare(before, after);

        Assert.False(match);
        Assert.Contains("max_connections", mismatches);
    }

    [Fact]
    public void NameRemoved_IsAMismatch()
    {
        var before = new[] { Applied("work_mem", "16MB"), Applied("max_connections", "100") };
        var after = new[] { Applied("work_mem", "16MB") };

        var (match, mismatches) = ManagedConfFileSettings.Compare(before, after);

        Assert.False(match);
        Assert.Contains("max_connections", mismatches);
    }

    [Fact]
    public void ErrorRowPresentInBoth_DoesNotBlock()
    {
        var before = new[] { Applied("work_mem", "16MB"), ErrorRow("bad_setting", "nope", "invalid value") };
        var after = new[] { Applied("work_mem", "16MB"), ErrorRow("bad_setting", "nope", "invalid value") };

        var (match, mismatches) = ManagedConfFileSettings.Compare(before, after);

        Assert.True(match);
        Assert.Empty(mismatches);
    }

    [Fact]
    public void NewErrorRow_IsAMismatch()
    {
        var before = new[] { Applied("work_mem", "16MB") };
        var after = new[] { Applied("work_mem", "16MB"), ErrorRow("bad_setting", "nope", "invalid value") };

        var (match, mismatches) = ManagedConfFileSettings.Compare(before, after);

        Assert.False(match);
        Assert.Contains("bad_setting", mismatches);
    }

    [Fact]
    public void SourcelineMoved_DoesNotMismatch()
    {
        var before = new[] { Applied("work_mem", "16MB", file: "postgresql.conf", line: 40) };
        var after = new[] { Applied("work_mem", "16MB", file: "darling-managed.conf", line: 12) };

        var (match, mismatches) = ManagedConfFileSettings.Compare(before, after);

        Assert.True(match);
        Assert.Empty(mismatches);
    }
}
