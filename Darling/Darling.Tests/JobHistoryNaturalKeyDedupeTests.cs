/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pure pins for job_history's natural-key pre-insert dedupe (#4487): <see cref="JobHistoryCollector"/>
/// implements <see cref="INaturalKeyDedupedCollector{TRow}"/>, its natural key is the four columns
/// <c>instance_id</c>, <c>job_id</c>, <c>step_id</c>, <c>run_datetime</c> (excluding <c>server_id</c>,
/// which the host's stored-key read already scopes to), and <see cref="JobHistoryCollector.DropAlreadyStored"/>
/// drops exact tuples only — no partial match on fewer than all four columns drops a row.
///
/// <para>RED on dev: <c>INaturalKeyDedupedCollector</c> does not exist there and <c>JobHistoryCollector</c>
/// declares no <c>NaturalKeyColumns</c>/<c>GetNaturalKey</c>/<c>DropAlreadyStored</c> members — a compile
/// failure against <c>origin/dev</c>, not a runtime mismatch.</para>
/// </summary>
public sealed class JobHistoryNaturalKeyDedupeTests
{
    private static JobHistoryCollector.Row MakeRow(long instanceId, string jobId, int stepId, DateTime runDateTime) =>
        new() { InstanceId = instanceId, JobId = jobId, StepId = stepId, RunDateTime = runDateTime, JobName = "j1", RunStatus = 1 };

    [Fact]
    public void JobHistoryCollector_ImplementsTheNaturalKeyDedupeContract()
    {
        Assert.IsAssignableFrom<INaturalKeyDedupedCollector<JobHistoryCollector.Row>>(JobHistoryCollector.Instance);
    }

    [Fact]
    public void NaturalKeyColumns_IsTheFourColumns_ExcludingServerId()
    {
        Assert.Equal(
            new[] { "instance_id", "job_id", "step_id", "run_datetime" },
            JobHistoryCollector.Instance.NaturalKeyColumns);
    }

    [Fact]
    public void GetNaturalKey_ReturnsTheFourFields_InTupleOrder()
    {
        var runDateTime = new DateTime(2026, 9, 20, 10, 0, 0, DateTimeKind.Unspecified);
        var row = MakeRow(101, "job-a", 3, runDateTime);

        var key = JobHistoryCollector.Instance.GetNaturalKey(row);

        Assert.Equal(101L, key.InstanceId);
        Assert.Equal("job-a", key.JobId);
        Assert.Equal(3, key.StepId);
        Assert.Equal(runDateTime, key.RunDateTime);
    }

    [Fact]
    public void DropAlreadyStored_WithAnEmptyStoredSet_ReturnsEveryRow()
    {
        var runDateTime = new DateTime(2026, 9, 20, 10, 0, 0, DateTimeKind.Unspecified);
        var rows = new List<JobHistoryCollector.Row>
        {
            MakeRow(1, "job-a", 0, runDateTime),
            MakeRow(2, "job-b", 1, runDateTime),
        };
        var storedKeys = new HashSet<(long InstanceId, string JobId, int StepId, DateTime RunDateTime)>();

        var kept = JobHistoryCollector.Instance.DropAlreadyStored(rows, storedKeys);

        Assert.Same(rows, kept);
        Assert.Equal(2, kept.Count);
    }

    [Fact]
    public void DropAlreadyStored_DropsOnlyTheExactTuple_KeepingASameKeyRowWithADifferentJobId()
    {
        var runDateTime = new DateTime(2026, 9, 20, 10, 0, 0, DateTimeKind.Unspecified);
        var storedRow = MakeRow(500, "job-a", 0, runDateTime);
        var sameEverythingElseDifferentJobId = MakeRow(500, "job-b", 0, runDateTime);
        var rows = new List<JobHistoryCollector.Row> { storedRow, sameEverythingElseDifferentJobId };

        var storedKeys = new HashSet<(long InstanceId, string JobId, int StepId, DateTime RunDateTime)>
        {
            (500, "job-a", 0, runDateTime),
        };

        var kept = JobHistoryCollector.Instance.DropAlreadyStored(rows, storedKeys);

        Assert.Single(kept);
        Assert.Same(sameEverythingElseDifferentJobId, kept[0]);
    }

    [Fact]
    public void DropAlreadyStored_DropsOnlyTheExactTuple_KeepingASameKeyRowWithADifferentStepId()
    {
        var runDateTime = new DateTime(2026, 9, 20, 10, 0, 0, DateTimeKind.Unspecified);
        var storedRow = MakeRow(500, "job-a", 0, runDateTime);
        var sameEverythingElseDifferentStepId = MakeRow(500, "job-a", 1, runDateTime);
        var rows = new List<JobHistoryCollector.Row> { storedRow, sameEverythingElseDifferentStepId };

        var storedKeys = new HashSet<(long InstanceId, string JobId, int StepId, DateTime RunDateTime)>
        {
            (500, "job-a", 0, runDateTime),
        };

        var kept = JobHistoryCollector.Instance.DropAlreadyStored(rows, storedKeys);

        Assert.Single(kept);
        Assert.Same(sameEverythingElseDifferentStepId, kept[0]);
    }

    [Fact]
    public void DropAlreadyStored_DropsAnExactTuple_AndKeepsARowNotInTheStoredSet()
    {
        var runDateTime = new DateTime(2026, 9, 20, 10, 0, 0, DateTimeKind.Unspecified);
        var duplicate = MakeRow(500, "job-a", 0, runDateTime);
        var newRow = MakeRow(600, "job-c", 0, runDateTime);
        var rows = new List<JobHistoryCollector.Row> { duplicate, newRow };

        var storedKeys = new HashSet<(long InstanceId, string JobId, int StepId, DateTime RunDateTime)>
        {
            (500, "job-a", 0, runDateTime),
        };

        var kept = JobHistoryCollector.Instance.DropAlreadyStored(rows, storedKeys);

        Assert.Single(kept);
        Assert.Same(newRow, kept[0]);
    }

    [Fact]
    public void DropAlreadyStored_PreservesRelativeOrder_AndMutatesNeitherArgument()
    {
        var runDateTime = new DateTime(2026, 9, 20, 10, 0, 0, DateTimeKind.Unspecified);
        var kept1 = MakeRow(1, "job-a", 0, runDateTime);
        var dropped = MakeRow(2, "job-b", 0, runDateTime);
        var kept2 = MakeRow(3, "job-c", 0, runDateTime);
        var rows = new List<JobHistoryCollector.Row> { kept1, dropped, kept2 };
        var rowsSnapshot = rows.ToList();

        var storedKeys = new HashSet<(long InstanceId, string JobId, int StepId, DateTime RunDateTime)>
        {
            (2, "job-b", 0, runDateTime),
        };

        var kept = JobHistoryCollector.Instance.DropAlreadyStored(rows, storedKeys);

        Assert.Equal(new[] { kept1, kept2 }, kept);
        Assert.Equal(rowsSnapshot, rows);
    }
}
