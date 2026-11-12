/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6968: the floor loop the latest CPU read and the tempdb read share. A fake <c>read</c> stands in for the statement, so
/// these check which statement the loop picks (the bound it hands over), what the floor becomes after each read, and that the
/// loop never runs more than two reads.
/// </summary>
public sealed class NewestRowFloorTests
{
    private const int ServerId = 7;

    private static readonly DateTime Now = new(2026, 10, 30, 12, 0, 0, DateTimeKind.Utc);

    private static readonly DateTime RowTime = new(2026, 10, 25, 3, 4, 5, DateTimeKind.Unspecified);

    /// <summary>A fake statement: records the bound of each call and answers from a script, one answer per call.</summary>
    private sealed class FakeRead
    {
        private readonly Queue<DateTime?> _answers;

        public FakeRead(params DateTime?[] answers) => _answers = new Queue<DateTime?>(answers);

        public List<DateTime?> Bounds { get; } = new();

        public Task<DateTime?> RunAsync(DateTime? since)
        {
            Bounds.Add(since);
            return Task.FromResult(_answers.Dequeue());
        }
    }

    private static Task Run(NewestRowFloor floor, FakeRead read) => floor.ReadAsync(ServerId, read.RunAsync, () => Now);

    [Fact]
    public async Task FirstRead_IsUnbounded()
    {
        var floor = new NewestRowFloor();
        var read = new FakeRead(RowTime);

        await Run(floor, read);

        Assert.Equal(new DateTime?[] { null }, read.Bounds);
    }

    [Fact]
    public async Task FoundRow_SetsTheFloorToItsCollectionTime_AndTheNextReadIsBoundedByIt()
    {
        var floor = new NewestRowFloor();
        var later = RowTime.AddMinutes(1);
        var read = new FakeRead(RowTime, later);

        await Run(floor, read);
        Assert.Equal(RowTime, floor.SinceFor(ServerId));

        await Run(floor, read);
        Assert.Equal(new DateTime?[] { null, RowTime }, read.Bounds);
        Assert.Equal(later, floor.SinceFor(ServerId));
    }

    [Fact]
    public async Task BoundedReadFindingNothing_AfterARowFloor_ReadsOnceMoreUnbounded_AndRestartsTheFloorFromIt()
    {
        var floor = new NewestRowFloor();
        var restarted = RowTime.AddDays(3);
        var read = new FakeRead(RowTime, null, restarted);

        await Run(floor, read);
        await Run(floor, read);

        Assert.Equal(new DateTime?[] { null, RowTime, null }, read.Bounds);
        Assert.Equal(restarted, floor.SinceFor(ServerId));
    }

    [Fact]
    public async Task BoundedReadFindingNothing_AfterARowFloor_RestartsFromNowMinusSlack_WhenTheRereadFindsNothing()
    {
        var floor = new NewestRowFloor();
        var read = new FakeRead(RowTime, null, null);

        await Run(floor, read);
        await Run(floor, read);

        Assert.Equal(new DateTime?[] { null, RowTime, null }, read.Bounds);
        Assert.Equal(Now - NewestRowFloor.EmptyProbeSlack, floor.SinceFor(ServerId));
    }

    [Fact]
    public async Task EmptyUnboundedRead_SetsTheFloorToNowMinusSlack_AndTheNextEmptyBoundedReadDoesNotReread()
    {
        var floor = new NewestRowFloor();
        var read = new FakeRead(null, null);

        await Run(floor, read);
        Assert.Equal(Now - NewestRowFloor.EmptyProbeSlack, floor.SinceFor(ServerId));

        await Run(floor, read);

        Assert.Equal(new DateTime?[] { null, Now - NewestRowFloor.EmptyProbeSlack }, read.Bounds);
        Assert.Equal(Now - NewestRowFloor.EmptyProbeSlack, floor.SinceFor(ServerId));
    }

    [Fact]
    public async Task RowAppearingAfterAnEmptyRead_IsFoundByTheBoundedRead_WithoutARereadAndMovesTheFloorToIt()
    {
        var floor = new NewestRowFloor();
        var read = new FakeRead(null, RowTime);

        await Run(floor, read);
        await Run(floor, read);

        Assert.Equal(2, read.Bounds.Count);
        Assert.Equal(RowTime, floor.SinceFor(ServerId));
    }

    [Fact]
    public async Task Floors_AreKeptPerServer()
    {
        var floor = new NewestRowFloor();
        var read = new FakeRead(RowTime);

        await Run(floor, read);
        var other = new FakeRead((DateTime?)null);
        await floor.ReadAsync(ServerId + 1, other.RunAsync, () => Now);

        Assert.Equal(new DateTime?[] { null }, other.Bounds);
        Assert.Equal(RowTime, floor.SinceFor(ServerId));
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task EveryCallTerminates_WithAtMostTwoReads(bool primeWithARow, bool readsFindNothing)
    {
        var floor = new NewestRowFloor();
        if (primeWithARow)
        {
            await Run(floor, new FakeRead(RowTime));
        }

        // A script of 5 answers, so a loop that read a third time would be seen as extra bounds, not as an empty queue.
        DateTime? answer = readsFindNothing ? null : RowTime.AddHours(1);
        var read = new FakeRead(answer, answer, answer, answer, answer);

        await Run(floor, read);

        Assert.InRange(read.Bounds.Count, 1, 2);
        if (read.Bounds.Count == 2)
        {
            Assert.NotNull(read.Bounds[0]);
            Assert.Null(read.Bounds[1]);
        }
    }
}
