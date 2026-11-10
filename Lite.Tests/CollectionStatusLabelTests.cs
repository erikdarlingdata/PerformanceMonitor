/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8533: the status bar's "Collection: Ns ago" is worded to the second, and the collection loop ends a cycle once a minute. A
/// label repainted only by the status bar's 30 second timer lands on the same two points of every cycle, so it shows the same two
/// numbers for as long as the window sits idle, and a repaint that lands inside a cycle leaves "Running" up for 30 seconds after
/// the cycle ended. The label therefore has a tick of its own and repaints when a cycle starts or ends. These drive it with a clock,
/// a tick and a cycle signal the test owns, so no fact waits. The window's own timer and the loop's own signal are driven in
/// CollectionStatusCadenceTests.
/// </summary>
public sealed class CollectionStatusLabelTests
{
    private static readonly DateTime CycleEnd = new(2026, 11, 10, 14, 32, 4, DateTimeKind.Utc);

    [Fact]
    public void TheAge_MovesOnTheLabelsOwnTick_SecondBySecond()
    {
        var bar = new StatusBar { LastCollectionUtc = CycleEnd, Now = CycleEnd.AddSeconds(9) };
        bar.Build();

        Assert.Equal("Collection: 9s ago", bar.Tick());
        bar.Now = CycleEnd.AddSeconds(10);
        Assert.Equal("Collection: 10s ago", bar.Tick());
        bar.Now = CycleEnd.AddSeconds(38);
        Assert.Equal("Collection: 38s ago", bar.Tick());
    }

    [Fact]
    public void TheLabelsTick_IsOneSecond()
    {
        Assert.Equal(TimeSpan.FromSeconds(1), CollectionStatusLabel.AgeTick);
    }

    [Fact]
    public void ACycleStartingAndEnding_RepaintsAtOnce_SoRunningDoesNotOutliveTheCycle()
    {
        var bar = new StatusBar { LastCollectionUtc = CycleEnd, Now = CycleEnd.AddSeconds(56) };
        bar.Build();

        bar.IsCollecting = true;
        Assert.Equal("Collection: Running", bar.CycleChanged());

        bar.IsCollecting = false;
        bar.LastCollectionUtc = CycleEnd.AddSeconds(60);
        bar.Now = CycleEnd.AddSeconds(60);
        Assert.Equal("Collection: 0s ago", bar.CycleChanged());
    }

    [Fact]
    public void ACycleThatFails_LeavesTheAgeCountingFromTheLastOneThatEnded()
    {
        var bar = new StatusBar { LastCollectionUtc = CycleEnd, IsCollecting = true, Now = CycleEnd.AddSeconds(58) };
        bar.Build();
        Assert.Equal("Collection: Running", bar.CycleChanged());

        bar.IsCollecting = false;
        bar.Now = CycleEnd.AddSeconds(61);
        Assert.Equal("Collection: 61s ago", bar.CycleChanged());
    }

    [Fact]
    public void PausedAndNotYetCollected_KeepTheirOwnWords()
    {
        var bar = new StatusBar { Now = CycleEnd };
        bar.Build();
        Assert.Equal("Collection: Starting...", bar.Tick());

        bar.LastCollectionUtc = CycleEnd;
        bar.IsPaused = true;
        bar.Now = CycleEnd.AddSeconds(20);
        Assert.Equal("Collection: Paused", bar.Tick());
    }

    /// <summary>The loop's state, the clock, and the two signals the label listens to, all held by the test.</summary>
    private sealed class StatusBar
    {
        private readonly List<string> _painted = new();
        private Action? _tick;
        private Action? _cycleChanged;

        public bool IsCollecting { get; set; }

        public bool IsPaused { get; set; }

        public DateTime? LastCollectionUtc { get; set; }

        public DateTime Now { get; set; }

        /// <summary>Builds the label over this state: the loop's three facts, the clock, the paint, then the tick and the cycle signal.</summary>
        public void Build()
        {
            _ = new CollectionStatusLabel(
                () => IsCollecting,
                () => IsPaused,
                () => LastCollectionUtc,
                () => Now,
                _painted.Add,
                handler => _tick = handler,
                handler => _cycleChanged = handler);

            Assert.True(_tick is not null, "the label did not subscribe to its tick");
            Assert.True(_cycleChanged is not null, "the label did not subscribe to the cycle signal");
        }

        /// <summary>Raises the label's tick and returns what it painted.</summary>
        public string Tick() => Painted(_tick);

        /// <summary>Raises the cycle signal and returns what the label painted.</summary>
        public string CycleChanged() => Painted(_cycleChanged);

        private string Painted(Action? signal)
        {
            Assert.NotNull(signal);
            var before = _painted.Count;
            signal();
            Assert.True(_painted.Count == before + 1, "the signal painted nothing");
            return _painted[^1];
        }
    }
}
