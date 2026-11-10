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
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8534: a Memory Broker grid read shreds only the broker events that can be a SHRINK. A window holds many STABLE and GROW events for each
/// SHRINK, the grid keeps the SHRINK ones, and shredding the XML is most of what the read costs. These hand
/// <see cref="SystemHealthSignificance.SignificantBrokerRecords"/> a window of events and a shred that records what reaches it: the events
/// that cannot be a SHRINK never do, and the rows and their order are those of shredding every event. Lite and the Viewer both keep their
/// grid rows through this function. No database, no WPF.
/// </summary>
public sealed class MemoryBrokerShredTests
{
    /// <summary>A broker event as the ring buffer target writes it, with <paramref name="notificationXml"/> as the inner XML of its notification value.</summary>
    private static string Event(int second, string notificationXml, string broker = "MEMORYBROKER_FOR_CACHE") =>
        $"<event name=\"memory_broker_ring_buffer_recorded\" package=\"sqlos\" timestamp=\"2026-11-10T12:00:{second:00}.000Z\">"
        + $"<data name=\"id\"><type name=\"uint32\" package=\"package0\"/><value>{second}</value></data>"
        + "<data name=\"new_target\"><type name=\"int64\" package=\"package0\"/><value>4000000</value></data>"
        + $"<data name=\"broker\"><type name=\"unicode_string\" package=\"package0\"/><value>{broker}</value></data>"
        + $"<data name=\"notification\"><type name=\"unicode_string\" package=\"package0\"/><value>{notificationXml}</value></data></event>";

    /// <summary>Newest first, as the store hands them over: three SHRINK events among STABLE and GROW ones, a cut-off event and an empty one.</summary>
    private static readonly string[] Window =
    [
        Event(59, "SHRINK"),
        Event(58, "GROW"),
        Event(57, "STABLE"),
        Event(56, "SHRINK", "MEMORYBROKER_FOR_STEAL"),
        Event(55, "STABLE", "MEMORYBROKER_FOR_RESERVE"),
        "<event name=\"memory_broker_ring_buffer_recorded\"><data name=\"notification\"><value>GRO",
        Event(54, "GROW", "MEMORYBROKER_FOR_STEAL"),
        Event(53, "SHRINK", "MEMORYBROKER_FOR_RESERVE"),
        "",
    ];

    /// <summary>What identifies a record in these windows: its time, its broker id, its broker and its notification.</summary>
    private static string Key(MemoryBrokerRecord record) => $"{record.EventTime:HH:mm:ss}|{record.BrokerId}|{record.Broker}|{record.Notification}";

    /// <summary>The rows of shredding every event and then applying the gate.</summary>
    private static List<string> ShredEveryEvent(IEnumerable<string> window) =>
        window.Select(SystemHealthParser.ParseMemoryBroker)
            .Where(record => record is not null && SystemHealthSignificance.IsSignificant(record))
            .Select(record => Key(record!))
            .ToList();

    [Fact]
    public void OnlyTheEventsThatCanBeAShrink_ReachTheShred()
    {
        var shredded = new List<string>();

        var kept = SystemHealthSignificance.SignificantBrokerRecords(Window, xml =>
        {
            shredded.Add(xml);
            return SystemHealthParser.ParseMemoryBroker(xml);
        });

        Assert.Equal(3, kept.Count);
        Assert.True(shredded.SequenceEqual(new[] { Window[0], Window[3], Window[7] }),
            $"{shredded.Count} of the window's {Window.Length} events reached the shred, where only its 3 SHRINK events can be one");
    }

    [Fact]
    public void TheRowsAndTheirOrder_AreThoseOfShreddingEveryEvent()
    {
        var kept = SystemHealthSignificance.SignificantBrokerRecords(Window).Select(Key).ToList();

        Assert.Equal(ShredEveryEvent(Window), kept);
        Assert.Equal(new[] { "12:00:59|59|MEMORYBROKER_FOR_CACHE|SHRINK", "12:00:56|56|MEMORYBROKER_FOR_STEAL|SHRINK", "12:00:53|53|MEMORYBROKER_FOR_RESERVE|SHRINK" }, kept);
    }

    /// <summary>
    /// XML can spell the notification without holding the word: with a character reference, with a CDATA section, or with a comment between
    /// two letters. The shredded value is SHRINK in each, so each is still shredded and kept.
    /// </summary>
    [Theory]
    [InlineData("&#83;HRINK")]
    [InlineData("SHRIN&#x4B;")]
    [InlineData("SHR<![CDATA[INK]]>")]
    [InlineData("<![CDATA[SHRINK]]>")]
    [InlineData("<![CDATA[SHR]]>INK")]
    [InlineData("<![CDATA[S]]>HRIN<![CDATA[K]]>")]
    [InlineData("S<!-- split -->HRINK")]
    [InlineData("SHRIN<!-- split -->K")]
    public void AnEventThatSpellsShrinkAnotherWay_IsStillShreddedAndKept(string notificationXml)
    {
        string[] window = [Event(30, "GROW"), Event(29, notificationXml), Event(28, "STABLE")];

        var kept = SystemHealthSignificance.SignificantBrokerRecords(window).Select(Key).ToList();

        Assert.Equal(ShredEveryEvent(window), kept);
        Assert.Equal(new[] { "12:00:29|29|MEMORYBROKER_FOR_CACHE|SHRINK" }, kept);
    }

    [Fact]
    public void AnEventThatNamesShrinkSomewhereElse_IsShredded_AndLeftOutByTheGate()
    {
        string[] window = [Event(20, "GROW", "MEMORYBROKER_FOR_SHRINK_TEST"), Event(19, "SHRINK")];
        var shredded = new List<string>();

        var kept = SystemHealthSignificance.SignificantBrokerRecords(window, xml =>
        {
            shredded.Add(xml);
            return SystemHealthParser.ParseMemoryBroker(xml);
        }).Select(Key).ToList();

        Assert.Equal(2, shredded.Count);
        Assert.Equal(new[] { "12:00:19|19|MEMORYBROKER_FOR_CACHE|SHRINK" }, kept);
    }
}
