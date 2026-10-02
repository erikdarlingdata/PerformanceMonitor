using System;
using System.Runtime.CompilerServices;
using PerformanceMonitor.Darling.Storage;

namespace Darling.Tests;

/// <summary>
/// Pins every live test's store session to UTC, the way the product pins its own store connections
/// (<see cref="DarlingStoreConnection.PinSessionTimeZoneUtc"/>). The store's timestamp columns hold naive UTC, and
/// many reads compare them with the session's clock, so a test store whose server runs in another time zone failed
/// tests that plant rows relative to UTC now, with no product bug behind them. More than four hundred classes read
/// DARLING_TEST_PG directly, so the variable is pinned once, when this assembly loads, before any of them reads it.
/// Pinned by <see cref="LiveStoreSessionTimeZoneTests"/>.
/// </summary>
/* #1776 own-store: this class never connects to any store, shared or its own. It rewrites the variable's text once,
   when the assembly loads and before any live class reads it, so it cannot race the shared store. */
internal static class LiveStoreSessionTimeZone
{
    [ModuleInitializer]
    internal static void PinTestStoreToUtc()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        if (string.IsNullOrEmpty(connectionString))
        {
            return;
        }

        try
        {
            Environment.SetEnvironmentVariable("DARLING_TEST_PG", DarlingStoreConnection.PinSessionTimeZoneUtc(connectionString));
        }
        catch (ArgumentException)
        {
            /* A malformed string stays as it was, so each live test reports it the way it always has, rather than
               this initializer failing every test in the assembly at load. */
        }
    }
}
