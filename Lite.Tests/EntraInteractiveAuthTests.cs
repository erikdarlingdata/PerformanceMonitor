/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #2184: interactive Entra auth needs a parent window handle because SqlClient routes it through the
/// WAM broker. These pin the wiring contracts that are verifiable without a tenant — argument
/// validation and the idempotent process-wide registration. Whether the picker actually authenticates
/// is a real-tenant step: the Studio twin of this seam (PerformanceStudio#426) was verified by this
/// issue's reporter against his Entra-MFA-on-Azure-VM environment.
/// </summary>
public class EntraInteractiveAuthTests
{
    [Fact]
    public void Register_RejectsANullHandleProvider()
    {
        /* The whole point of the type is supplying a handle; accepting null would register a provider
           that fails at prompt time instead of at wiring time, which is the harder bug to find. */
        Assert.Throws<ArgumentNullException>(() => EntraInteractiveAuth.Register(null!));
    }

    [Fact]
    public void Register_IsProcessWideAndIdempotent()
    {
        /* SqlAuthenticationProvider.SetProvider is process-wide and a second registration would
           silently replace the first, so "first one wins, later ones are no-ops" is the contract worth
           pinning — the app registers at startup and nothing else should be able to swap the provider
           out from under it. The reset makes registration order observable regardless of what ran
           earlier in the test process. */
        EntraInteractiveAuth.ResetRegistrationForTests();

        var first = EntraInteractiveAuth.Register(() => IntPtr.Zero);
        var second = EntraInteractiveAuth.Register(() => new IntPtr(1234));

        Assert.True(first, "the first registration after reset must install the provider");
        Assert.False(second, "a second Register must be a no-op rather than replacing the provider");
    }
}
