/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using Microsoft.Win32;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8430: Settings &gt; Apps shows a Setup.exe install's full version, so one nightly reads differently from the
/// next and from the release. Each test runs against a scratch "Uninstall" key under HKCU (never the real one,
/// which on a developer machine holds the real installs), with the entry written the way the Setup.exe writes it.
/// </summary>
public sealed class InstalledAppsVersionTests
{
    private const string ViewerPackageId = "PerformanceMonitorDarlingViewer";
    private const string ReleaseVersion = "3.12.0";
    private const string NightlyVersion = "3.12.0-nightly.20261109.659";

    /// <summary>A scratch "Uninstall" key under HKCU.</summary>
    private sealed class FakeUninstallKey : IDisposable
    {
        private readonly string _path = @"Software\DarlingTests8430\" + Guid.NewGuid().ToString("N");

        public FakeUninstallKey() => Root = Registry.CurrentUser.CreateSubKey(_path);

        public RegistryKey Root { get; }

        /// <summary>The entry a Setup.exe install leaves: the name, the publisher, and major.minor.patch as the
        /// version, whatever build was installed.</summary>
        public void InstallWithSetup(string packageId, string shortVersion)
        {
            using var entry = Root.CreateSubKey(packageId);
            entry.SetValue("DisplayName", "Performance Monitor Darling Viewer", RegistryValueKind.String);
            entry.SetValue("Publisher", "Darling Data, LLC", RegistryValueKind.String);
            entry.SetValue("DisplayVersion", shortVersion, RegistryValueKind.String);
        }

        public string? Shown(string packageId, string value = "DisplayVersion")
        {
            using var entry = Root.OpenSubKey(packageId);
            return entry?.GetValue(value) as string;
        }

        public void Dispose()
        {
            Root.Dispose();
            Registry.CurrentUser.DeleteSubKeyTree(_path, throwOnMissingSubKey: false);
        }
    }

    [Fact]
    public void ANightlyInstall_ShowsItsFullVersion()
    {
        using var uninstall = new FakeUninstallKey();
        uninstall.InstallWithSetup(ViewerPackageId, ReleaseVersion);

        var wrote = InstalledAppsVersion.Stamp(uninstall.Root, ViewerPackageId, NightlyVersion, isPortable: false);

        Assert.Equal(NightlyVersion, uninstall.Shown(ViewerPackageId));
        Assert.True(wrote);
        Assert.Equal("Performance Monitor Darling Viewer", uninstall.Shown(ViewerPackageId, "DisplayName"));
        Assert.Equal("Darling Data, LLC", uninstall.Shown(ViewerPackageId, "Publisher"));
    }

    [Fact]
    public void AnUpdateToTheNextNightly_ShowsTheNextNightly()
    {
        const string nextNightly = "3.12.0-nightly.20261110.661";
        using var uninstall = new FakeUninstallKey();
        uninstall.InstallWithSetup(ViewerPackageId, ReleaseVersion);
        InstalledAppsVersion.Stamp(uninstall.Root, ViewerPackageId, NightlyVersion, isPortable: false);

        // The updater rewrites the entry with major.minor.patch when it applies the next package.
        uninstall.InstallWithSetup(ViewerPackageId, ReleaseVersion);
        var wrote = InstalledAppsVersion.Stamp(uninstall.Root, ViewerPackageId, nextNightly, isPortable: false);

        Assert.Equal(nextNightly, uninstall.Shown(ViewerPackageId));
        Assert.True(wrote);
    }

    [Fact]
    public void AReleaseInstall_KeepsItsVersionAndIsNotRewritten()
    {
        using var uninstall = new FakeUninstallKey();
        uninstall.InstallWithSetup(ViewerPackageId, ReleaseVersion);

        var wrote = InstalledAppsVersion.Stamp(uninstall.Root, ViewerPackageId, ReleaseVersion, isPortable: false);

        Assert.Equal(ReleaseVersion, uninstall.Shown(ViewerPackageId));
        Assert.False(wrote);
    }

    [Fact]
    public void ACopyThatWasNotInstalled_CreatesNoEntry()
    {
        using var uninstall = new FakeUninstallKey();

        // The zip and the viewer folder beside the service carry no package id and no installed version.
        Assert.False(InstalledAppsVersion.Stamp(uninstall.Root, packageId: null, installedVersion: null, isPortable: false));
        // A package id with no entry: the install was removed, or never made for this user.
        Assert.False(InstalledAppsVersion.Stamp(uninstall.Root, ViewerPackageId, NightlyVersion, isPortable: false));

        Assert.Empty(uninstall.Root.GetSubKeyNames());
    }

    [Fact]
    public void APortableCopy_LeavesTheInstalledCopysEntryAlone()
    {
        using var uninstall = new FakeUninstallKey();
        uninstall.InstallWithSetup(ViewerPackageId, ReleaseVersion);

        var wrote = InstalledAppsVersion.Stamp(uninstall.Root, ViewerPackageId, NightlyVersion, isPortable: true);

        Assert.Equal(ReleaseVersion, uninstall.Shown(ViewerPackageId));
        Assert.False(wrote);
    }
}
