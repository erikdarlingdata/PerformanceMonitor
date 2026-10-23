/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Security.AccessControl;
using System.Security.Principal;
using Microsoft.Win32;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6473: a Viewer installed in its own folder finds the installed Darling service's darling.json through the
/// service's registered ImagePath. The lookup runs here against a scratch registry key (never the machine's real
/// Services key, which on a developer laptop holds the real service) and temp folders.
/// </summary>
[Collection("darling-config-env")]
public sealed class ViewerInstalledServiceTests
{
    private const string MinimalConfig = """{ "postgres": { "connectionString": "Host=h;Username=u;Password=p;Database=d" } }""";

    /// <summary>A scratch "Services" key under HKCU, with the service's subkey and ImagePath written the way the
    /// Service Control Manager writes them.</summary>
    private sealed class FakeServicesKey : IDisposable
    {
        private readonly string _path = @"Software\DarlingTests6473\" + Guid.NewGuid().ToString("N");

        public FakeServicesKey() => Root = Registry.CurrentUser.CreateSubKey(_path);

        public RegistryKey Root { get; }

        public void RegisterService(string imagePath)
        {
            using var service = Root.CreateSubKey(ViewerInstalledService.ServiceName);
            service.SetValue("ImagePath", imagePath, RegistryValueKind.ExpandString);
        }

        public void Dispose()
        {
            Root.Dispose();
            Registry.CurrentUser.DeleteSubKeyTree(_path, throwOnMissingSubKey: false);
        }
    }

    private static string? ClearConfigEnvironment()
    {
        var saved = Environment.GetEnvironmentVariable("DARLING_CONFIG");
        Environment.SetEnvironmentVariable("DARLING_CONFIG", null);
        return saved;
    }

    [Fact]
    public void ImagePath_QuotedWithSpacesAndArguments_YieldsTheServiceFolder()
    {
        var folder = ViewerInstalledService.ParseImagePathFolder(
            "\"C:\\Program Files\\PerformanceMonitorDarling\\PerformanceMonitor.Darling.Service.exe\" --service");

        Assert.Equal(@"C:\Program Files\PerformanceMonitorDarling", folder);
    }

    [Fact]
    public void ImagePath_UnquotedWithArguments_IsCutAtTheExecutable()
    {
        var folder = ViewerInstalledService.ParseImagePathFolder(@"D:\Darling\PerformanceMonitor.Darling.Service.exe --foo bar");

        Assert.Equal(@"D:\Darling", folder);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void ImagePath_Empty_YieldsNothing(string? imagePath)
    {
        Assert.Null(ViewerInstalledService.ParseImagePathFolder(imagePath));
    }

    [Fact]
    public void FindInstallFolder_ReadsTheRegisteredImagePath()
    {
        using var services = new FakeServicesKey();
        services.RegisterService("\"C:\\Program Files\\PerformanceMonitorDarling\\PerformanceMonitor.Darling.Service.exe\"");

        Assert.Equal(@"C:\Program Files\PerformanceMonitorDarling", ViewerInstalledService.FindInstallFolder(services.Root));
    }

    [Fact]
    public void FindInstallFolder_NoServiceRegistered_YieldsNothing()
    {
        using var services = new FakeServicesKey();
        using (var other = services.Root.CreateSubKey("SomeOtherService"))
        {
            other.SetValue("ImagePath", @"C:\Other\other.exe");
        }

        Assert.Null(ViewerInstalledService.FindInstallFolder(services.Root));
    }

    [Fact]
    public void AViewerInItsOwnFolder_FindsTheInstalledServicesDarlingJson_AndLoadsIt()
    {
        var saved = ClearConfigEnvironment();
        var root = Directory.CreateTempSubdirectory("darling-viewer-svcfind-");
        try
        {
            var serviceFolder = Directory.CreateDirectory(Path.Combine(root.FullName, "service")).FullName;
            var viewerFolder = Directory.CreateDirectory(Path.Combine(root.FullName, "elsewhere", "viewer")).FullName;
            var serviceConfig = Path.Combine(serviceFolder, "darling.json");
            File.WriteAllText(serviceConfig, MinimalConfig);

            using var services = new FakeServicesKey();
            services.RegisterService($"\"{Path.Combine(serviceFolder, "PerformanceMonitor.Darling.Service.exe")}\"");

            var location = ViewerSettings.ResolveConfigLocation(
                baseDirectory: viewerFolder, installedServiceFolder: () => ViewerInstalledService.FindInstallFolder(services.Root));

            Assert.Equal(ViewerConfigSource.InstalledService, location.Source);
            Assert.Equal(serviceConfig, location.Path);
            Assert.True(location.Exists);

            /* The path the Viewer used is the one its diagnostics (and so its connection details) show. */
            var text = string.Join(Environment.NewLine, ViewerConfigDiagnostics.DescribeConfigLocation(location));
            Assert.Contains(serviceConfig, text, StringComparison.Ordinal);
            Assert.Contains("installed Darling service", text, StringComparison.Ordinal);

            /* Loaded the way MainWindow loads it: the resolved path handed to TryLoad. */
            var settings = ViewerSettings.TryLoad(location.Path);
            Assert.NotNull(settings);
        }
        finally
        {
            Environment.SetEnvironmentVariable("DARLING_CONFIG", saved);
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public void TheExistingPlaces_StillOutrankTheInstalledService()
    {
        var saved = ClearConfigEnvironment();
        var root = Directory.CreateTempSubdirectory("darling-viewer-svcorder-");
        try
        {
            var serviceFolder = Directory.CreateDirectory(Path.Combine(root.FullName, "service")).FullName;
            File.WriteAllText(Path.Combine(serviceFolder, "darling.json"), MinimalConfig);
            var viewerFolder = Directory.CreateDirectory(Path.Combine(root.FullName, "app", "viewer")).FullName;
            Func<string?> service = () => serviceFolder;

            /* One folder up from the viewer. */
            var oneUp = Path.Combine(root.FullName, "app", "darling.json");
            File.WriteAllText(oneUp, MinimalConfig);
            var fromParent = ViewerSettings.ResolveConfigLocation(baseDirectory: viewerFolder, installedServiceFolder: service);
            Assert.Equal(ViewerConfigSource.ServiceRoot, fromParent.Source);
            Assert.Equal(oneUp, fromParent.Path);

            /* Beside the viewer. */
            var beside = Path.Combine(viewerFolder, "darling.json");
            File.WriteAllText(beside, MinimalConfig);
            var fromBeside = ViewerSettings.ResolveConfigLocation(baseDirectory: viewerFolder, installedServiceFolder: service);
            Assert.Equal(ViewerConfigSource.BesideViewer, fromBeside.Source);

            /* DARLING_CONFIG. */
            var fromEnvFile = Path.Combine(root.FullName, "env.json");
            File.WriteAllText(fromEnvFile, MinimalConfig);
            Environment.SetEnvironmentVariable("DARLING_CONFIG", fromEnvFile);
            var fromEnv = ViewerSettings.ResolveConfigLocation(baseDirectory: viewerFolder, installedServiceFolder: service);
            Assert.Equal(ViewerConfigSource.EnvironmentVariable, fromEnv.Source);

            /* An explicit path outranks everything. */
            var explicitFile = Path.Combine(root.FullName, "explicit.json");
            var fromArgument = ViewerSettings.ResolveConfigLocation(explicitFile, viewerFolder, service);
            Assert.Equal(ViewerConfigSource.CommandLine, fromArgument.Source);
        }
        finally
        {
            Environment.SetEnvironmentVariable("DARLING_CONFIG", saved);
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public void NoServiceInstalled_OrNoDarlingJsonInItsFolder_FallsBackToTheNotFoundResult()
    {
        var saved = ClearConfigEnvironment();
        var root = Directory.CreateTempSubdirectory("darling-viewer-svcnone-");
        try
        {
            var viewerFolder = Directory.CreateDirectory(Path.Combine(root.FullName, "app", "viewer")).FullName;
            var emptyServiceFolder = Directory.CreateDirectory(Path.Combine(root.FullName, "service")).FullName;

            var noService = ViewerSettings.ResolveConfigLocation(baseDirectory: viewerFolder, installedServiceFolder: () => null);
            Assert.Equal(ViewerConfigSource.BesideViewer, noService.Source);
            Assert.False(noService.Exists);
            Assert.Null(ViewerSettings.TryLoad(noService.Path));

            var emptyFolder = ViewerSettings.ResolveConfigLocation(baseDirectory: viewerFolder, installedServiceFolder: () => emptyServiceFolder);
            Assert.Equal(ViewerConfigSource.BesideViewer, emptyFolder.Source);
            Assert.False(emptyFolder.Exists);
        }
        finally
        {
            Environment.SetEnvironmentVariable("DARLING_CONFIG", saved);
            root.Delete(recursive: true);
        }
    }

    /// <summary>
    /// The installer locks darling.json to the operator group. A Viewer whose account is not in it must be told
    /// that, by group name and with the way to be added, not shown "not found" or a generic error. The account
    /// is refused here with a deny entry on the file, which is the same refusal Windows gives a non-member.
    /// </summary>
    [Fact]
    public void AServiceConfigThisAccountMayNotRead_SaysSoAndNamesTheGroup()
    {
        var saved = ClearConfigEnvironment();
        var root = Directory.CreateTempSubdirectory("darling-viewer-svcdenied-");
        var serviceFolder = Directory.CreateDirectory(Path.Combine(root.FullName, "service")).FullName;
        var viewerFolder = Directory.CreateDirectory(Path.Combine(root.FullName, "app", "viewer")).FullName;
        var serviceConfig = Path.Combine(serviceFolder, "darling.json");
        File.WriteAllText(serviceConfig, MinimalConfig);

        var me = WindowsIdentity.GetCurrent().User!;
        var file = new FileInfo(serviceConfig);
        var deny = new FileSystemAccessRule(
            me,
            FileSystemRights.ReadData | FileSystemRights.ReadAttributes | FileSystemRights.ReadExtendedAttributes,
            AccessControlType.Deny);
        var acl = file.GetAccessControl();
        acl.AddAccessRule(deny);
        file.SetAccessControl(acl);
        try
        {
            var location = ViewerSettings.ResolveConfigLocation(baseDirectory: viewerFolder, installedServiceFolder: () => serviceFolder);

            Assert.Equal(ViewerConfigSource.InstalledService, location.Source);
            Assert.Equal(serviceConfig, location.Path);

            /* The load, as MainWindow runs it, throws the one message that names the group and the way in. */
            var ex = Assert.Throws<UnauthorizedAccessException>(() => ViewerSettings.TryLoad(location.Path));
            Assert.Contains(serviceConfig, ex.Message, StringComparison.Ordinal);
            Assert.Contains(ViewerOperatorGroup.Name, ex.Message, StringComparison.Ordinal);
            Assert.Contains("add your account", ex.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("sign out and back in", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            acl = file.GetAccessControl();
            acl.RemoveAccessRule(deny);
            file.SetAccessControl(acl);
            Environment.SetEnvironmentVariable("DARLING_CONFIG", saved);
            root.Delete(recursive: true);
        }
    }
}
