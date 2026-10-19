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
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.RDS;
using Amazon.RDS.Model;
using Amazon.Runtime;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Where a server's own AWS role meets the RDS readers (#5452): the clients every call uses, the host check that runs
/// under the same role, a role edit that reconnects, and the worker arm that records a role the service cannot use. Each
/// test builds the real ingestor or log source over a fake STS and a recording RDS client, so no AWS call is made.
/// </summary>
public sealed class RdsAssumeRoleSeamTests
{
    private const string Role = "arn:aws:iam::123456789012:role/darling-monitor";
    private const string OtherRole = "arn:aws:iam::123456789012:role/darling-other";
    private const string ExternalId = "ext-7Hq2mZ9vLx";
    private const string InstanceHost = "solo.abc123.us-east-1.rds.amazonaws.com";
    private const string ConnectionString = "Host=solo.abc123.us-east-1.rds.amazonaws.com;Username=u;Database=d";
    private const string ProcessAccessKey = "AKIAPROCESSEXAMPLE";
    private const string AssumedAccessKey = "ASIAEXAMPLEASSUMED";

    private static readonly AwsRoleKey KeyA = new(Role, ExternalId);

    private static RdsEndpointVerifier Enforcing()
        => RdsEndpointVerifier.ForTests(enforce: true, loginProbe: (_, _) => Task.FromResult<string?>(null));

    private static NpgsqlDataSource UnusedDataSource() => NpgsqlDataSource.Create("Host=localhost;Port=1;Database=unused");

    private static AwsRoleCredentialCache Cache(FakeSts sts, params string[] allowed)
        => new(AwsRoleAllowlist.From(allowed), () => "install-1", sts.Factory, FakeSts.Source, new ManualTimeProvider(), null, null);

    /// <summary>An RDS client that records the access key each Describe call was signed with.</summary>
    private sealed class RecordingRds : AmazonRDSClient
    {
        private readonly AWSCredentials _credentials;

        public RecordingRds(AWSCredentials credentials) : base(credentials, RegionEndpoint.USEast1) => _credentials = credentials;

        public List<string> InstanceDescribeKeys { get; } = [];

        public List<string> OtherKeys { get; } = [];

        public override async Task<DescribeDBInstancesResponse> DescribeDBInstancesAsync(
            DescribeDBInstancesRequest request, CancellationToken cancellationToken = default)
        {
            InstanceDescribeKeys.Add((await _credentials.GetCredentialsAsync()).AccessKey);

            return new DescribeDBInstancesResponse
            {
                DBInstances = [new DBInstance { Endpoint = new Endpoint { Address = InstanceHost }, DbiResourceId = "db-EXAMPLE" }],
            };
        }

        public override async Task<DescribeDBLogFilesResponse> DescribeDBLogFilesAsync(
            DescribeDBLogFilesRequest request, CancellationToken cancellationToken = default)
        {
            OtherKeys.Add((await _credentials.GetCredentialsAsync()).AccessKey);

            return new DescribeDBLogFilesResponse
            {
                DescribeDBLogFiles = [new DescribeDBLogFilesDetails { LogFileName = "error/postgresql.log.2026-08-25-18", LastWritten = 1000 }],
            };
        }

        public override async Task<DownloadDBLogFilePortionResponse> DownloadDBLogFilePortionAsync(
            DownloadDBLogFilePortionRequest request, CancellationToken cancellationToken = default)
        {
            OtherKeys.Add((await _credentials.GetCredentialsAsync()).AccessKey);

            return new DownloadDBLogFilePortionResponse { LogFileData = string.Empty, Marker = "M" };
        }
    }

    /// <summary>The role-aware factory a test hands the log source: process credentials for no role, the cache's handle for a role.</summary>
    private sealed class Clients
    {
        private readonly AwsRoleCredentialCache _cache;

        public Clients(AwsRoleCredentialCache cache) => _cache = cache;

        public List<RecordingRds> Built { get; } = [];

        public Func<string, AwsRoleKey?, IAmazonRDS> Factory => (region, role) =>
        {
            var client = new RecordingRds(role is null
                ? new BasicAWSCredentials(ProcessAccessKey, "example-process-secret")
                : _cache.For(role.Value, region));
            Built.Add(client);
            return client;
        };

        public IEnumerable<string> InstanceDescribeKeys => Built.SelectMany(c => c.InstanceDescribeKeys);
    }

    private static Task<RdsLogSource.LogChunk?> ReadAsync(RdsLogSource source, AwsRoleKey? role)
        => source.ReadNewestAsync(InstanceHost, RdsLogSource.LogFileKind.Stderr, 7, CancellationToken.None, ConnectionString, role);

    /* ---- the host check runs under the role ----------------------------------------------------------------- */

    [Fact]
    public async Task HostCheck_UnderARole_UsesTheAssumedCredentials_NotTheProcessOnes()
    {
        var sts = new FakeSts();
        var clients = new Clients(Cache(sts, Role));
        var source = new RdsLogSource(roleClientFactory: clients.Factory, verifier: Enforcing());

        await ReadAsync(source, KeyA);

        Assert.Equal([AssumedAccessKey], clients.InstanceDescribeKeys.ToArray());
        Assert.All(clients.Built.SelectMany(c => c.OtherKeys), key => Assert.Equal(AssumedAccessKey, key));
        var request = Assert.Single(sts.Requests);
        Assert.Equal(Role, request.RoleArn);
        Assert.Equal(ExternalId, request.ExternalId);
    }

    [Fact]
    public async Task HostCheck_WithNoRole_KeepsTheProcessCredentials_AndMakesNoStsCall()
    {
        var sts = new FakeSts();
        var clients = new Clients(Cache(sts, Role));
        var source = new RdsLogSource(roleClientFactory: clients.Factory, verifier: Enforcing());

        await ReadAsync(source, role: null);

        Assert.Equal([ProcessAccessKey], clients.InstanceDescribeKeys.ToArray());
        Assert.Equal(0, sts.Calls);
    }

    [Fact]
    public async Task HostCheck_TheVerdictIsKeptPerRole_AndPerExternalIdPresence()
    {
        var sts = new FakeSts();
        var clients = new Clients(Cache(sts, Role, OtherRole));
        var source = new RdsLogSource(roleClientFactory: clients.Factory, verifier: Enforcing());

        await ReadAsync(source, KeyA);
        Assert.Single(clients.InstanceDescribeKeys);

        // Same server, host and role: the held verdict answers, no second Describe.
        await ReadAsync(source, KeyA);
        Assert.Single(clients.InstanceDescribeKeys);

        // A different role is a different scope: the check asks AWS again.
        await ReadAsync(source, new AwsRoleKey(OtherRole, ExternalId));
        Assert.Equal(2, clients.InstanceDescribeKeys.Count());

        // The same role with the external ID removed is another scope too.
        await ReadAsync(source, new AwsRoleKey(Role, null));
        Assert.Equal(3, clients.InstanceDescribeKeys.Count());

        // And back to no role at all.
        await ReadAsync(source, role: null);
        Assert.Equal(4, clients.InstanceDescribeKeys.Count());
    }

    [Fact]
    public void CredentialScope_NamesTheRoleAndWhetherAnIdIsSet_NeverTheId()
    {
        Assert.Equal(Role + "|ext=set", KeyA.CredentialScope);
        Assert.Equal(Role + "|ext=none", new AwsRoleKey(Role, null).CredentialScope);
        Assert.Equal(Role + "|ext=none", new AwsRoleKey(Role, string.Empty).CredentialScope);
        Assert.Equal(string.Empty, AwsRoleKey.ScopeOf(null));
        Assert.DoesNotContain(ExternalId, KeyA.CredentialScope, StringComparison.Ordinal);
    }

    /* ---- a role edit reconnects ----------------------------------------------------------------------------- */

    private static MonitoredServer Server(string? role = null, string? externalId = null) => new()
    {
        Name = "alpha-pg-01",
        Host = InstanceHost,
        Engine = "postgresql",
        AwsRoleArn = role,
        AwsExternalId = externalId,
    };

    [Fact]
    public void ServerDefinitionEquals_ARoleOnlyEdit_IsADifferentDefinition()
    {
        Assert.True(DarlingWorker.ServerDefinitionEquals(Server(Role, ExternalId), Server(Role, ExternalId)));
        Assert.False(DarlingWorker.ServerDefinitionEquals(Server(), Server(Role)));
        Assert.False(DarlingWorker.ServerDefinitionEquals(Server(Role), Server()));
        Assert.False(DarlingWorker.ServerDefinitionEquals(Server(Role, ExternalId), Server(OtherRole, ExternalId)));
        Assert.False(DarlingWorker.ServerDefinitionEquals(Server(Role, ExternalId), Server(Role, "ext-another-id")));
        Assert.False(DarlingWorker.ServerDefinitionEquals(Server(Role, ExternalId), Server(Role)));
    }

    [Fact]
    public void ServerDefinitionEquals_BlankAndNullRoleAreTheSame_AndAnIdWithNoRoleIsIgnored()
    {
        Assert.True(DarlingWorker.ServerDefinitionEquals(Server(null), Server("  ")));
        Assert.True(DarlingWorker.ServerDefinitionEquals(Server(null, ExternalId), Server()));
    }

    /* ---- a role the service cannot use never reaches AWS ---------------------------------------------------- */

    private static async Task<AwsRoleAssumeException> NotAllowedFrom(string reader, AwsRoleCredentialCache cache)
    {
        var verifier = Enforcing();
        Task call = reader switch
        {
            "cpu" => new RdsCpuIngestor(UnusedDataSource(), verifier: verifier, roles: cache)
                .IngestAsync(1, "s", InstanceHost, ConnectionString, KeyA, CancellationToken.None),
            "plan" => new RdsPlanIngestor(UnusedDataSource(), verifier: verifier, roles: cache)
                .IngestAsync(1, "s", InstanceHost, false, ConnectionString, KeyA, CancellationToken.None),
            "deadlock" => new RdsDeadlockIngestor(UnusedDataSource(), verifier: verifier, roles: cache)
                .IngestAsync(1, "s", InstanceHost, false, false, ConnectionString, KeyA, CancellationToken.None),
            "events" => new RdsLogEventIngestor(UnusedDataSource(), TestLogHashKeys.Fixed, verifier: verifier, roles: cache)
                .IngestAsync(1, "s", InstanceHost, false, false, ConnectionString, KeyA, CancellationToken.None),
            _ => throw new ArgumentOutOfRangeException(nameof(reader)),
        };

        var thrown = await Record.ExceptionAsync(() => call);

        // The exact type: wrapped in an "unavailable" exception it would be read by its text, not by its kind.
        return Assert.IsType<AwsRoleAssumeException>(thrown);
    }

    [Theory]
    [InlineData("cpu")]
    [InlineData("plan")]
    [InlineData("deadlock")]
    [InlineData("events")]
    public async Task EveryReader_ARoleTheListDoesNotAllow_ThrowsTheRoleExceptionItself_AndMakesNoStsCall(string reader)
    {
        var sts = new FakeSts();

        var ex = await NotAllowedFrom(reader, Cache(sts));

        Assert.Equal(AwsRoleAssumeKind.NotAllowed, ex.Kind);
        Assert.Equal(0, sts.Calls);
        Assert.NotNull(DarlingWorker.AwsRoleConfigurationFault(ex));
        Assert.DoesNotContain(ExternalId, ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("cpu")]
    [InlineData("plan")]
    [InlineData("deadlock")]
    [InlineData("events")]
    public async Task EveryReader_AnStsDenial_ReachesTheWorkerAsAPermissionsFault_NotAsAnIamGrantGap(string reader)
    {
        var sts = new FakeSts((_, _) => throw FakeSts.Error(
            "AccessDenied",
            "User: arn:aws:sts::123456789012:assumed-role/monitoring-host/i-0abc is not authorized to perform: sts:AssumeRole on resource: " + Role));
        var verifier = Enforcing();
        var cache = Cache(sts, Role);

        // A real AmazonRDSClient over the cache's credentials: the denial surfaces from the first signed request.
        Task call = reader switch
        {
            "cpu" => new RdsCpuIngestor(UnusedDataSource(), verifier: verifier, roles: cache)
                .IngestAsync(1, "s", InstanceHost, ConnectionString, KeyA, CancellationToken.None),
            "plan" => new RdsPlanIngestor(UnusedDataSource(), verifier: verifier, roles: cache)
                .IngestAsync(1, "s", InstanceHost, false, ConnectionString, KeyA, CancellationToken.None),
            "deadlock" => new RdsDeadlockIngestor(UnusedDataSource(), verifier: verifier, roles: cache)
                .IngestAsync(1, "s", InstanceHost, false, false, ConnectionString, KeyA, CancellationToken.None),
            _ => new RdsLogEventIngestor(UnusedDataSource(), TestLogHashKeys.Fixed, verifier: verifier, roles: cache)
                .IngestAsync(1, "s", InstanceHost, false, false, ConnectionString, KeyA, CancellationToken.None),
        };

        var ex = Assert.IsType<AwsRoleAssumeException>(await Record.ExceptionAsync(() => call));

        Assert.Equal(AwsRoleAssumeKind.Denied, ex.Kind);
        Assert.Same(ex, DarlingWorker.AwsRoleConfigurationFault(ex));

        // The role arm sees through a wrapper that the arms after it would claim as a missing grant on the monitoring host's
        // own IAM role, so the order of the arms is what keeps this row naming the role.
        Assert.Same(ex, DarlingWorker.AwsRoleConfigurationFault(new RdsLogUnavailableException("wrapped", true, ex)));
        Assert.Same(ex, DarlingWorker.AwsRoleConfigurationFault(new PiMetricsUnavailableException("wrapped", true, ex)));

        var logger = new CapturingAwsLogger();
        var note = DarlingWorker.AwsRoleFaultNote(logger, "alpha-pg-01", "pg_plan_capture", ex);

        Assert.Equal(ex.Message, note);
        Assert.Contains(Role, note, StringComparison.Ordinal);
        Assert.Contains("with the external ID set on this server", note, StringComparison.Ordinal);
        Assert.DoesNotContain("lacks a grant", note, StringComparison.Ordinal);
        Assert.Contains("PERMISSIONS", logger.All, StringComparison.Ordinal);
        Assert.Contains(Role, logger.All, StringComparison.Ordinal);
        Assert.DoesNotContain(ExternalId, note + logger.All, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AnStsFailureThatIsNotAConfiguration_IsNotClaimedByTheRoleArm_ButStillSurfacesUnwrapped()
    {
        var sts = new FakeSts((_, _) => throw FakeSts.Error("ExpiredToken", "The security token included in the request is expired"));
        var ingestor = new RdsCpuIngestor(UnusedDataSource(), verifier: Enforcing(), roles: Cache(sts, Role));

        var ex = Assert.IsType<AwsRoleAssumeException>(await Record.ExceptionAsync(
            () => ingestor.IngestAsync(1, "s", InstanceHost, ConnectionString, KeyA, CancellationToken.None)));

        Assert.Equal(AwsRoleAssumeKind.SourceCredentials, ex.Kind);
        Assert.False(ex.IsConfiguration);
        Assert.Null(DarlingWorker.AwsRoleConfigurationFault(ex));
        Assert.Null(DarlingWorker.AwsRoleConfigurationFault(new InvalidOperationException("unrelated")));
    }

    /* ---- the public factory parameters --------------------------------------------------------------------- */

    [Fact]
    public async Task APublicFactory_ServesAServerWithNoRole_AndRefusesAServerWithOne_WithoutCallingIt()
    {
        var called = 0;
        var source = new RdsLogSource(_ =>
        {
            called++;
            return new RecordingRds(new BasicAWSCredentials(ProcessAccessKey, "example-process-secret"));
        }, verifier: Enforcing());

        await Assert.ThrowsAsync<InvalidOperationException>(() => ReadAsync(source, KeyA));
        Assert.Equal(0, called);

        await ReadAsync(source, role: null);
        Assert.Equal(1, called);
    }

    [Fact]
    public async Task ADefaultFactoryWithNoCache_RefusesARole_Too()
    {
        var ingestor = new RdsCpuIngestor(UnusedDataSource(), verifier: Enforcing());

        var ex = await Record.ExceptionAsync(() => ingestor.IngestAsync(1, "s", InstanceHost, ConnectionString, KeyA, CancellationToken.None));

        var wrapped = Assert.IsType<PiMetricsUnavailableException>(ex);
        Assert.IsType<InvalidOperationException>(wrapped.InnerException);
    }

    /* ---- the wiring, pinned in the source ------------------------------------------------------------------- */

    [Fact]
    public void TheWorkersRoleArm_ComesBeforeEveryArmThatReadsTheTextOfAnAwsFailure()
    {
        var worker = RepoFile.ReadRepoFileLf("Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs");

        var roleArm = worker.IndexOf("catch (Exception ex) when (AwsRoleConfigurationFault(ex) is { } assume)", StringComparison.Ordinal);
        var mismatch = worker.IndexOf("catch (RdsEndpointMismatchException ex)", StringComparison.Ordinal);
        var logArm = worker.IndexOf("catch (RdsLogUnavailableException ex) when (ex.IsAuthorizationFailure)", StringComparison.Ordinal);
        var piArm = worker.IndexOf("catch (PiMetricsUnavailableException ex) when (ex.IsAuthorizationFailure)", StringComparison.Ordinal);
        var general = worker.IndexOf("        catch (Exception ex)\n        {\n            /* #3095", StringComparison.Ordinal);

        Assert.True(roleArm > 0, "the role arm is missing");
        Assert.True(roleArm < mismatch, "the role arm must come before the endpoint arm");
        Assert.True(roleArm < logArm, "the role arm must come before the RDS log arm");
        Assert.True(roleArm < piArm, "the role arm must come before the Performance Insights arm");
        Assert.True(general < 0 || roleArm < general, "the role arm must come before the general arm");

        var armBody = worker.Substring(roleArm, mismatch - roleArm);
        Assert.Contains("\"PERMISSIONS\"", armBody, StringComparison.Ordinal);
        Assert.Contains("AwsRoleFaultNote(", armBody, StringComparison.Ordinal);
    }

    [Fact]
    public void TheRunner_HandsTheRoleCacheToEveryReader_AndEveryCallPassesTheServersRole()
    {
        var runner = RepoFile.ReadRepoFileLf("Darling/PerformanceMonitor.Darling.Service/DarlingCollectorRunner.cs");

        Assert.Equal(4, CountOf(runner, "roles: _awsRoles));"));
        Assert.Equal(4, CountOf(runner, "server.Config.AwsRoleKey, cancellationToken);"));
        Assert.Contains("RetainAwsRoles", RepoFile.ReadRepoFileLf("Darling/PerformanceMonitor.Darling.Service/DarlingWorker.cs"), StringComparison.Ordinal);
    }

    private static int CountOf(string text, string needle)
    {
        var count = 0;
        for (var at = text.IndexOf(needle, StringComparison.Ordinal); at >= 0; at = text.IndexOf(needle, at + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }
}
