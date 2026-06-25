/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitorDashboard.Services.Remediation
{
    /// <summary>
    /// The production <see cref="IRemediationExecutor"/>: forces/unforces and
    /// preflights through the per-server <see cref="DatabaseService"/> (retargeted
    /// user-DB connection), and audits through a <see cref="RemediationAuditWriter"/>
    /// over the same per-server monitoring connection. Holds no credential of its
    /// own — it reuses the existing monitoring connection string.
    /// </summary>
    public sealed class DatabaseServiceRemediationExecutor : IRemediationExecutor
    {
        private readonly DatabaseService _databaseService;
        private readonly RemediationAuditWriter _auditWriter;

        public DatabaseServiceRemediationExecutor(DatabaseService databaseService)
        {
            _databaseService = databaseService ?? throw new ArgumentNullException(nameof(databaseService));
            _auditWriter = new RemediationAuditWriter(databaseService.ConnectionString);
        }

        public Task<TargetPreflight> PreflightForcePlanAsync(string database, long queryId, long planId, CancellationToken ct)
            => _databaseService.PreflightForcePlanAsync(database, queryId, planId, ct);

        public Task<bool> AuditTableExistsAsync(CancellationToken ct)
            => _auditWriter.TableExistsAsync(ct);

        public Task<bool> HasPriorForceAsync(string database, long queryId, long planId, CancellationToken ct)
            => _auditWriter.HasPriorForceAsync(database, queryId, planId, ct);

        public Task<ForcePlanOutcome> ForcePlanAsync(string database, long queryId, long planId, RemediationIdentity identity, CancellationToken ct)
            => _databaseService.ForcePlanAsync(database, queryId, planId, identity, ct);

        public Task<ForcePlanOutcome> UnforcePlanAsync(string database, long queryId, long planId, RemediationIdentity identity, CancellationToken ct)
            => _databaseService.UnforcePlanAsync(database, queryId, planId, identity, ct);

        public Task<DbConfigPreflight> PreflightDbConfigAsync(string database, DbConfigSetting setting, CancellationToken ct)
            => _databaseService.PreflightDbConfigAsync(database, setting, ct);

        public Task<DbConfigOutcome> SetDatabaseOptionAsync(string database, DbConfigSetting setting, RemediationIdentity identity, CancellationToken ct)
            => _databaseService.SetDatabaseOptionAsync(database, setting, identity, ct);

        public Task<FileGrowthPreflight> PreflightFileGrowthAsync(string database, string logicalFileName, int growthMb, CancellationToken ct)
            => _databaseService.PreflightFileGrowthAsync(database, logicalFileName, growthMb, ct);

        public Task<FileGrowthOutcome> SetFileGrowthAsync(string database, string logicalFileName, int growthMb, RemediationIdentity identity, CancellationToken ct)
            => _databaseService.SetFileGrowthAsync(database, logicalFileName, growthMb, identity, ct);

        public Task<ServerConfigPreflight> PreflightServerConfigAsync(ServerConfigSetting setting, long recommendedValue, CancellationToken ct)
            => _databaseService.PreflightServerConfigAsync(setting, recommendedValue, ct);

        public Task<ServerConfigOutcome> SetServerConfigAsync(ServerConfigSetting setting, long value, RemediationIdentity identity, CancellationToken ct)
            => _databaseService.SetServerConfigAsync(setting, value, identity, ct);

        public Task<ClearPlanOutcome> ClearProcCacheAsync(string queryHash, RemediationIdentity identity, CancellationToken ct)
            => _databaseService.ClearProcCacheAsync(queryHash, identity, ct);

        public Task<bool> WriteAuditAsync(RemediationAuditRecord record, CancellationToken ct)
            => _auditWriter.WriteAsync(record, ct);
    }
}
