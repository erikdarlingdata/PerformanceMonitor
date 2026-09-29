/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using Xunit;

namespace Darling.Tests;

/// <summary>
/// Serializes the log-rotation live classes (#4704). They call <c>pg_rotate_logfile()</c> on a shared target,
/// and PgServerLogTailRotationLiveTests and PgDeadlocksResumeLiveTests use the SAME one
/// (<c>DARLING_TEST_PG_LOGROTATE</c>). Run in parallel, one class's rotation lands between another's workload
/// and its read and moves the file it expects to find its entry in.
/// </summary>
[CollectionDefinition("pg-log-rotation", DisableParallelization = true)]
public sealed class PgLogRotationCollection
{
}
