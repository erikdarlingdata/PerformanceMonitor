/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Collectors;

public enum SchemaChangeKind
{
    None,
    Baseline,
    Created,
    Altered,
    Renamed,
    Dropped
}

/// <summary>The current stored version of an object: what the classifier compares a fresh read against.</summary>
public sealed record StoredSchemaObject(long VersionId, int ObjectId, string Schema, string Name, byte[] Hash);

/// <summary>
/// #7643: what a read of an object means against its current stored version. Works purely from the three inputs. A drop is never
/// classified here: the writer records it when a complete read no longer holds the object.
/// </summary>
public static class SchemaChangeClassifier
{
    public static SchemaChangeKind Classify(StoredSchemaObject? current, SchemaObjectDefinition read, bool baselineRead)
    {
        ArgumentNullException.ThrowIfNull(read);
        if (current == null)
        {
            return baselineRead ? SchemaChangeKind.Baseline : SchemaChangeKind.Created;
        }

        if (!current.Hash.AsSpan().SequenceEqual(SchemaCanonicalForm.Hash(read)))
        {
            return SchemaChangeKind.Altered;
        }

        var sameName = string.Equals(current.Schema, read.Schema, StringComparison.Ordinal)
            && string.Equals(current.Name, read.Name, StringComparison.Ordinal);
        return sameName ? SchemaChangeKind.None : SchemaChangeKind.Renamed;
    }
}
