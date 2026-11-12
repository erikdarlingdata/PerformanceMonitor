using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using System.Xml;

namespace PerformanceMonitor.Exporter;

/// <summary>
/// Computes the procedure_plan shape hash: a hash of a module plan's ordered statement <c>QueryPlanHash</c> values.
/// Forward-only <see cref="XmlReader"/>, no DOM — this only needs one attribute value per statement, and module plans
/// can run to tens of statements over hundreds of KB of XML. Never throws: a malformed document returns whatever
/// hashes were collected before the reader hit the error, because a truncated plan must not stop the export run that
/// carries it.
/// </summary>
public static class PlanShapeHasher
{
    /// <summary>
    /// Uppercases the hex digits of a hash like "0xfd1abb424011422e" while keeping the "0x" prefix's own
    /// lowercase "x" — that is the form the showplan XML and a loader's query_plan_hash column both use.
    /// </summary>
    private static string Uppercase(string value)
    {
        if (value.Length >= 2 && value[0] == '0' && (value[1] == 'x' || value[1] == 'X'))
            return "0x" + value.Substring(2).ToUpperInvariant();
        return value.ToUpperInvariant();
    }

    public static (string ShapeHash, IReadOnlyList<string> StatementPlanHashes) Compute(string xml)
    {
        var hashes = new List<string>();

        try
        {
            var settings = new XmlReaderSettings
            {
                DtdProcessing = DtdProcessing.Prohibit,
                XmlResolver = null,
            };

            using var stringReader = new System.IO.StringReader(xml);
            using var reader = XmlReader.Create(stringReader, settings);

            while (reader.Read())
            {
                if (reader.NodeType != XmlNodeType.Element)
                    continue;

                var value = reader.GetAttribute("QueryPlanHash");
                if (!string.IsNullOrEmpty(value))
                    hashes.Add(Uppercase(value));
            }
        }
        catch
        {
            // Return whatever was collected before the reader failed.
        }

        var joined = string.Join(',', hashes);
        var digest = SHA256.HashData(Encoding.UTF8.GetBytes(joined));
        var shapeHash = Convert.ToHexStringLower(digest.AsSpan(0, 16));

        return (shapeHash, hashes);
    }
}
