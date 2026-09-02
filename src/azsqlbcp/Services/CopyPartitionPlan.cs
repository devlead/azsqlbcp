using System.Data;
using AzSqlBcp.Commands;
using Microsoft.Data.SqlClient;

namespace AzSqlBcp.Services;

public static class CopyPartitionPlan
{
    private const int HistogramCardinalityThreshold = 100_000;

    public static async Task<CopyCheckpointPlan> BuildAsync(
        CopySettings settings,
        string sourceQualified,
        string partitionQuoted,
        long minId,
        long maxId,
        long totalRowCount,
        string accessToken,
        CancellationToken cancellationToken)
    {
        List<PartitionPlanEntry> ranges = settings.PartitionStrategy switch
        {
            PartitionStrategy.RowBalanced => await BuildRowBalancedAsync(
                settings, sourceQualified, partitionQuoted, minId, maxId, totalRowCount,
                accessToken, cancellationToken).ConfigureAwait(false),
            _ => await BuildIdRangeAsync(
                settings, sourceQualified, partitionQuoted, minId, maxId, totalRowCount,
                accessToken, cancellationToken).ConfigureAwait(false)
        };

        return new CopyCheckpointPlan
        {
            TotalRowCount = totalRowCount,
            CreatedUtc = DateTimeOffset.UtcNow,
            Ranges = ranges
        };
    }

    private static async Task<List<PartitionPlanEntry>> BuildIdRangeAsync(
        CopySettings settings,
        string sourceQualified,
        string partitionQuoted,
        long minId,
        long maxId,
        long totalRowCount,
        string accessToken,
        CancellationToken cancellationToken)
    {
        var idRanges = BulkCopyService.BuildRanges(minId, maxId, settings.EffectivePartitions);
        var entries = idRanges.Select(r => new PartitionPlanEntry
        {
            Index = r.Index,
            Lo = r.Lo,
            Hi = r.Hi,
            ExpectedRows = EstimateExpectedRows(totalRowCount, minId, maxId, r.Lo, r.Hi),
            Status = PartitionStatus.Pending
        }).ToList();

        await PopulateExpectedRowsAsync(
            settings, sourceQualified, partitionQuoted, entries, accessToken, cancellationToken)
            .ConfigureAwait(false);

        return entries;
    }

    private static async Task<List<PartitionPlanEntry>> BuildRowBalancedAsync(
        CopySettings settings,
        string sourceQualified,
        string partitionQuoted,
        long minId,
        long maxId,
        long totalRowCount,
        string accessToken,
        CancellationToken cancellationToken)
    {
        var cardinality = unchecked(maxId - minId + 1);
        if (cardinality <= HistogramCardinalityThreshold)
        {
            var histogram = await LoadHistogramAsync(
                settings, sourceQualified, partitionQuoted, accessToken, cancellationToken)
                .ConfigureAwait(false);

            if (histogram.Count > 0)
                return BuildFromHistogram(histogram, settings.EffectivePartitions, totalRowCount);
        }

        return await BuildFromNtileAsync(
            settings, sourceQualified, partitionQuoted, settings.EffectivePartitions,
            accessToken, cancellationToken).ConfigureAwait(false);
    }

    internal static List<PartitionPlanEntry> BuildFromHistogram(
        IReadOnlyList<(long Key, long Count)> histogram,
        int partitions,
        long totalRowCount)
    {
        if (histogram.Count == 0 || partitions < 1)
            return [];

        var targetPerPartition = Math.Max(1L, (long)Math.Ceiling((double)totalRowCount / partitions));
        var entries = new List<PartitionPlanEntry>(partitions);
        var index = 0;
        long lo = histogram[0].Key;
        long partitionRows = 0;
        long previousKey = histogram[0].Key;

        for (var i = 0; i < histogram.Count; i++)
        {
            var (key, count) = histogram[i];
            var remainingPartitions = partitions - entries.Count;
            var wouldExceedTarget = partitionRows > 0
                && partitionRows + count > targetPerPartition
                && remainingPartitions > 1;

            if (wouldExceedTarget)
            {
                entries.Add(new PartitionPlanEntry
                {
                    Index = index++,
                    Lo = lo,
                    Hi = previousKey,
                    ExpectedRows = partitionRows,
                    Status = PartitionStatus.Pending
                });

                lo = key;
                partitionRows = 0;
            }

            partitionRows += count;
            previousKey = key;

            var isLastKey = i == histogram.Count - 1;
            if (isLastKey)
            {
                entries.Add(new PartitionPlanEntry
                {
                    Index = index++,
                    Lo = lo,
                    Hi = key,
                    ExpectedRows = partitionRows,
                    Status = PartitionStatus.Pending
                });
            }
        }

        while (entries.Count < partitions)
        {
            var lastHi = entries.Count > 0 ? entries[^1].Hi : histogram[^1].Key;
            entries.Add(new PartitionPlanEntry
            {
                Index = index++,
                Lo = lastHi,
                Hi = lastHi,
                ExpectedRows = 0,
                Status = PartitionStatus.Pending
            });
        }

        return entries;
    }

    private static async Task<List<(long Key, long Count)>> LoadHistogramAsync(
        CopySettings settings,
        string sourceQualified,
        string partitionQuoted,
        string accessToken,
        CancellationToken cancellationToken)
    {
        await using var conn = BulkCopyService.CreateConnection(
            settings.SourceServer, settings.SourceDatabase, accessToken,
            settings.SourcePort, settings.SourceReadOnly, settings.SourceTrustServerCertificate);
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);

        await using var cmd = new SqlCommand(
            $"SELECT {partitionQuoted}, COUNT_BIG(*) FROM {sourceQualified} GROUP BY {partitionQuoted} ORDER BY {partitionQuoted}",
            conn)
        {
            CommandType = CommandType.Text,
            CommandTimeout = 500000
        };

        var result = new List<(long Key, long Count)>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            result.Add((Convert.ToInt64(reader.GetValue(0)), Convert.ToInt64(reader.GetValue(1))));
        }

        return result;
    }

    private static async Task<List<PartitionPlanEntry>> BuildFromNtileAsync(
        CopySettings settings,
        string sourceQualified,
        string partitionQuoted,
        int partitions,
        string accessToken,
        CancellationToken cancellationToken)
    {
        await using var conn = BulkCopyService.CreateConnection(
            settings.SourceServer, settings.SourceDatabase, accessToken,
            settings.SourcePort, settings.SourceReadOnly, settings.SourceTrustServerCertificate);
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);

        await using var cmd = new SqlCommand(
            $"""
             SELECT bucket, MIN({partitionQuoted}) AS lo, MAX({partitionQuoted}) AS hi, COUNT_BIG(*) AS expectedRows
             FROM (
                 SELECT {partitionQuoted}, NTILE(@partitions) OVER (ORDER BY {partitionQuoted}) AS bucket
                 FROM {sourceQualified}
             ) t
             GROUP BY bucket
             ORDER BY bucket
             """,
            conn)
        {
            CommandType = CommandType.Text,
            CommandTimeout = 500000
        };

        cmd.Parameters.AddWithValue("@partitions", partitions);

        var entries = new List<PartitionPlanEntry>(partitions);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            entries.Add(new PartitionPlanEntry
            {
                Index = reader.GetInt32(0) - 1,
                Lo = Convert.ToInt64(reader.GetValue(1)),
                Hi = Convert.ToInt64(reader.GetValue(2)),
                ExpectedRows = Convert.ToInt64(reader.GetValue(3)),
                Status = PartitionStatus.Pending
            });
        }

        return entries;
    }

    private static async Task PopulateExpectedRowsAsync(
        CopySettings settings,
        string sourceQualified,
        string partitionQuoted,
        IReadOnlyList<PartitionPlanEntry> entries,
        string accessToken,
        CancellationToken cancellationToken)
    {
        var semaphore = new SemaphoreSlim(Math.Min(8, settings.Parallelism));
        var tasks = entries.Select(async entry =>
        {
            await semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                entry.ExpectedRows = await BulkCopyService.GetRangeCountAsync(
                    settings.SourceServer, settings.SourceDatabase, sourceQualified, partitionQuoted,
                    entry.Lo, entry.Hi, accessToken, settings.SourcePort, settings.SourceReadOnly,
                    settings.SourceTrustServerCertificate, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                semaphore.Release();
            }
        });

        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    private static long EstimateExpectedRows(long totalRowCount, long minId, long maxId, long lo, long hi)
    {
        var idSpan = Math.Max(1L, unchecked(maxId - minId + 1));
        var span = Math.Max(1L, unchecked(hi - lo + 1));
        return Math.Max(0L, (long)Math.Round(totalRowCount * (double)span / idSpan));
    }
}
