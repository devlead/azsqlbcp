using System.Data;
using Microsoft.Data.SqlClient;

namespace AzSqlBcp.Services;

public sealed partial class BulkCopyService
{
    private static async Task CopyPartitionAsync(
        string sourceServer,
        string sourceDatabase,
        string sourceQualified,
        string targetServer,
        string targetDatabase,
        string targetQualified,
        string? partitionQuoted,
        IdRange? range,
        int batchSize,
        string accessToken,
        bool sourceReadOnly,
        int sourcePort,
        int targetPort,
        bool sourceTrustServerCertificate,
        bool targetTrustServerCertificate,
        CopyProgressReporter progress,
        CancellationToken cancellationToken)
    {
        await using SqlConnection
            sourceConn = CreateConnection(
                sourceServer, sourceDatabase, accessToken, sourcePort, sourceReadOnly, sourceTrustServerCertificate),
            targetConn = CreateConnection(
                targetServer, targetDatabase, accessToken, targetPort, trustServerCertificate: targetTrustServerCertificate);

        await sourceConn.OpenAsync(cancellationToken).ConfigureAwait(false);
        await targetConn.OpenAsync(cancellationToken).ConfigureAwait(false);

        var partitionIndex = range?.Index ?? 0;
        var commandText = partitionQuoted is null || range is null
            ? $"SELECT * FROM {sourceQualified}"
            : $"SELECT * FROM {sourceQualified} WHERE {partitionQuoted} >= @lo AND {partitionQuoted} <= @hi";

        await using var sourceCmd = new SqlCommand
        {
            Connection = sourceConn,
            CommandType = CommandType.Text,
            CommandText = commandText,
            CommandTimeout = 500000
        };

        if (partitionQuoted is not null && range is not null)
        {
            sourceCmd.Parameters.AddWithValue("@lo", range.Value.Lo);
            sourceCmd.Parameters.AddWithValue("@hi", range.Value.Hi);
        }

        await using var reader = await sourceCmd
            .ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken)
            .ConfigureAwait(false);

        var notifyAfter = Math.Min(batchSize, 100_000);

        using var bcp = new SqlBulkCopy(targetConn, SqlBulkCopyOptions.KeepIdentity|SqlBulkCopyOptions.TableLock, null)
        {
            DestinationTableName = targetQualified,
            BatchSize = batchSize,
            NotifyAfter = notifyAfter,
            BulkCopyTimeout = 3600,
            EnableStreaming = true
        };

        for (var i = 0; i < reader.FieldCount; i++)
        {
            var name = reader.GetName(i);
            bcp.ColumnMappings.Add(name, name);
        }

        bcp.SqlRowsCopied += (_, _) => progress.Report(partitionIndex, bcp.RowsCopied64);

        await bcp.WriteToServerAsync(reader, cancellationToken).ConfigureAwait(false);

        progress.Complete(partitionIndex, bcp.RowsCopied64);
    }

    internal static List<IdRange> BuildRanges(long minId, long maxId, int parallelism)
    {
        if (parallelism < 1)
            throw new ArgumentOutOfRangeException(nameof(parallelism));

        var total = unchecked(maxId - minId + 1);
        if (total <= 0)
            return [new IdRange(0, minId, maxId)];

        var parts = (int)Math.Min(parallelism, total);
        var ranges = new List<IdRange>(parts);
        var size = total / parts;
        var remainder = total % parts;
        var lo = minId;

        for (var i = 0; i < parts; i++)
        {
            var count = size + (i < remainder ? 1 : 0);
            var hi = lo + count - 1;
            ranges.Add(new IdRange(i, lo, hi));
            lo = hi + 1;
        }

        return ranges;
    }

    public readonly record struct IdRange(int Index, long Lo, long Hi);
}
