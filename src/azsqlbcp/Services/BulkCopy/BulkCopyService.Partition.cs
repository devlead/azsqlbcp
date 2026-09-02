using System.Data;
using AzSqlBcp.Services;
using Microsoft.Data.SqlClient;

namespace AzSqlBcp.Services.BulkCopy;

public sealed partial class BulkCopyService
{
    internal static async Task<long> GetRangeCountAsync(
        string server,
        string database,
        string qualifiedTable,
        string partitionQuoted,
        long lo,
        long hi,
        string accessToken,
        int port,
        bool readOnlyIntent,
        bool trustServerCertificate,
        CancellationToken cancellationToken,
        int commandTimeoutSeconds = 500_000)
    {
        await using var conn = CreateConnection(server, database, accessToken, port, readOnlyIntent, trustServerCertificate);
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);

        await using var cmd = new SqlCommand(
            $"SELECT COUNT_BIG(*) FROM {qualifiedTable} WHERE {partitionQuoted} >= @lo AND {partitionQuoted} <= @hi",
            conn)
        {
            CommandType = CommandType.Text,
            CommandTimeout = commandTimeoutSeconds
        };

        cmd.Parameters.AddWithValue("@lo", lo);
        cmd.Parameters.AddWithValue("@hi", hi);

        var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is long count ? count : Convert.ToInt64(result);
    }

    internal static async Task DeletePartitionSliceAsync(
        string server,
        string database,
        string qualifiedTable,
        string partitionQuoted,
        long lo,
        long hi,
        string accessToken,
        int port,
        bool trustServerCertificate,
        CancellationToken cancellationToken)
    {
        await using var conn = CreateConnection(server, database, accessToken, port, trustServerCertificate: trustServerCertificate);
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);

        await using var cmd = new SqlCommand(
            $"DELETE FROM {qualifiedTable} WHERE {partitionQuoted} >= @lo AND {partitionQuoted} <= @hi",
            conn)
        {
            CommandType = CommandType.Text,
            CommandTimeout = 500000
        };

        cmd.Parameters.AddWithValue("@lo", lo);
        cmd.Parameters.AddWithValue("@hi", hi);

        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

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
        CancellationToken cancellationToken,
        bool useTableLock = true,
        bool finalizeProgress = true)
    {
        var partitionIndex = range?.Index ?? 0;
        progress.Start(partitionIndex);

        await using SqlConnection
            sourceConn = CreateConnection(
                sourceServer, sourceDatabase, accessToken, sourcePort, sourceReadOnly, sourceTrustServerCertificate),
            targetConn = CreateConnection(
                targetServer, targetDatabase, accessToken, targetPort, trustServerCertificate: targetTrustServerCertificate);

        await sourceConn.OpenAsync(cancellationToken).ConfigureAwait(false);
        await targetConn.OpenAsync(cancellationToken).ConfigureAwait(false);
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

        var notifyAfter = Math.Min(batchSize, 50_000);
        var options = useTableLock
            ? SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.TableLock
            : SqlBulkCopyOptions.KeepIdentity;

        using var bcp = new SqlBulkCopy(targetConn, options, null)
        {
            DestinationTableName = targetQualified,
            BatchSize = batchSize,
            NotifyAfter = notifyAfter,
            BulkCopyTimeout = 7200,
            EnableStreaming = true
        };

        for (var i = 0; i < reader.FieldCount; i++)
        {
            var name = reader.GetName(i);
            bcp.ColumnMappings.Add(name, name);
        }

        bcp.SqlRowsCopied += (_, _) => progress.Report(partitionIndex, bcp.RowsCopied64);

        await bcp.WriteToServerAsync(reader, cancellationToken).ConfigureAwait(false);

        if (finalizeProgress)
            progress.Complete(partitionIndex, bcp.RowsCopied64);
        else
            progress.Report(partitionIndex, bcp.RowsCopied64);
    }

    internal static List<IdRange> BuildRanges(long minId, long maxId, int partitions)
    {
        if (partitions < 1)
            throw new ArgumentOutOfRangeException(nameof(partitions));

        var total = unchecked(maxId - minId + 1);
        if (total <= 0)
            return [new IdRange(0, minId, maxId)];

        var parts = (int)Math.Min(partitions, total);
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
