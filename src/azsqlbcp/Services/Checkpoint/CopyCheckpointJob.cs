using System.Security.Cryptography;
using System.Text;
using AzSqlBcp.Commands;

namespace AzSqlBcp.Services.Checkpoint;

public sealed class CopyCheckpointJob
{
    public required string SourceServer { get; init; }
    public required string SourceDatabase { get; init; }
    public required string SourceTable { get; init; }
    public required string TargetServer { get; init; }
    public required string TargetDatabase { get; init; }
    public required string TargetTable { get; init; }
    public required string PartitionColumn { get; init; }
    public PartitionStrategy PartitionStrategy { get; init; }
    public int Partitions { get; init; }
    public int Parallelism { get; init; }
    public int BatchSize { get; init; }

    public string ComputeFingerprint()
    {
        // BatchSize and Parallelism are runtime knobs and may change on --resume.
        var payload = string.Join('\n', new[]
        {
            SourceServer.Trim().ToLowerInvariant(),
            SourceDatabase.Trim().ToLowerInvariant(),
            SourceTable.Trim().ToLowerInvariant(),
            TargetServer.Trim().ToLowerInvariant(),
            TargetDatabase.Trim().ToLowerInvariant(),
            TargetTable.Trim().ToLowerInvariant(),
            PartitionColumn.Trim().ToLowerInvariant(),
            PartitionStrategy.ToString(),
            Partitions.ToString()
        });

        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(payload));
        return Convert.ToHexStringLower(hash);
    }

    public static CopyCheckpointJob FromSettings(CopySettings settings) => new()
    {
        SourceServer = settings.SourceServer,
        SourceDatabase = settings.SourceDatabase,
        SourceTable = settings.SourceTable,
        TargetServer = settings.TargetServer,
        TargetDatabase = settings.TargetDatabase,
        TargetTable = settings.TargetTable,
        PartitionColumn = settings.PartitionColumn!,
        PartitionStrategy = settings.PartitionStrategy,
        Partitions = settings.EffectivePartitions,
        Parallelism = settings.Parallelism,
        BatchSize = settings.BatchSize
    };
}
