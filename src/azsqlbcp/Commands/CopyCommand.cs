using AzSqlBcp.Services;
using Spectre.Console.Cli;

namespace AzSqlBcp.Commands;

public sealed class CopyCommand(BulkCopyService service) : AsyncCommand<CopySettings>
{
    protected override async Task<int> ExecuteAsync(CommandContext context, CopySettings settings, CancellationToken cancellationToken)
    {
        await service.CopyAsync(
            settings.SourceServer,
            settings.SourceDatabase,
            settings.SourceTable,
            settings.TargetServer,
            settings.TargetDatabase,
            settings.TargetTable,
            settings.PartitionColumn,
            settings.NoPartitionColumn,
            settings.SourceReadOnly,
            settings.SourcePort,
            settings.TargetPort,
            settings.SourceTrustServerCertificate,
            settings.TargetTrustServerCertificate,
            settings.Parallelism,
            settings.BatchSize,
            cancellationToken).ConfigureAwait(false);

        return 0;
    }
}
