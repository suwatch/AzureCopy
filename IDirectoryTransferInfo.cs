using System.Collections.Generic;

namespace AzureCopy
{
    public interface IDirectoryTransferInfo : ITransferInfo
    {
        IDirectoryTransferInfo CreateIfNotExists(string relativePath = null);

        IFileTransferInfo GetChildEntry(string relativePath);

        IEnumerable<ITransferInfo> EnumerateEntries();
    }
}
