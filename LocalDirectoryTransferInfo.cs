using System.Collections.Generic;
using System.IO;

namespace AzureCopy
{
    public class LocalDirectoryTransferInfo : TransferInfo, IDirectoryTransferInfo
    {
        private readonly DirectoryInfo _directoryInfo;

        public LocalDirectoryTransferInfo(DirectoryInfo directoryInfo, string relativePath) : base(directoryInfo.FullName, relativePath)
        {
            _directoryInfo = directoryInfo;
        }

        public override bool Exists => _directoryInfo.Exists;

        public IDirectoryTransferInfo CreateIfNotExists(string relativePath = null)
        {
            var dir = string.IsNullOrEmpty(relativePath) ? _directoryInfo : new DirectoryInfo(Path.Combine(_directoryInfo.FullName, relativePath));
            if (!dir.Exists)
            {
                dir.Create();
            }
            return string.IsNullOrEmpty(relativePath) ? this : new LocalDirectoryTransferInfo(dir, relativePath);
        }

        public IFileTransferInfo GetChildEntry(string relativePath) 
            => new LocalFileTransferInfo(new FileInfo(Path.Combine(_directoryInfo.FullName, relativePath)), relativePath);

        public IEnumerable<ITransferInfo> EnumerateEntries()
        {
            foreach (var item in _directoryInfo.EnumerateFileSystemInfos("*", SearchOption.AllDirectories))
            {
                var relativePath = item.FullName.Substring(_directoryInfo.FullName.Length).Replace('\\', '/').Trim('/');
                var fileInfo = item as FileInfo;
                if (fileInfo != null)
                {
                    yield return new LocalFileTransferInfo(fileInfo, relativePath);
                }
                else
                {
                    yield return new LocalDirectoryTransferInfo((DirectoryInfo)item, relativePath);
                }
            }
        }
    }
}
