using System;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.File;

namespace AzureCopy
{
    public abstract class TransferInfo : ITransferInfo
    {
        // same default as DataMovement SDK
        protected const int MaxBatchResults = 250;
        protected static readonly TimeSpan MaximumExecutionTime = TimeSpan.FromMinutes(45);
        protected static readonly TimeSpan ServerTimeout = TimeSpan.FromMinutes(10);

        protected readonly static BlobRequestOptions DefaultBlobRequestOptions = new BlobRequestOptions
        {
            MaximumExecutionTime = MaximumExecutionTime,
            ServerTimeout = ServerTimeout,
            StoreBlobContentMD5 = true,
            UseTransactionalMD5 = true,
        };

        protected readonly static FileRequestOptions DefaultFileRequestOptions = new FileRequestOptions
        {
            MaximumExecutionTime = MaximumExecutionTime,
            ServerTimeout = ServerTimeout,
            StoreFileContentMD5 = true,
            UseTransactionalMD5 = true,
        };

        protected TransferInfo(string fullName, string relativePath)
        {
            FullName = fullName;
            RelativePath = relativePath;
        }

        public string FullName { get; private set; }

        public string RelativePath { get; private set; }

        public abstract bool Exists { get; }

    }
}
