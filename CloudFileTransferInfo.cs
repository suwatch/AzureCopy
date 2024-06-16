using System.IO;
using Microsoft.WindowsAzure.Storage.File;

namespace AzureCopy
{
    public class CloudFileTransferInfo : TransferInfo, IFileTransferInfo
    {
        private readonly CloudFile _file;
        private bool _attributesFetched;

        public CloudFileTransferInfo(CloudFile file, string relativePath) : base(file.Uri.AbsoluteUri, relativePath)
        {
            _file = file;
        }

        public override bool Exists => _file.Exists();

        public long Length => FetchProperties().Length;

        public string MD5Hash => FetchProperties().ContentMD5;

        public Stream ReadStream() => _file.OpenRead();

        public void WriteStream(Stream stream)
        {
            _attributesFetched = false;
            _file.UploadFromStream(stream, options: DefaultFileRequestOptions);
        }

        private FileProperties FetchProperties()
        {
            if (!_attributesFetched)
            {
                if (Exists)
                {
                    _file.FetchAttributes();
                }

                _attributesFetched = true;
            }

            return _file.Properties;
        }
    }
}
