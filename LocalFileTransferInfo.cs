using System;
using System.IO;
using System.Security.Cryptography;

namespace AzureCopy
{
    public class LocalFileTransferInfo : TransferInfo, IFileTransferInfo
    {
        private readonly FileInfo _file;
        private string _md5Hash;

        public LocalFileTransferInfo(FileInfo file, string relativePath) : base(file.FullName, relativePath)
        {
            _file = file;
        }

        public override bool Exists => _file.Exists;

        public long Length => _file.Length;

        public string MD5Hash => GetMD5Hash();

        public Stream ReadStream() => _file.OpenRead();

        public void WriteStream(Stream stream)
        {
            _md5Hash = null;
            using (var fileStream = _file.OpenWrite())
            {
                stream.CopyTo(fileStream);
            }
        }

        private string GetMD5Hash()
        {
            if (_md5Hash == null)
            {
                if (Exists)
                {
                    using (var md5 = MD5.Create())
                    using (var stream = _file.OpenRead())
                    {
                        var hash = md5.ComputeHash(stream);
                        _md5Hash = Convert.ToBase64String(hash);
                    }
                }
                else
                {
                    _md5Hash = string.Empty;
                }
            }

            return _md5Hash;
        }
    }
}
