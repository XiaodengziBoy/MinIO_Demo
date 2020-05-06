using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;

namespace MinIOProject
{
    class Program
    {
        static void Main(string[] args)
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            Task task = Task.Run(MainAsync).ContinueWith(d =>
             {
                 Console.WriteLine("文件传输完毕");
                 stopwatch.Stop();
                 Console.WriteLine($"耗时:{stopwatch.ElapsedMilliseconds}ms");
             });
            task.Wait();
        }

        public static string accessKey = "minioadmin";
        public static string secretKey = "minioadmin";
        public static AmazonS3Config config = new AmazonS3Config
        {
            RegionEndpoint = RegionEndpoint.USEast1, // 必须在设置ServiceURL前进行设置，并且需要和`MINIO_REGION`环境变量一致。
            ServiceURL = "http://localhost:9000", // 替换成你自己的MinIO Server的URL
            ForcePathStyle = true // 必须设为true
        };

        public static async Task MainAsync()
        {
            string path = string.Format(@$"E:\迅雷下载");
            DirectoryInfo directoryInfo = new DirectoryInfo(path);
            List<FileInfo> files = GetFile(path);
            var amazonS3Client = new AmazonS3Client(accessKey, secretKey, config);
            var listBucketResponse = await amazonS3Client.ListBucketsAsync();
            if (await DeleBucket(amazonS3Client, listBucketResponse))
            {
                string bucketName = "3333333333333";
                if (await AddBuckets(amazonS3Client, bucketName))
                {
                    await AddObjects(amazonS3Client, bucketName, files);
                }
            }
            else
            {
                throw new Exception("删除Bucket或Object失败");
            }
        }

        public static async Task<bool> AddObjects(AmazonS3Client amazonS3Client, string bucketName, List<FileInfo> fileInfos)
        {
            foreach (var item in fileInfos)
            {
                PutObjectRequest putObjectRequest = new PutObjectRequest
                {
                    BucketName = bucketName,
                    Key = item.Name,
                    //ContentType = "text/plain",
                    //FilePath = item.FullName,
                    InputStream=File.OpenRead(item.FullName)
                };
                PutObjectResponse putObjectResponse = await amazonS3Client.PutObjectAsync(putObjectRequest);
                if (putObjectResponse.HttpStatusCode != HttpStatusCode.OK)
                {
                    Console.WriteLine($"添加失败 {Environment.NewLine}BucketName:{bucketName}");
                    throw new Exception("添加失败");
                }
            }
            return true;
        }

        public static async Task<bool> AddBuckets(AmazonS3Client amazonS3Client, string bucketName)
        {
            PutBucketResponse putBucketResponse = await amazonS3Client.PutBucketAsync(bucketName);
            if (putBucketResponse.HttpStatusCode != HttpStatusCode.OK)
            {
                Console.WriteLine($"添加失败 {Environment.NewLine}BucketName:{bucketName}");
                throw new Exception("添加失败");
            }
            return true;
        }

        public static async Task<bool> DeleBucket(AmazonS3Client amazonS3Client, ListBucketsResponse listBucketsResponse)
        {
            bool isDel = true;
            foreach (S3Bucket bucket in listBucketsResponse.Buckets)
            {
                ListObjectsResponse getObjectResponse = await amazonS3Client.ListObjectsAsync(bucket.BucketName);
                if (!await DeleObject(amazonS3Client, getObjectResponse))
                {
                    isDel = false;
                }
                DeleteBucketResponse deleteBucketResponse = await amazonS3Client.DeleteBucketAsync(bucket.BucketName);
                if (deleteBucketResponse.HttpStatusCode != HttpStatusCode.NoContent)
                {
                    Console.WriteLine($"删除失败 {Environment.NewLine}BucketName:{bucket.BucketName}BucketName:{bucket.CreationDate}");
                    throw new Exception("删除Bucket失败");
                }
            }
            return isDel;
        }

        public static async Task<bool> DeleObject(AmazonS3Client amazonS3Client, ListObjectsResponse listObjectsResponse)
        {
            foreach (S3Object s3Object in listObjectsResponse.S3Objects)
            {
                DeleteObjectResponse deleteObjectResponse = await amazonS3Client.DeleteObjectAsync(s3Object.BucketName, s3Object.Key);
                if (deleteObjectResponse.HttpStatusCode != HttpStatusCode.NoContent)
                {
                    Console.WriteLine($"删除失败 {Environment.NewLine}BucketName:{s3Object.BucketName}BucketName:{s3Object.Key}Size:{s3Object.Size}");
                    throw new Exception("删除Object失败");
                }
            }
            return true;
        }

        /// <summary>
        /// 非递归的遍历所有的子目录与文件
        /// </summary>
        /// <param name="node"></param>
        /// <param name="dir"></param>
        public static List<FileSystemInfo> Traverse(DirectoryInfo dir)
        {
            List<FileSystemInfo> fileSystemInfos = new List<FileSystemInfo>();
            Stack<DirectoryInfo> stack_dir = new Stack<DirectoryInfo>(); // 用栈来保存没有遍历的子目录
            Stack<FileInfo> stack_file = new Stack<FileInfo>(); // 用栈来保存没有遍历的子目录
            DirectoryInfo currentDir = dir;
            stack_dir.Push(dir);

            while (stack_dir.Count != 0) // 栈不为空，说明还有子节点没有访问到
            {
                currentDir = stack_dir.Pop(); // 出栈，获取上一个结点

                // 访问当前目录所有子目录
                DirectoryInfo[] subDirs = currentDir.GetDirectories();
                foreach (DirectoryInfo di in subDirs)
                {
                    fileSystemInfos.Add(di);
                    stack_dir.Push(di);  // 将子节点入栈
                }

                // 访问当前目录所有子文件
                FileInfo[] files = currentDir.GetFiles();
                foreach (FileInfo f in files)
                {
                    fileSystemInfos.Add(f);
                    stack_file.Push(f);
                }
            }
            return fileSystemInfos;
        }

        /// <summary>
        /// 获取路径下所有文件以及子文件夹中文件
        /// </summary>
        /// <param name="path">全路径根目录</param>
        /// <returns></returns>
        public static List<FileInfo> GetFile(string path)
        {
            DirectoryInfo dir = new DirectoryInfo(path);
            List<FileInfo> fil = dir.GetFiles().ToList();
            DirectoryInfo[] dii = dir.GetDirectories();
            //获取子文件夹内的文件列表，递归遍历
            foreach (DirectoryInfo d in dii)
            {
                fil.AddRange(GetFile(d.FullName));
            }
            return fil;
        }
    }
}
