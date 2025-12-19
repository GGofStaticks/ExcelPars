using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using ClosedXML.Excel;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace ExcelPars
{
    class Program
    {
        static async Task Main(string[] args)
        {
            string excelFilePath = "input.xlsx";   
            string bucketName = "";
            string accessKey = "";
            string secretKey = ""; 
            string serviceUrl = "";
            string outputFolder = "DownloadedFiles";

            if (!Directory.Exists(outputFolder))
            {
                Directory.CreateDirectory(outputFolder);
            }

            List<string> paths = ReadPathsFromExcel(excelFilePath);

            if (paths.Count == 0)
            {
                Console.WriteLine("В Excel не найдено ни одного пути. Проверьте файл.");
                Console.ReadKey();
                return;
            }

            await DownloadFilesFromS3(paths, bucketName, accessKey, secretKey, serviceUrl, outputFolder);

            Console.WriteLine("Все файлы скачаны.");
            Console.ReadKey();
        }

        private static List<string> ReadPathsFromExcel(string excelFilePath)
        {
            var paths = new List<string>();

            using (var workbook = new XLWorkbook(excelFilePath))
            {
                var worksheet = workbook.Worksheet(1);
                int row = 2;

                while (!worksheet.Cell(row, 1).IsEmpty()) 
                {
                    string path = worksheet.Cell(row, 2).GetValue<string>().Trim();
                    if (!string.IsNullOrWhiteSpace(path))
                    {
                        paths.Add(path);
                        Console.WriteLine($"Найден путь: {path}");
                    }
                    row++;
                }
            }

            return paths;
        }


        private static async Task DownloadFilesFromS3(List<string> paths, string bucketName,
            string accessKey, string secretKey, string serviceUrl, string outputFolder)
        {
            var credentials = new BasicAWSCredentials(accessKey, secretKey);
            var config = new AmazonS3Config
            {
                ServiceURL = serviceUrl,
                ForcePathStyle = true 
            };

            using (var s3Client = new AmazonS3Client(credentials, config))
            {
                var transferUtility = new TransferUtility(s3Client);

                foreach (var key in paths)
                {
                    try
                    {
                        string fileName = Path.GetFileName(key); 
                        string localPath = Path.Combine(outputFolder, fileName); 

                        // если нужно сохранять с полной структурой папок, то тогда этот метод
                        // string localPath = Path.Combine(outputFolder, key.Replace("/", Path.DirectorySeparatorChar.ToString()));

                        var request = new TransferUtilityDownloadRequest
                        {
                            BucketName = bucketName,
                            Key = key,
                            FilePath = localPath
                        };

                        await transferUtility.DownloadAsync(request);
                        Console.WriteLine($"Скачан: {key} → {localPath}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Ошибка при скачивании {key}: {ex.Message}");
                    }
                }
            }
        }
    }
}