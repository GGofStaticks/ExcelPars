using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using ClosedXML.Excel;
using Npgsql;

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

            string connectionString = "Host=localhost;Port=5434;Database=;Username=postgres;Password=";

            if (!Directory.Exists(outputFolder))
            {
                Directory.CreateDirectory(outputFolder);
            }

            List<string> pathsFromDb = GetPathsFromDatabase(connectionString);

            if (pathsFromDb.Count == 0)
            {
                Console.WriteLine("Не найдено ни одного заказа");
                Console.ReadKey();
                return;
            }

            UpdateExcelWithPaths(excelFilePath, pathsFromDb);

            List<string> paths = ReadPathsFromExcel(excelFilePath);

            await DownloadFilesFromS3(paths, bucketName, accessKey, secretKey, serviceUrl, outputFolder);

            AnalyzeDownloadedFilesAndUpdateExcel(excelFilePath, outputFolder);

            Console.WriteLine("Всё завершено. Отчёт в input.xlsx");
            Console.ReadKey();
        }

        private static List<string> GetPathsFromDatabase(string connectionString)
        {
            var paths = new List<string>();

            string sql = @"
                SELECT DISTINCT ""DigitalOrders"".""S3ExportPath""
                FROM ""DigitalOrders""
                INNER JOIN ""DigitalOrderItems"" ON ""DigitalOrderItems"".""OrderId"" = ""DigitalOrders"".""Id""
                INNER JOIN ""ProductSources"" ON ""ProductSources"".""GoodId"" = ""DigitalOrderItems"".""GoodId""
                WHERE 
                    ""DigitalOrders"".""BasketRowsIds"" = '{0}'::bigint[]
                    AND NOT EXISTS (
                        SELECT 1 
                        FROM ""SteamOperatorUsageHistory"" 
                        WHERE ""SteamOperatorUsageHistory"".""OrderId"" = ""DigitalOrders"".""Id""
                    )
                    AND ""ProductSources"".""SourceType"" = 'DigitalPay'
                    AND ""DigitalOrders"".""S3ExportPath"" IS NOT NULL 
                    AND ""DigitalOrders"".""S3ExportPath"" != ''
                    AND ""DigitalOrders"".""S3ExportDate"" IS NOT NULL
                ORDER BY ""DigitalOrders"".""S3ExportPath""";

            using (var conn = new NpgsqlConnection(connectionString))
            {
                conn.Open();
                using (var cmd = new NpgsqlCommand(sql, conn))
                {
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            string path = reader.GetString(0).Trim();
                            if (!string.IsNullOrWhiteSpace(path))
                            {
                                paths.Add(path);
                                Console.WriteLine($"Добавлен путь из БД: {path}");
                            }
                        }
                    }
                }
            }

            return paths;
        }

        private static void UpdateExcelWithPaths(string excelFilePath, List<string> paths)
        {
            XLWorkbook workbook;
            if (File.Exists(excelFilePath))
            {
                workbook = new XLWorkbook(excelFilePath);
            }
            else
            {
                workbook = new XLWorkbook();
                workbook.AddWorksheet("Sheet1");
            }

            var worksheet = workbook.Worksheet(1);
            worksheet.Clear();

            worksheet.Cell(1, 1).Value = "№";
            worksheet.Cell(1, 2).Value = "S3 Path";
            worksheet.Row(1).Style.Font.Bold = true;

            for (int i = 0; i < paths.Count; i++)
            {
                worksheet.Cell(i + 2, 1).Value = i + 1;
                worksheet.Cell(i + 2, 2).Value = paths[i];
            }
            worksheet.ColumnsUsed().AdjustToContents();

            workbook.SaveAs(excelFilePath);
            Console.WriteLine($"Excel обновлён: {paths.Count} строк записано.");
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
                int downloaded = 0;
                int skipped = 0;

                foreach (var key in paths)
                {
                    string fileName = Path.GetFileName(key);
                    string localPath = Path.Combine(outputFolder, fileName);

                    if (File.Exists(localPath))
                    {
                        Console.WriteLine($"Уже существует локально (пропуск скачивания): {key}");
                        skipped++;
                        continue;
                    }

                    try
                    {
                        var request = new TransferUtilityDownloadRequest
                        {
                            BucketName = bucketName,
                            Key = key,
                            FilePath = localPath
                        };
                        await transferUtility.DownloadAsync(request);
                        Console.WriteLine($"Скачан: {key} → {localPath}");
                        downloaded++;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Ошибка при скачивании {key}: {ex.Message}");
                    }
                }

                Console.WriteLine($"Скачивание завершено: новых {downloaded}, пропущено {skipped}");
            }
        }

        private static void AnalyzeDownloadedFilesAndUpdateExcel(string excelFilePath, string outputFolder)
        {
            var workbook = new XLWorkbook(excelFilePath);
            var ws = workbook.Worksheet(1);

            ws.Cell(1, 3).Value = "Файл";
            ws.Cell(1, 4).Value = "Brand";
            ws.Cell(1, 5).Value = "Проблемный?";
            ws.Cell(1, 6).Value = "GoodIds";

            ws.Row(1).Style.Font.Bold = true;

            var problemOrders = new List<long>();

            int row = 2;
            while (!ws.Cell(row, 1).IsEmpty())
            {
                string s3Path = ws.Cell(row, 2).GetValue<string>();
                string fileName = Path.GetFileName(s3Path);
                string localPath = Path.Combine(outputFolder, fileName);

                string brand = "Файл не найден";
                string isProblem = "—";
                string goodIds = "—";

                if (File.Exists(localPath))
                {
                    try
                    {
                        string json = File.ReadAllText(localPath);

                        brand = ExtractBrandFromJson(json);

                        goodIds = ExtractGoodIdsFromJson(json);

                        bool wrongBrand = !string.Equals(brand, "Digitalpay", StringComparison.OrdinalIgnoreCase);
                        isProblem = wrongBrand ? "ДА" : "Нет";

                        if (wrongBrand)
                        {
                            long orderId = ExtractOrderIdFromPath(s3Path);
                            problemOrders.Add(orderId);
                            ws.Row(row).Style.Font.FontColor = XLColor.Red;
                        }
                    }
                    catch (Exception ex)
                    {
                        brand = "Ошибка парсинга";
                        isProblem = ex.Message;
                        goodIds = "Ошибка";
                    }
                }

                ws.Cell(row, 3).Value = fileName;
                ws.Cell(row, 4).Value = brand;
                ws.Cell(row, 5).Value = isProblem;
                ws.Cell(row, 6).Value = goodIds;

                row++;
            }

            ws.Columns().AdjustToContents();
            workbook.SaveAs(excelFilePath);

            Console.WriteLine($"\nАнализ завершён. Проблемных заказов: {problemOrders.Count}");
            if (problemOrders.Count > 0)
            {
                Console.WriteLine("Проблемные OrderId:");
                foreach (var id in problemOrders)
                {
                    Console.WriteLine(id);
                }
            }
        }

        private static string ExtractBrandFromJson(string jsonContent)
        {
            try
            {
                var json = JsonNode.Parse(jsonContent);
                var brandNode = json["data"]?[0]?["shipment"]?["brand"];
                return brandNode?.GetValue<string>() ?? "Не найден";
            }
            catch
            {
                return "Ошибка парсинга";
            }
        }

        private static long ExtractOrderIdFromPath(string s3Path)
        {
            string fileName = Path.GetFileNameWithoutExtension(s3Path);
            if (long.TryParse(fileName, out long id))
                return id;
            return 0;
        }

        private static string ExtractGoodIdsFromJson(string jsonContent)
        {
            try
            {
                var json = JsonNode.Parse(jsonContent);
                var lots = json["data"]?[0]?["shipment"]?["lots"] as JsonArray;

                if (lots == null || lots.Count == 0)
                    return "Нет лотов";

                var goodIds = new List<string>();

                foreach (var lot in lots)
                {
                    var merchantGoodsId = lot?["good"]?["merchantGoodsId"]?.GetValue<string>();
                    if (!string.IsNullOrEmpty(merchantGoodsId))
                    {
                        goodIds.Add(merchantGoodsId);
                    }
                }

                return string.Join(", ", goodIds.Distinct());
            }
            catch
            {
                return "Ошибка извлечения";
            }
        }
    }
}