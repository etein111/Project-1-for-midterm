package com;

import java.sql.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180ParserBuilder;

public class BaselineMultiThread {

    /* ===================== 1. 数据库连接常量 ===================== */
    private static final String URL   = "jdbc:postgresql://localhost:5432/project1_copy";
    private static final String USER  = "postgres";
    private static final String PWD   = "123456";

    /* ===================== 2. 可调整的测试参数 ===================== */
    private static final int BATCH_SIZE = 3_500; // 批处理大小
    private static final int NUM_THREADS = 2;   // 线程数

    /* ===================== 3. 列索引映射 ===================== */
    private static final int[] TARGET_INDICES = {0, 1, 2, 7, 8, 9, 12, 13, 25};

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("需要提供 CSV 文件目录参数！");
        }
        Path dir = Paths.get(args[0]);
        Path sourceCsvFile = dir.resolve("recipes_checktime.csv");

        if (!Files.exists(sourceCsvFile)) {
            System.err.println("文件不存在: " + sourceCsvFile);
            return;
        }

        // 1. 清空目标表
        System.out.println("===== 准备清空 Recipe 表 =====");
        try (Connection con = DriverManager.getConnection(URL, USER, PWD);
             Statement stmt = con.createStatement()) {
            stmt.execute("TRUNCATE TABLE recipe RESTART IDENTITY CASCADE;");
        }
        System.out.println("===== Recipe 表已清空 =====");

        // 2. 分割文件
        long startTime = System.currentTimeMillis();
        long split_start = System.currentTimeMillis();
        List<Path> parts = preprocessAndSplit(sourceCsvFile, NUM_THREADS);
        System.out.println("文件分割完成：共 " + parts.size() + " 个分片");
        long split_end = System.currentTimeMillis();

        // 3. 多线程导入
        ExecutorService pool = Executors.newFixedThreadPool(NUM_THREADS);
        AtomicLong totalRows = new AtomicLong(0);
        List<Future<ImportResult>> futures = new ArrayList<>();

        for (int i = 0; i < parts.size(); i++) {
            Path part = parts.get(i);
            int threadId = i;
            futures.add(pool.submit(() -> importPartition(part, threadId)));
        }

        // 4. 汇总结果
        long successfulRows = 0;
        long failedRows = 0;
        for (Future<ImportResult> future : futures) {
            try {
                ImportResult result = future.get();
                successfulRows += result.successfulRows;
                failedRows += result.failedRows;
                totalRows.addAndGet(result.successfulRows);
                System.out.printf("线程 %d 完成: 成功 %,d 行, 失败 %,d 行, 耗时 %.2f 秒%n",
                        result.threadId, result.successfulRows, result.failedRows, result.elapsedSeconds);
            } catch (Exception e) {
                System.err.println("线程执行异常: " + e.getMessage());
            }
        }

        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.MINUTES);

        // 5. 清理临时文件
        for (Path part : parts) {
            try {
                Files.deleteIfExists(part);
            } catch (Exception ignored) {}
        }

        // 6. 总体性能报告
        long endTime = System.currentTimeMillis();
        double totalElapsed = (endTime - startTime) / 1000.0;
        double split_time = (split_end - split_start) / 1000.0;
        double import_time = (System.currentTimeMillis() - split_end) / 1000.0;
        double overallRate = totalElapsed > 0 ? totalRows.get() / totalElapsed : 0;

        System.out.println("\n========================================================");
        System.out.println("Baseline 多线程导入完成");
        System.out.println("--------------------------------------------------------");
        System.out.printf("线程数: %,d\n", NUM_THREADS);
        System.out.printf("批处理大小: %,d\n", BATCH_SIZE);
        System.out.printf("总计导入行数: %,d (成功: %,d, 失败: %,d)%n", 
                totalRows.get(), successfulRows, failedRows);
        System.out.printf("总耗时: %.2f 秒\n", totalElapsed);
        System.out.printf("Split_time:%.2f\n", split_time);
        System.out.printf("Import_time:%.2f\n", import_time);
        System.out.printf("平均导入速率: %,.2f 行/秒\n", overallRate);
        System.out.println("========================================================");
    }

    /**
     * 导入单个文件分片（baseline_eff 的批处理逻辑）
     */
    private static ImportResult importPartition(Path partFile, int threadId) {
        long threadStart = System.currentTimeMillis();
        long successfulCount = 0;
        long failedCount = 0;

        String recSql = "INSERT INTO recipe (id, name, author_id, date_published, description, category, aggregated_rating, review_count, instructions) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (Connection con = DriverManager.getConnection(URL, USER, PWD);
             PreparedStatement psRec = con.prepareStatement(recSql);
             CSVReader reader = new CSVReaderBuilder(Files.newBufferedReader(partFile))
                     .withCSVParser(new RFC4180ParserBuilder().build())
                     .build()) {

            con.setAutoCommit(false);
            reader.readNext(); // 跳过表头
            String[] r;
            int localRowNum = 0;
            int localBatchCount = 0;

            while ((r = reader.readNext()) != null) {
                if (r.length == 0) continue;
                localRowNum++;

                try {
                    // 解析 CSV 行数据
                    int recipeId = Integer.parseInt(r[0]);
                    String name = nullify(r[1]);
                    int authorId = Integer.parseInt(r[2]);
                    LocalDateTime datePublished = parseTimestamp(r[3]);
                    String description = nullify(r[4]);
                    String recipeCategory = nullify(r[5]);
                    double aggregatedRating = parseDoubleOr0(r[6]);
                    int reviewCount = parseIntOr0(r[7]);
                    String recipeInstructions = nullify(r[8]);

                    // 设置参数
                    psRec.setInt(1, recipeId);
                    psRec.setString(2, name);
                    psRec.setInt(3, authorId);
                    psRec.setObject(4, datePublished);
                    psRec.setString(5, description);
                    psRec.setString(6, recipeCategory);
                    psRec.setDouble(7, aggregatedRating);
                    psRec.setInt(8, reviewCount);
                    psRec.setString(9, recipeInstructions);
                    psRec.addBatch();

                    localBatchCount++;
                    successfulCount++;

                    // 执行批处理
                    if (localBatchCount % BATCH_SIZE == 0) {
                        psRec.executeBatch();
                        System.out.printf("线程 %d: 已处理 %,d 行%n", threadId, successfulCount);
                    }

                } catch (Exception e) {
                    // 错误报告
                    System.err.printf("线程 %d - 行 %d 错误: %s | 数据: %s%n",
                            threadId, localRowNum, e.getMessage(), String.join(",", r));
                    failedCount++;
                }
            }

            // 提交剩余批处理
            psRec.executeBatch();
            con.commit();

        } catch (Exception e) {
            System.err.printf("线程 %d 致命错误: %s%n", threadId, e.getMessage());
            failedCount += successfulCount; // 估算失败数
            successfulCount = 0;
        }

        long threadEnd = System.currentTimeMillis();
        double elapsed = (threadEnd - threadStart) / 1000.0;

        return new ImportResult(threadId, successfulCount, failedCount, elapsed);
    }

    /**
     * 预处理并分割 CSV 文件（保持列筛选）
     */
    private static List<Path> preprocessAndSplit(Path source, int numParts) throws IOException {
        List<String[]> allRows = new ArrayList<>();
        String[] header = null;

        try (CSVReader reader = new CSVReaderBuilder(Files.newBufferedReader(source))
                .withCSVParser(new RFC4180ParserBuilder().build())
                .build()) {

            String[] row;
            boolean isFirst = true;
            while ((row = reader.readNext()) != null) {
                if (isFirst) {
                    header = row;
                    isFirst = false;
                } else {
                    allRows.add(row);
                }
            }
        } catch (Exception e) {
            throw new IOException("CSV 读取失败", e);
        }

        if (allRows.isEmpty() || header == null) {
            return Collections.singletonList(source);
        }

        // 生成目标表头（映射后的列名）
        String[] targetHeader = new String[TARGET_INDICES.length];
        for (int i = 0; i < TARGET_INDICES.length; i++) {
            targetHeader[i] = header[TARGET_INDICES[i]];
        }

        int rowsPerPart = allRows.size() / numParts + (allRows.size() % numParts == 0 ? 0 : 1);
        List<Path> result = new ArrayList<>();

        for (int i = 0; i < numParts; i++) {
            Path partFile = source.getParent().resolve(source.getFileName() + ".baseline.part" + i);
            try (BufferedWriter bw = Files.newBufferedWriter(partFile)) {
                // 写入映射后的表头
                writeCsvLine(bw, targetHeader, ',');
                
                int start = i * rowsPerPart;
                int end = Math.min(start + rowsPerPart, allRows.size());
                for (int j = start; j < end; j++) {
                    String[] row = allRows.get(j);
                    String[] targetRow = new String[TARGET_INDICES.length];
                    for (int k = 0; k < TARGET_INDICES.length; k++) {
                        int idx = TARGET_INDICES[k];
                        targetRow[k] = idx < row.length ? row[idx] : "";
                    }
                    writeCsvLine(bw, targetRow, ',');
                }
            }
            result.add(partFile);
        }
        return result;
    }

    /**
     * 安全写入 CSV 行
     */
    private static void writeCsvLine(BufferedWriter bw, String[] fields, char sep) throws IOException {
        for (int i = 0; i < fields.length; i++) {
            String f = fields[i] == null ? "" : fields[i];
            boolean quote = f.contains("\"") || f.contains(String.valueOf(sep)) || 
                           f.contains("\n") || f.contains("\r");
            if (quote) {
                bw.write('"');
                bw.write(f.replace("\"", "\"\""));
                bw.write('"');
            } else {
                bw.write(f);
            }
            if (i < fields.length - 1) bw.write(sep);
        }
        bw.newLine();
    }

    // ===================== 辅助方法 =====================
    private static String nullify(String s) {
        return (s == null || s.isEmpty() || s.equalsIgnoreCase("NULL")) ? null : s;
    }

    private static int parseIntOr0(String s) {
        try {
            return Integer.parseInt(nullify(s));
        } catch (NumberFormatException | NullPointerException e) {
            return 0;
        }
    }

    private static double parseDoubleOr0(String s) {
        try {
            return Double.parseDouble(nullify(s));
        } catch (NumberFormatException | NullPointerException e) {
            return 0.0;
        }
    }

    private static LocalDateTime parseTimestamp(String s) {
        try {
            return LocalDateTime.ofInstant(java.time.Instant.parse(s), java.time.ZoneOffset.UTC);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 记录每个线程的导入结果
     */
    private static class ImportResult {
        final int threadId;
        final long successfulRows;
        final long failedRows;
        final double elapsedSeconds;

        ImportResult(int threadId, long successfulRows, long failedRows, double elapsedSeconds) {
            this.threadId = threadId;
            this.successfulRows = successfulRows;
            this.failedRows = failedRows;
            this.elapsedSeconds = elapsedSeconds;
        }
    }
}