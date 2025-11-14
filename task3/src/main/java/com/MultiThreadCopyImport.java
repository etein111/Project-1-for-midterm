package com;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180ParserBuilder;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class MultiThreadCopyImport {

    private static final String URL = "jdbc:postgresql://localhost:5432/project1_copy";
    private static final String USER = "postgres";
    private static final String PWD = "123456";
    private static final int NUM_THREADS = 1;

    private static final int[] TARGET_INDICES = {
        0, 1, 2, 7, 8, 9, 12, 13, 25
    };

    private static final String[] TARGET_COLUMNS = {
        "id", "name", "author_id", "date_published", "description",
        "category", "aggregated_rating", "review_count", "instructions"
    };

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("用法: java MultiThreadImport /path/to/csv/dir");
            return;
        }

        Path sourceDir = Paths.get(args[0]);
        Path sourceCsvFile = sourceDir.resolve("recipes_checktime.csv");

        if (!Files.exists(sourceCsvFile)) {
            System.err.println("文件不存在: " + sourceCsvFile);
            return;
        }

        try (Connection conn = DriverManager.getConnection(URL, USER, PWD);
             Statement stmt = conn.createStatement()) {
            stmt.execute("TRUNCATE TABLE recipe RESTART IDENTITY CASCADE;");
            System.out.println("表已清空");
        }

        long start = System.currentTimeMillis();
        AtomicLong totalRows = new AtomicLong(0);

        long split_start = System.currentTimeMillis();
        List<Path> parts = preprocessAndSplit(sourceCsvFile, NUM_THREADS);
        System.out.println("分割完成：" + parts.size() + " 个文件");
        long split_end = System.currentTimeMillis();

        ExecutorService pool = Executors.newFixedThreadPool(NUM_THREADS);
        for (Path part : parts) {
            pool.submit(() -> {
                try (Connection conn = DriverManager.getConnection(URL, USER, PWD)) {
                    conn.setAutoCommit(false);
                    long rows = copyImport(conn, part);
                    totalRows.addAndGet(rows);
                    conn.commit();
                } catch (Exception e) {
                    System.err.println("导入失败: " + part.getFileName());
                    e.printStackTrace();
                }
            });
        }

        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.MINUTES);

        double elapsed = (System.currentTimeMillis() - start) / 1000.0;
        double split_time = (split_end - split_start) / 1000.0;
        double import_time = (System.currentTimeMillis() - split_end) / 1000.0;
        System.out.printf("%n导入完成: %,d 行 | %.2f 秒 | %,.0f 行/秒%n\nSplit_time:%.2f, Import_time:%.2f",
                totalRows.get(), elapsed, totalRows.get() / elapsed, split_time, import_time);

        parts.forEach(p -> {
            try { Files.deleteIfExists(p); } catch (Exception ignored) {}
        });
    }

    /** 使用 CSVReader 自动处理多行字段 */
    private static List<Path> preprocessAndSplit(Path source, int parts) throws IOException {
        List<String[]> allRows = new ArrayList<>();

        try (CSVReader reader = new CSVReaderBuilder(Files.newBufferedReader(source))
                .withCSVParser(new RFC4180ParserBuilder().withSeparator(',').build())
                .build()) {

            String[] row;
            while ((row = reader.readNext()) != null) {
                allRows.add(row);
            }
        } catch (Exception e) {
            throw new IOException("CSV 读取失败", e);
        }

        if (allRows.isEmpty()) {
            return Collections.singletonList(source);
        }

        allRows.remove(0); // 移除表头
        int dataRows = allRows.size();
        int rowsPerPart = dataRows / parts + (dataRows % parts == 0 ? 0 : 1);

        List<Path> result = new ArrayList<>();
        for (int i = 0; i < parts; i++) {
            Path partFile = source.getParent().resolve(source.getFileName() + ".clean.part" + i);
            try (BufferedWriter bw = Files.newBufferedWriter(partFile)) {
                writeCsvLine(bw, TARGET_COLUMNS, ',');

                int start = i * rowsPerPart;
                int end = Math.min(start + rowsPerPart, dataRows);
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

    /** 安全写 CSV 行 */
    private static void writeCsvLine(BufferedWriter bw, String[] fields, char sep) throws IOException {
        for (int i = 0; i < fields.length; i++) {
            String f = fields[i] == null ? "" : fields[i];
            boolean quote = f.contains("\"") || f.contains(String.valueOf(sep)) || f.contains("\n") || f.contains("\r");
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

    /** COPY 导入 */
    private static long copyImport(Connection conn, Path file) throws SQLException, IOException {
        CopyManager cm = new CopyManager((BaseConnection) conn);
        String sql = "COPY recipe (id, name, author_id, date_published, description, category, aggregated_rating, review_count, instructions) " +
                     "FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '\"', ESCAPE '\"')";

        try (FileReader fr = new FileReader(file.toFile())) {
            return cm.copyIn(sql, fr);
        }
    }
}