package com;
import java.sql.Statement;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180ParserBuilder;
import com.opencsv.exceptions.CsvValidationException;

public class baseline_eff {

    /* ===================== 1. 数据库连接常量 ===================== */
    private static final String URL   = "jdbc:postgresql://localhost:5432/project1_copy";
    private static final String USER  = "postgres";
    private static final String PWD   = "123456";

    /* ===================== 2. 可调整的测试参数 ===================== */
    private static final int BATCH_SIZE = 3_500; 

    public static void main(String[] args) throws Exception {
        if (args.length != 2) throw new IllegalArgumentException("需要提供 CSV 文件目录和数据库名称两个参数！");
        Path dir = Paths.get(args[0]);

        try (Connection con = DriverManager.getConnection(URL, USER, PWD)) {
            con.setAutoCommit(false);


            loadRecipe(con, dir.resolve("recipes_checktime.csv"));


            con.commit();
            System.out.println("===== 全部表导入完成 =====");
        }
    }

    /* =================================================================
       recipes.csv -> table Recipe
       ================================================================= */
    private static void loadRecipe(Connection con, Path file) throws IOException, SQLException {
        
        // 1. 集成清空表的操作
        System.out.println("===== 准备清空 Recipe 表 =====");
        try (Statement stmt = con.createStatement()) {
            stmt.execute("TRUNCATE TABLE recipe RESTART IDENTITY CASCADE;");
        }
        con.commit(); 
        System.out.println("===== Recipe 表已清空，数据导入即将开始 =====");

        // 2.  SQL 语句
        String recSql = "INSERT INTO recipe (id, name, author_id, date_published, description, category, aggregated_rating, review_count, instructions) "
                      + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

        // 3. 添加性能测量变量
        long startTime = System.currentTimeMillis();
        int totalRows = 0;

        try (
                PreparedStatement psRec = con.prepareStatement(recSql);
                CSVReader reader = new CSVReaderBuilder(Files.newBufferedReader(file))
                     .withCSVParser(new RFC4180ParserBuilder().build())
                     .build()
             ) {
            
            reader.readNext(); // 跳过表头
            String[] r;

            while ((r = reader.readNext()) != null) {
                if (r.length == 0) continue;
                try {
                    // 解析 CSV 行数据
                    int recipeId = Integer.parseInt(r[0]);
                    String name = nullify(r[1]);
                    int authorId = Integer.parseInt(r[2]);
                    LocalDateTime datePublished = parseTimestamp(r[7]);
                    String description = nullify(r[8]);
                    String recipeCategory = nullify(r[9]);
                    double aggregatedRating = parseDoubleOr0(r[12]);
                    int reviewCount = parseIntOr0(r[13]);
                    String recipeInstructions = nullify(r[25]);

                    // 设置 PreparedStatement 参数
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

                    totalRows++;

                    // 4. 使用 BATCH_SIZE 常量，并移除内部 commit
                    if (totalRows % BATCH_SIZE == 0) {
                        psRec.executeBatch();
                        System.out.println("Recipe 表已缓存 " + totalRows + " 行数据...");
                    }

                } catch (Exception e) { 
                    // 5. 增强错误报告
                    System.err.println("处理行 " + totalRows + " 时出错: " + e.getMessage() + " | 行数据: " + String.join(",", r));
                }
            }
            
            // 提交剩余的批处理
            psRec.executeBatch();
            con.commit();

            // 6. 添加完整的性能报告
            long endTime = System.currentTimeMillis();
            double elapsedTimeInSeconds = (endTime - startTime) / 1000.0;
            double rowsPerSecond = (elapsedTimeInSeconds > 0) ? (totalRows / elapsedTimeInSeconds) : 0;

            System.out.println("========================================================");
            System.out.println("Recipe 表导入完成 (基准测试)");
            System.out.println("--------------------------------------------------------");
            System.out.printf("批处理大小 (Batch Size): %,d\n", BATCH_SIZE);
            System.out.printf("总计导入行数: %,d\n", totalRows);
            System.out.printf("总耗时: %.2f 秒\n", elapsedTimeInSeconds);
            System.out.printf("平均导入速率: %,.2f 行/秒\n", rowsPerSecond);
            System.out.println("========================================================");

        } catch (CsvValidationException | IOException | SQLException e) {
            con.rollback(); // 如果在批处理或提交时出错，回滚整个事务
            throw new IOException("CSV 解析或数据库操作失败", e);
        }
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
    
    // 辅助方法需要 Instant 类，所以需要 import
    private static LocalDateTime parseTimestamp(String s) {
        try {
            // 确保 java.time.Instant 被导入
            return LocalDateTime.ofInstant(java.time.Instant.parse(s), java.time.ZoneOffset.UTC);
        } catch (Exception e) {
            return null;
        }
    }
}