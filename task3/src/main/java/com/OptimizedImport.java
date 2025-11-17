package com;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import java.sql.*;
import java.nio.file.*;
import java.io.*;
import java.util.*;
import java.time.*;
import java.util.stream.*;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180ParserBuilder;
import com.opencsv.exceptions.CsvValidationException;

public class OptimizedImport {

    /* ===================== 连接常量 ===================== */
    private static final String URL = "jdbc:postgresql://localhost:5432/project_midterm";
    private static final String USER = "postgres";
    private static final String PWD = "123456";

    /* ===================== 主入口 ===================== */
    public static void main(String[] args) throws Exception {
        if (args.length != 1) throw new IllegalArgumentException("需要提供 CSV 文件目录");
        Path dir = Paths.get(args[0]);

        try (Connection con = DriverManager.getConnection(URL, USER, PWD)) {
            CopyManager copyManager = new CopyManager(con.unwrap(BaseConnection.class));
            con.setAutoCommit(false);
            clearDatabase(con);
            
            long StartTime = System.currentTimeMillis();
            loadUser(copyManager, dir.resolve("user_fixed.csv"));
            con.commit();
            loadFollows(copyManager, dir.resolve("user_fixed.csv"));
            con.commit();
            loadRecipe(copyManager, dir.resolve("recipes_checktime.csv"));
            con.commit();
            loadIngredient(copyManager, dir.resolve("recipes_checktime.csv"));
            con.commit();
            loadNutritionalInformation(copyManager, dir.resolve("recipes_checktime.csv"));
            con.commit();
            loadServingInformation(copyManager, dir.resolve("recipes_checktime.csv"));
            con.commit();
            loadKeyword(copyManager, dir.resolve("recipes_checktime.csv"));
            con.commit();
            loadFavorite(copyManager, dir.resolve("recipes_checktime.csv"));
            con.commit();
            loadTimeInformation(copyManager, dir.resolve("recipes_checktime.csv"));
            con.commit();
            loadReview(copyManager, dir.resolve("reviews_recipeid2int.csv"));
            con.commit();
            loadLike(copyManager, dir.resolve("reviews_recipeid2int.csv"));
            con.commit();
            long EndTime = System.currentTimeMillis();
            System.out.println("===== 全部表导入完成 =====");
            double TotalTime = (EndTime - StartTime) / 1000.0;
            System.out.printf("TotalTime:%.2f", TotalTime);
        }
    }

        private static void clearDatabase(Connection con) throws SQLException {
        System.out.println("===== 准备清空所有相关的表... =====");
        
        // 将您所有需要清空的表名放在这个列表里
        // 注意：顺序很重要！先清空依赖别人的表，再清空被依赖的表。
        List<String> tableNames = Arrays.asList(
            "\"Like\"", "Favorite", "Recipe_Keyword", "Recipe_Ingredient",
            "Serving_Information", "Nutritional_Information", "Time_Information",
            "Review", "Follows", "Recipe", "Keyword", "Ingredient", "\"User\""
        );

        try (Statement stmt = con.createStatement()) {
            for (String tableName : tableNames) {
                // TRUNCATE ... RESTART IDENTITY CASCADE;
                // TRUNCATE: 快速删除所有行
                // RESTART IDENTITY: 重置自增 ID (如 SERIAL) 的计数器
                // CASCADE: 级联清空依赖此表的其他表
                stmt.execute("TRUNCATE TABLE " + tableName + " RESTART IDENTITY CASCADE;");
            }
        }
        con.commit(); // 立即提交清空操作
        System.out.println("===== 所有相关的表已清空 =====");
    }

    /* =================================================================
        user.csv -> table User
       ================================================================= */
    private static void loadUser(CopyManager copyManager, Path file) throws IOException, SQLException {
        String sql = "COPY \"User\" (id,name,gender,age,followers_count,following_count) FROM STDIN CSV";
        Path tempFile = Files.createTempFile("user_import_", ".csv");
        
        try (BufferedWriter writer = Files.newBufferedWriter(tempFile);
             CSVReader reader = new CSVReaderBuilder(Files.newBufferedReader(file))
                     .withCSVParser(new RFC4180ParserBuilder().build())
                     .build()) {
            
            String[] header = reader.readNext();
            String[] r;
            int lineCnt = 0;
            
            while ((r = reader.readNext()) != null) {
                if (r.length == 0) continue;
                try {
                    List<String> fields = Arrays.asList(
                        r[0],                                   // id
                        nullifyCsv(r[1]),                       // name
                        nullifyCsv(r[2]),                       // gender
                        r[3].isBlank() ? "" : r[3],            // age
                        r[4].isBlank() ? "" : r[4],            // followers_count
                        r[5].isBlank() ? "" : r[5]             // following_count
                    );
                    writeCsvRow(writer, fields);
                    lineCnt++;
                    
                    if (lineCnt % 10_000 == 0) {
                        writer.flush();
                        System.out.println("User 表已处理 " + lineCnt + " 行数据...");
                    }
                } catch (Exception e) {
                    System.err.println("跳过行: " + Arrays.toString(r) + " - " + e.getMessage());
                }
            }
            writer.flush();
            System.out.println("User 临时文件写入完成，共 " + lineCnt + " 行");
            
            // 执行COPY
            try (Reader fileReader = Files.newBufferedReader(tempFile)) {
                copyManager.copyIn(sql, fileReader);
            }
            System.out.println("User 表导入完成");
            
        } catch (CsvValidationException e) {
            throw new IOException("CSV 解析失败", e);
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    /* =================================================================
        user.csv -> table Follows
       ================================================================= */
    private static void loadFollows(CopyManager copyManager, Path file) throws IOException, SQLException {
        String sql = "COPY Follows (follower_id,following_id) FROM STDIN CSV";
        Path tempFile = Files.createTempFile("follows_import_", ".csv");
        
        try (BufferedWriter writer = Files.newBufferedWriter(tempFile);
             CSVReader reader = new CSVReaderBuilder(Files.newBufferedReader(file))
                     .withCSVParser(new RFC4180ParserBuilder().build())
                     .build()) {
            
            String[] header = reader.readNext();
            String[] r;
            int lineCnt = 0;
            int relCnt = 0;
            Set<String> uniquePairs = new LinkedHashSet<>();
            
            while ((r = reader.readNext()) != null) {
                if (r.length == 0) continue;
                try {
                    int authorId = Integer.parseInt(r[0]);
                    
                    // 作者关注的所有人
                    Set<String> followingSet = splitParts(r[7]);
                    for (String uid : followingSet) {
                        String pairKey = authorId + "," + uid; // 创建唯一键
                        if (uniquePairs.add(pairKey)) { // 添加成功说明是新关系
                            writeCsvRow(writer, Arrays.asList(String.valueOf(authorId), uid));
                            relCnt++;
                        }
                    }
                    
                    // 所有关注作者的人
                    Set<String> followerSet = splitParts(r[6]);
                    for (String uid : followerSet) {
                        String pairKey = uid + "," + authorId; // 创建唯一键
                        if (uniquePairs.add(pairKey)) { // 添加成功说明是新关系
                            writeCsvRow(writer, Arrays.asList(uid, String.valueOf(authorId)));
                            relCnt++;
                        }
                    }
                    
                    lineCnt++;
                    if (lineCnt % 1_000 == 0) {
                        writer.flush();
                        System.out.println("Follows 表已处理 " + lineCnt + " 行主数据，生成 " + relCnt + " 条唯一关系...");
                    }
                } catch (Exception e) {
                    System.err.println("跳过行: " + Arrays.toString(r) + " - " + e.getMessage());
                }
            }
            writer.flush();
            System.out.println("Follows 临时文件写入完成，共 " + relCnt + " 条唯一关系");
            
            try (Reader fileReader = Files.newBufferedReader(tempFile)) {
                copyManager.copyIn(sql, fileReader);
            }
            System.out.println("Follows 表导入完成");
            
        } catch (CsvValidationException e) {
            throw new IOException("CSV 解析失败", e);
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    /* =================================================================
       recipes.csv -> table recipe
       ================================================================= */
    private static void loadRecipe(CopyManager copyManager, Path file) throws IOException, SQLException {
        String sql = "COPY Recipe (id,name,author_id,date_published,description,category,aggregated_rating,review_count,instructions) FROM STDIN CSV";
        Path tempFile = Files.createTempFile("recipe_import_", ".csv");
        
        try (BufferedWriter writer = Files.newBufferedWriter(tempFile);
             CSVReader reader = new CSVReaderBuilder(Files.newBufferedReader(file))
                     .withCSVParser(new RFC4180ParserBuilder().build())
                     .build()) {
            
            String[] header = reader.readNext();
            String[] r;
            int lineCnt = 0;
            
            while ((r = reader.readNext()) != null) {
                if (r.length == 0) continue;
                try {
                    String datePublished = r[7].isBlank() ? "" : formatTimestamp(r[7]);
                    List<String> fields = Arrays.asList(
                        r[0],                                   // id
                        nullifyCsv(r[1]),                       // name
                        r[2],                                   // author_id
                        datePublished,                          // date_published
                        nullifyCsv(r[8]),                       // description
                        nullifyCsv(r[9]),                       // category
                        r[12].isBlank() ? "0" : r[12],         // aggregated_rating
                        r[13].isBlank() ? "0" : r[13],         // review_count
                        nullifyCsv(r[25])                       // instructions
                    );
                    writeCsvRow(writer, fields);
                    lineCnt++;
                    
                    if (lineCnt % 10_000 == 0) {
                        writer.flush();
                        System.out.println("Recipe 表已处理 " + lineCnt + " 行数据...");
                    }
                } catch (Exception e) {
                    System.err.println("跳过行: " + Arrays.toString(r) + " - " + e.getMessage());
                }
            }
            writer.flush();
            System.out.println("Recipe 临时文件写入完成，共 " + lineCnt + " 行");
            
            try (Reader fileReader = Files.newBufferedReader(tempFile)) {
                copyManager.copyIn(sql, fileReader);
            }
            System.out.println("Recipe 表导入完成");
            
        } catch (CsvValidationException e) {
            throw new IOException("CSV 解析失败", e);
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    /* =================================================================
       recipes.csv -> table ingredient, recipe_ingredient
       ================================================================= */
    private static void loadIngredient(CopyManager copyManager, Path file) throws IOException, SQLException {
        String ingSql = "COPY Ingredient (id,name) FROM STDIN CSV";
        String recIngSql = "COPY Recipe_Ingredient (recipe_id,ingredient_id) FROM STDIN CSV";
        
        Path ingTempFile = Files.createTempFile("ingredient_import_", ".csv");
        Path recIngTempFile = Files.createTempFile("recipe_ingredient_import_", ".csv");
        
        try (BufferedWriter ingWriter = Files.newBufferedWriter(ingTempFile);
             BufferedWriter recIngWriter = Files.newBufferedWriter(recIngTempFile);
             CSVReader reader = new CSVReaderBuilder(Files.newBufferedReader(file))
                     .withCSVParser(new RFC4180ParserBuilder().build())
                     .build()) {
            
            String[] header = reader.readNext();
            String[] r;
            int lineCnt = 0;
            int ingRelCnt = 0;
            
            while ((r = reader.readNext()) != null) {
                if (r.length == 0) continue;
                try {
                    int recipeId = Integer.parseInt(r[0]);
                    Set<String> ingredientNames = splitParts(r[11]);
                    Set<String> usedIngredientsThisRecipe = new HashSet<>();
                    
                    for (String name : ingredientNames) {
                        if (name.isBlank() || !usedIngredientsThisRecipe.add(name)) continue;
                        
                        // 处理Ingredient表
                        if (!ING_CACHE.containsKey(name)) {
                            int ingredientId = ING_CACHE.size() + 1;
                            writeCsvRow(ingWriter, Arrays.asList(String.valueOf(ingredientId), escapeCsv(name)));
                            ING_CACHE.put(name, ingredientId);
                        }
                        
                        // 处理Recipe_Ingredient表
                        int ingredientId = ING_CACHE.get(name);
                        writeCsvRow(recIngWriter, Arrays.asList(String.valueOf(recipeId), String.valueOf(ingredientId)));
                        ingRelCnt++;
                    }
                    
                    lineCnt++;
                    if (lineCnt % 5_000 == 0) {
                        ingWriter.flush();
                        recIngWriter.flush();
                        System.out.println("Ingredient 关系已处理 " + lineCnt + " 行主数据...");
                    }
                } catch (Exception e) {
                    System.err.println("跳过行: " + Arrays.toString(r) + " - " + e.getMessage());
                }
            }
            ingWriter.flush();
            recIngWriter.flush();
            System.out.println("Ingredient 临时文件写入完成");
            
            // 导入Ingredient表
            try (Reader ingReader = Files.newBufferedReader(ingTempFile)) {
                copyManager.copyIn(ingSql, ingReader);
            }
            System.out.println("Ingredient 表导入完成");
            
            // 导入Recipe_Ingredient表
            try (Reader recIngReader = Files.newBufferedReader(recIngTempFile)) {
                copyManager.copyIn(recIngSql, recIngReader);
            }
            System.out.println("Recipe_Ingredient 表导入完成，共 " + ingRelCnt + " 条关系");
            
        } catch (CsvValidationException e) {
            throw new IOException("CSV 解析失败", e);
        } finally {
            Files.deleteIfExists(ingTempFile);
            Files.deleteIfExists(recIngTempFile);
        }
    }

    /* =================================================================
       recipes.csv -> table nutritional_information
       ================================================================= */
    private static void loadNutritionalInformation(CopyManager copyManager, Path file) throws IOException, SQLException {
        String sql = "COPY Nutritional_Information (recipe_id,calories,fat_content,saturated_fat,cholesterol,sodium,carbohydrate,fiber,sugar,protein) FROM STDIN CSV";
        Path tempFile = Files.createTempFile("nutritional_import_", ".csv");
        
        try (BufferedWriter writer = Files.newBufferedWriter(tempFile);
             CSVReader reader = new CSVReaderBuilder(Files.newBufferedReader(file))
                     .withCSVParser(new RFC4180ParserBuilder().build())
                     .build()) {
            
            String[] header = reader.readNext();
            String[] r;
            int lineCnt = 0;
            
            while ((r = reader.readNext()) != null) {
                if (r.length == 0) continue;
                try {
                    List<String> fields = Arrays.asList(
                        r[0],                                   // recipe_id
                        r[14].isBlank() ? "0" : r[14],         // calories
                        r[15].isBlank() ? "0" : r[15],         // fat_content
                        r[16].isBlank() ? "0" : r[16],         // saturated_fat
                        r[17].isBlank() ? "0" : r[17],         // cholesterol
                        r[18].isBlank() ? "0" : r[18],         // sodium
                        r[19].isBlank() ? "0" : r[19],         // carbohydrate
                        r[20].isBlank() ? "0" : r[20],         // fiber
                        r[21].isBlank() ? "0" : r[21],         // sugar
                        r[22].isBlank() ? "0" : r[22]          // protein
                    );
                    writeCsvRow(writer, fields);
                    lineCnt++;
                    
                    if (lineCnt % 10_000 == 0) {
                        writer.flush();
                        System.out.println("NutritionalInformation 已处理 " + lineCnt + " 行...");
                    }
                } catch (Exception e) {
                    System.err.println("跳过行: " + Arrays.toString(r) + " - " + e.getMessage());
                }
            }
            writer.flush();
            System.out.println("NutritionalInformation 临时文件写入完成，共 " + lineCnt + " 行");
            
            try (Reader fileReader = Files.newBufferedReader(tempFile)) {
                copyManager.copyIn(sql, fileReader);
            }
            System.out.println("NutritionalInformation 表导入完成");
            
        } catch (CsvValidationException e) {
            throw new IOException("CSV 解析失败", e);
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    /* =================================================================
       recipes.csv -> table serving_information
       ================================================================= */
    private static void loadServingInformation(CopyManager copyManager, Path file) throws IOException, SQLException {
        String sql = "COPY Serving_Information (recipe_id,servings,yield) FROM STDIN CSV";
        Path tempFile = Files.createTempFile("serving_import_", ".csv");
        
        try (BufferedWriter writer = Files.newBufferedWriter(tempFile);
             CSVReader reader = new CSVReaderBuilder(Files.newBufferedReader(file))
                     .withCSVParser(new RFC4180ParserBuilder().build())
                     .build()) {
            
            String[] header = reader.readNext();
            String[] r;
            int lineCnt = 0;
            
            while ((r = reader.readNext()) != null) {
                if (r.length == 0) continue;
                try {
                    List<String> fields = Arrays.asList(
                        r[0],                                   // recipe_id
                        r[23].isBlank() ? "0" : r[23],         // servings
                        nullifyCsv(r[24])                       // yield
                    );
                    writeCsvRow(writer, fields);
                    lineCnt++;
                    
                    if (lineCnt % 10_000 == 0) {
                        writer.flush();
                        System.out.println("ServingInformation 已处理 " + lineCnt + " 行...");
                    }
                } catch (Exception e) {
                    System.err.println("跳过行: " + Arrays.toString(r) + " - " + e.getMessage());
                }
            }
            writer.flush();
            System.out.println("ServingInformation 临时文件写入完成，共 " + lineCnt + " 行");
            
            try (Reader fileReader = Files.newBufferedReader(tempFile)) {
                copyManager.copyIn(sql, fileReader);
            }
            System.out.println("ServingInformation 表导入完成");
            
        } catch (CsvValidationException e) {
            throw new IOException("CSV 解析失败", e);
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    /* =================================================================
       recipes.csv -> table keyword, recipe_ingredient
       ================================================================= */
    private static void loadKeyword(CopyManager copyManager, Path file) throws IOException, SQLException {
        String keySql = "COPY Keyword (id,text) FROM STDIN CSV";
        String recKeySql = "COPY Recipe_Keyword (recipe_id,keyword_id) FROM STDIN CSV";
        
        Path keyTempFile = Files.createTempFile("keyword_import_", ".csv");
        Path recKeyTempFile = Files.createTempFile("recipe_keyword_import_", ".csv");
        
        try (BufferedWriter keyWriter = Files.newBufferedWriter(keyTempFile);
             BufferedWriter recKeyWriter = Files.newBufferedWriter(recKeyTempFile);
             CSVReader reader = new CSVReaderBuilder(Files.newBufferedReader(file))
                     .withCSVParser(new RFC4180ParserBuilder().build())
                     .build()) {
            
            String[] header = reader.readNext();
            String[] r;
            int lineCnt = 0;
            int keyRelCnt = 0;
            
            while ((r = reader.readNext()) != null) {
                if (r.length == 0) continue;
                try {
                    int recipeId = Integer.parseInt(r[0]);
                    Set<String> keywordsSet = splitParts(r[10]);
                    Set<String> usedKeywordsThisRecipe = new HashSet<>();
                    
                    for (String keyword : keywordsSet) {
                        if (keyword.isBlank() || !usedKeywordsThisRecipe.add(keyword)) continue;
                        
                        // 处理Keyword表
                        if (!KEY_CACHE.containsKey(keyword)) {
                            int keywordId = KEY_CACHE.size() + 1;
                            writeCsvRow(keyWriter, Arrays.asList(String.valueOf(keywordId), escapeCsv(keyword)));
                            KEY_CACHE.put(keyword, keywordId);
                        }
                        
                        // 处理Recipe_Keyword表
                        int keywordId = KEY_CACHE.get(keyword);
                        writeCsvRow(recKeyWriter, Arrays.asList(String.valueOf(recipeId), String.valueOf(keywordId)));
                        keyRelCnt++;
                    }
                    
                    lineCnt++;
                    if (lineCnt % 5_000 == 0) {
                        keyWriter.flush();
                        recKeyWriter.flush();
                        System.out.println("Keyword 关系已处理 " + lineCnt + " 行主数据...");
                    }
                } catch (Exception e) {
                    System.err.println("跳过行: " + Arrays.toString(r) + " - " + e.getMessage());
                }
            }
            keyWriter.flush();
            recKeyWriter.flush();
            System.out.println("Keyword 临时文件写入完成");
            
            // 导入Keyword表
            try (Reader keyReader = Files.newBufferedReader(keyTempFile)) {
                copyManager.copyIn(keySql, keyReader);
            }
            System.out.println("Keyword 表导入完成");
            
            // 导入Recipe_Keyword表
            try (Reader recKeyReader = Files.newBufferedReader(recKeyTempFile)) {
                copyManager.copyIn(recKeySql, recKeyReader);
            }
            System.out.println("Recipe_Keyword 表导入完成，共 " + keyRelCnt + " 条关系");
            
        } catch (CsvValidationException e) {
            throw new IOException("CSV 解析失败", e);
        } finally {
            Files.deleteIfExists(keyTempFile);
            Files.deleteIfExists(recKeyTempFile);
        }
    }

    /* =================================================================
       recipes.csv -> table favorite
       ================================================================= */
    private static void loadFavorite(CopyManager copyManager, Path file) throws IOException, SQLException {
        String sql = "COPY Favorite (author_id,recipe_id) FROM STDIN CSV";
        Path tempFile = Files.createTempFile("favorite_import_", ".csv");
        
        try (BufferedWriter writer = Files.newBufferedWriter(tempFile);
             CSVReader reader = new CSVReaderBuilder(Files.newBufferedReader(file))
                     .withCSVParser(new RFC4180ParserBuilder().build())
                     .build()) {
            
            String[] header = reader.readNext();
            String[] r;
            int lineCnt = 0;
            int favCnt = 0;
            Set<String> uniquePairs = new LinkedHashSet<>();
            
            while ((r = reader.readNext()) != null) {
                if (r.length == 0) continue;
                try {
                    int recipeId = Integer.parseInt(r[0]);
                    Set<String> favoriteUsersSet = splitParts(r[26]);
                    
                    for (String userId : favoriteUsersSet) {
                        if (userId.isBlank()) continue;
                        String pairKey = userId + "," + recipeId;
                        if (uniquePairs.add(pairKey)) { // 仅写入唯一关系
                            writeCsvRow(writer, Arrays.asList(userId, String.valueOf(recipeId)));
                            favCnt++;
                        }
                    }
                    
                    lineCnt++;
                    if (lineCnt % 5_000 == 0) {
                        writer.flush();
                        System.out.println("Favorite 关系已处理 " + lineCnt + " 行主数据...");
                    }
                } catch (Exception e) {
                    System.err.println("跳过行: " + Arrays.toString(r) + " - " + e.getMessage());
                }
            }
            writer.flush();
            System.out.println("Favorite 临时文件写入完成，共 " + favCnt + " 条唯一关系");
            
            try (Reader fileReader = Files.newBufferedReader(tempFile)) {
                copyManager.copyIn(sql, fileReader);
            }
            System.out.println("Favorite 表导入完成");
            
        } catch (CsvValidationException e) {
            throw new IOException("CSV 解析失败", e);
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    /* =================================================================
       recipes.csv -> table time_information
       ================================================================= */
    private static void loadTimeInformation(CopyManager copyManager, Path file) throws IOException, SQLException {
        String sql = "COPY Time_Information (recipe_id,cook_time_minutes,prep_time_minutes,total_time_minutes) FROM STDIN CSV";
        Path tempFile = Files.createTempFile("time_import_", ".csv");
        
        try (BufferedWriter writer = Files.newBufferedWriter(tempFile);
             CSVReader reader = new CSVReaderBuilder(Files.newBufferedReader(file))
                     .withCSVParser(new RFC4180ParserBuilder().build())
                     .build()) {
            
            String[] header = reader.readNext();
            String[] r;
            int lineCnt = 0;
            
            while ((r = reader.readNext()) != null) {
                if (r.length == 0) continue;
                try {
                    List<String> fields = Arrays.asList(
                        r[0],                                   // recipe_id
                        String.valueOf(parseIsoDuration(r[4])), // cook_time_minutes
                        String.valueOf(parseIsoDuration(r[5])), // prep_time_minutes
                        String.valueOf(parseIsoDuration(r[6]))  // total_time_minutes
                    );
                    writeCsvRow(writer, fields);
                    lineCnt++;
                    
                    if (lineCnt % 10_000 == 0) {
                        writer.flush();
                        System.out.println("TimeInformation 已处理 " + lineCnt + " 行...");
                    }
                } catch (Exception e) {
                    System.err.println("跳过行: " + Arrays.toString(r) + " - " + e.getMessage());
                }
            }
            writer.flush();
            System.out.println("TimeInformation 临时文件写入完成，共 " + lineCnt + " 行");
            
            try (Reader fileReader = Files.newBufferedReader(tempFile)) {
                copyManager.copyIn(sql, fileReader);
            }
            System.out.println("TimeInformation 表导入完成");
            
        } catch (CsvValidationException e) {
            throw new IOException("CSV 解析失败", e);
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    /* =================================================================
       reviews.csv -> table review
       ================================================================= */
    private static void loadReview(CopyManager copyManager, Path file) throws IOException, SQLException {
        String sql = "COPY Review (id,recipe_id,author_id,rating,content,date_submitted,date_modified) FROM STDIN CSV";
        Path tempFile = Files.createTempFile("review_import_", ".csv");
        
        try (BufferedWriter writer = Files.newBufferedWriter(tempFile);
             CSVReader reader = new CSVReaderBuilder(Files.newBufferedReader(file))
                     .withCSVParser(new RFC4180ParserBuilder().build())
                     .build()) {
            
            String[] header = reader.readNext();
            String[] r;
            int lineCnt = 0;
            
            while ((r = reader.readNext()) != null) {
                if (r.length == 0) continue;
                if (r[1].isBlank()) continue;
                if (r[2].isBlank()) continue;
                try {
                    String dateSubmitted = formatTimestamp(r[6]);
                    String dateModified = formatTimestamp(r[7]);
                    List<String> fields = Arrays.asList(
                        r[0],                                   // id
                        r[1],                                   // recipe_id
                        r[2],                                   // author_id
                        r[4].isBlank() ? "0" : r[4],           // rating
                        nullifyCsv(r[5]),                       // content
                        dateSubmitted,                          // date_submitted
                        dateModified                            // date_modified
                    );
                    writeCsvRow(writer, fields);
                    lineCnt++;
                    
                    if (lineCnt % 10_000 == 0) {
                        writer.flush();
                        System.out.println("Review 表已处理 " + lineCnt + " 行...");
                    }
                } catch (Exception e) {
                    System.err.println("跳过行: " + Arrays.toString(r) + " - " + e.getMessage());
                }
            }
            writer.flush();
            System.out.println("Review 临时文件写入完成，共 " + lineCnt + " 行");
            
            try (Reader fileReader = Files.newBufferedReader(tempFile)) {
                copyManager.copyIn(sql, fileReader);
            }
            System.out.println("Review 表导入完成");
            
        } catch (CsvValidationException e) {
            throw new IOException("CSV 解析失败", e);
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    /* =================================================================
       reviews.csv -> table like
       ================================================================= */
    private static void loadLike(CopyManager copyManager, Path file) throws IOException, SQLException {
        String sql = "COPY \"Like\" (author_id,review_id) FROM STDIN CSV";
        Path tempFile = Files.createTempFile("like_import_", ".csv");
        
        try (BufferedWriter writer = Files.newBufferedWriter(tempFile);
             CSVReader reader = new CSVReaderBuilder(Files.newBufferedReader(file))
                     .withCSVParser(new RFC4180ParserBuilder().build())
                     .build()) {
            
            String[] header = reader.readNext();
            String[] r;
            int lineCnt = 0;
            int likeCnt = 0;
            Set<String> uniquePairs = new LinkedHashSet<>();
            
            while ((r = reader.readNext()) != null) {
                if (r.length == 0) continue;
                try {
                    int reviewId = Integer.parseInt(r[0]);
                    Set<String> likeUserSet = splitParts(r[8]);
                    
                    for (String userId : likeUserSet) {
                        if (userId.isBlank()) continue;
                        String pairKey = userId + "," + reviewId;
                        if (uniquePairs.add(pairKey)) { // 仅写入唯一关系
                            writeCsvRow(writer, Arrays.asList(userId, String.valueOf(reviewId)));
                            likeCnt++;
                        }
                    }
                    
                    lineCnt++;
                    if (lineCnt % 5_000 == 0) {
                        writer.flush();
                        System.out.println("Like 关系已处理 " + lineCnt + " 行主数据...");
                    }
                } catch (Exception e) {
                    System.err.println("跳过行: " + Arrays.toString(r) + " - " + e.getMessage());
                }
            }
            writer.flush();
            System.out.println("Like 临时文件写入完成，共 " + likeCnt + " 条唯一关系");
            
            try (Reader fileReader = Files.newBufferedReader(tempFile)) {
                copyManager.copyIn(sql, fileReader);
            }
            System.out.println("Like 表导入完成");
            
        } catch (CsvValidationException e) {
            throw new IOException("CSV 解析失败", e);
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    /* ===================== 工具方法 ===================== */
    private static final Map<String, Integer> ING_CACHE = new HashMap<>();
    private static final Map<String, Integer> KEY_CACHE = new HashMap<>();

    /* 拆多值字段 */
    private static Set<String> splitParts(String src) {
        if (src == null || src.isBlank()) return Set.of();
        src = src.replaceAll("^c\\(?(\"?)", "");
        src = src.replaceAll("(\"?)\\)$", "");
        src = src.replace("\"\"", "");
        src = src.replace("\"", "");
        src = src.trim();
        if (src.startsWith(",")) src = src.substring(1).trim();
        if (src.endsWith(",")) src = src.substring(0, src.length() - 1).trim();
        return Arrays.stream(src.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /* 解析 ISO-8601 时长到分钟 */
    private static int parseIsoDuration(String s) {
        if (s == null || s.isBlank()) return 0;
        int minutes = 0;
        s = s.substring(2);
        if (s.contains("D")) { String[] sp = s.split("D"); minutes += Integer.parseInt(sp[0]) * 24 * 60; s = sp[1]; }
        if (s.contains("H")) { String[] sp = s.split("H"); minutes += Integer.parseInt(sp[0]) * 60; s = sp.length > 1 ? sp[1] : ""; }
        if (s.contains("M")) { String[] sp = s.split("M"); minutes += Integer.parseInt(sp[0]); }
        return minutes;
    }

    /* CSV写入辅助方法 */
    private static void writeCsvRow(BufferedWriter writer, List<String> fields) throws IOException {
        for (int i = 0; i < fields.size(); i++) {
            String field = fields.get(i);
            if (field == null || field.isEmpty()) {
                // 空值，直接跳过
            } else if (field.contains(",") || field.contains("\"") || field.contains("\n") || field.contains("\r")) {
                // 需要引号包裹
                field = field.replace("\"", "\"\"");
                writer.write("\"" + field + "\"");
            } else {
                writer.write(field);
            }
            if (i < fields.size() - 1) {
                writer.write(",");
            }
        }
        writer.newLine();
    }
    
    private static String nullifyCsv(String s) {
        if (s == null || s.isBlank() || "null".equalsIgnoreCase(s)) return "";
        return escapeCsv(s);
    }
    
    private static String escapeCsv(String s) {
        if (s == null) return "";
        if (s.contains(",") || s.contains("\"") || s.contains("\n") || s.contains("\r")) {
            return "\"" + s.replace("\"", "\"\"") + "\"";
        }
        return s;
    }

    private static String formatTimestamp(String s) {
        if (s == null || s.isBlank()) return "";
        try {
            Instant instant = Instant.parse(s);
            LocalDateTime ldt = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
            return ldt.toString();
        } catch (Exception e) {
            return "";
        }
    }
}