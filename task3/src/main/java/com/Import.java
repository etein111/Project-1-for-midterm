package main.java.com;
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

// import org.apache.commons.csv.CSVFormat;
// import org.apache.commons.csv.CSVParser;
// import org.apache.commons.csv.CSVRecord;

public class Import {

    /* ===================== 连接常量 ===================== */
    private static final String URL   = "jdbc:postgresql://localhost:5432/project1";
    private static final String USER  = "postgres";
    private static final String PWD   = "123456";
    private static final int    BATCH = 2_000;

    /* ===================== 主入口 ===================== */
    public static void main(String[] args) throws Exception {
        if (args.length != 2) throw new IllegalArgumentException("需要提供 CSV 文件目录和数据库名称两个参数！");
        Path dir = Paths.get(args[0]);

        try (Connection con = DriverManager.getConnection(URL, USER, PWD)) {
            con.setAutoCommit(false);

            // loadUser        (con, dir.resolve("user.csv"));
            // loadFollows     (con, dir.resolve("user.csv"));

            loadUser        (con, dir.resolve("user_fixed.csv"));
            loadFollows     (con, dir.resolve("user_fixed.csv"));

            // loadRecipe                      (con, dir.resolve("recipes.csv"));
            // loadIngredient                  (con, dir.resolve("recipes.csv"));
            // loadRecipeIngredient            (con, dir.resolve("recipes.csv"));
            // loadNutritionalInformation      (con, dir.resolve("recipes.csv"));
            // loadServingInformation          (con, dir.resolve("recipes.csv"));
            // loadKeyword                     (con, dir.resolve("recipes.csv"));
            // loadRecipeKeyword               (con, dir.resolve("recipes.csv"));
            // loadFavorite                    (con, dir.resolve("recipes.csv"));
            // loadTimeInformation             (con, dir.resolve("recipes.csv"));

            loadRecipe                      (con, dir.resolve("recipes_checktime.csv"));
            loadIngredient                  (con, dir.resolve("recipes_checktime.csv"));
            loadNutritionalInformation      (con, dir.resolve("recipes_checktime.csv"));
            loadServingInformation          (con, dir.resolve("recipes_checktime.csv"));
            loadKeyword                     (con, dir.resolve("recipes_checktime.csv"));
            loadFavorite                    (con, dir.resolve("recipes_checktime.csv"));
            loadTimeInformation             (con, dir.resolve("recipes_checktime.csv"));

            // loadReview      (con, dir.resolve("reviews.csv"));
            // loadLike        (con, dir.resolve("reviews.csv"));

            loadReview    (con, dir.resolve("reviews_recipeid2int.csv"));
            loadLike      (con, dir.resolve("reviews_recipeid2int.csv"));

            con.commit();
            System.out.println("===== 全部表导入完成 =====");
        }
    }

    /* =================================================================
        user.csv -> table User
        ================================================================= */
    private static void loadUser(Connection con, Path file) throws IOException, SQLException {
        String Usersql = "INSERT INTO \"User\" (id,name,gender,age,followers_count,following_count) "
                    +"VALUES (?,?,?,?,?,?) on conflict (id) do nothing";
        try (PreparedStatement psUser = con.prepareStatement(Usersql);
            CSVReader reader = new CSVReaderBuilder(Files.newBufferedReader(file))
                     .withCSVParser(new RFC4180ParserBuilder().build())
                     .build()
            ) {
            int LineCnt = 0;
            String[] header = reader.readNext();
            String[] r;
            while ((r = reader.readNext()) != null) {
                if (r.length == 0) continue;
                try {
                    /* user.csv -> table User */
                    psUser.setInt   (1, Integer.parseInt(r[0]));            // id
                    psUser.setString(2, nullify(r[1]));                     // name
                    psUser.setString(3, nullify(r[2]));                     // gender
                    psUser.setInt   (4, parseIntOr0(r[3]));                 // age
                    psUser.setInt   (5, parseIntOr0(r[4]));                 // followers_count
                    psUser.setInt   (6, parseIntOr0(r[5]));                 // following_count
                    psUser.addBatch();

                    if (++LineCnt % BATCH == 0) {  // 每 BATCH 行提交一次
                        psUser.executeBatch();
                        System.out.println("User 表已缓存 " + LineCnt + " 行数据...");
                    }

                    if (LineCnt % 10_000 == 0) {
                        con.commit();
                        System.out.println("中间提交点：已处理 " + LineCnt + " 行...");
                    }
                } catch (Exception e) {
                    // writeBadLine(file, r, e);
                    System.out.println(e);
                }
            }
            psUser.executeBatch();   // 剩余不足一批
            con.commit();
            System.out.println("User 表导入完成");
        } catch (CsvValidationException | IOException | SQLException e) {
            throw new IOException("CSV 解析失败", e);
        }
    }

    /* =================================================================
        user.csv -> table Follows
        ================================================================= */
    private static void loadFollows(Connection con, Path file) throws IOException, SQLException {
        String Followsql = "INSERT INTO Follows (follower_id,following_id) VALUES (?,?) on conflict do nothing";
        try (PreparedStatement psFollow = con.prepareStatement(Followsql);
            CSVReader reader = new CSVReaderBuilder(Files.newBufferedReader(file))
                     .withCSVParser(new RFC4180ParserBuilder().build())
                     .build()
            ) {
            int LineCnt = 0;
            int FollowerBatchCnt = 0;
            int FollowingBatchCnt = 0;
            int TotalBatchCnt = 0;
            String[] header = reader.readNext();
            String[] r;
            while ((r = reader.readNext()) != null) {
                if (r.length == 0) continue;
                try {
                    /* user.csv -> table follower */
                    /* 作者关注的所有人： (authorId → followingUser)  */
                    Set<String> followingSet = splitParts(r[7]); // FollowingUsers
                    for (String uid : followingSet) {
                        psFollow.setInt(1, Integer.parseInt(r[0])); // authorId
                        psFollow.setInt(2, Integer.parseInt(uid));
                        psFollow.addBatch();
                        FollowingBatchCnt++;
                        if ( (FollowerBatchCnt + FollowingBatchCnt) % BATCH == 0) {
                            psFollow.executeBatch();
                            TotalBatchCnt = FollowerBatchCnt + FollowingBatchCnt;
                            System.out.println("Follower 表已缓存 " + TotalBatchCnt + " 行数据...");
                        }
                    }

                    /* 所有关注作者的人： (followerUser → authorId)  */
                    Set<String> followerSet = splitParts(r[6]); // FollowerUsers
                    for (String uid : followerSet) {
                        psFollow.setInt(1, Integer.parseInt(uid));
                        psFollow.setInt(2, Integer.parseInt(r[0])); // authorId
                        psFollow.addBatch();
                        FollowerBatchCnt++;
                        if ( (FollowerBatchCnt + FollowingBatchCnt) % BATCH == 0) {
                            psFollow.executeBatch();
                            TotalBatchCnt = FollowerBatchCnt + FollowingBatchCnt;
                            System.out.println("Follower 表已缓存 " + TotalBatchCnt + " 行数据...");
                        }
                    }

                    if (LineCnt > 10_000) {
                        con.commit();
                        System.out.println("中间提交点：已处理 " + LineCnt + " 行...");
                        LineCnt = 0;
                    }
                } catch (Exception e) {
                    System.out.println(e);
                }
            }
            psFollow.executeBatch();
            con.commit();
            System.out.println("Follows 表导入完成");
        } catch (CsvValidationException | IOException | SQLException e) {
            throw new IOException("CSV 解析失败", e);
        }
    }

    /* =================================================================
       recipes.csv -> table recipe
       ================================================================= */
    private static void loadRecipe(Connection con, Path file) throws IOException, SQLException {
        String recSql = "insert into Recipe (id,name,author_id,date_published,description,category,aggregated_rating,review_count,instructions) "
                        +"values (?,?,?,?,?,?,?,?,?)";
        try (
                PreparedStatement psRec  = con.prepareStatement(recSql);
                CSVReader reader = new CSVReaderBuilder(Files.newBufferedReader(file))
                     .withCSVParser(new RFC4180ParserBuilder().build())
                     .build()
             ) {
            int IngredientId = 0;
            int KeywordId = 0;
            int cnt = 0;
            int relTotal = 0;
            String[] header = reader.readNext();
            String[] r;
            // String line;
            while ((r = reader.readNext()) != null) {
                // if (line.isBlank()) continue;
                if (r.length == 0) continue;
                try {
                    // String[] r = splitCsvLine(line);

                    int RecipeId = Integer.parseInt(r[0]);
                    String Name = nullify(r[1]);
                    int AuthorId = Integer.parseInt(r[2]);
                    // String AuthorName = nullify(r[3]);
                    // int CookTime = parseIsoDuration(r[4]);
                    // int PrepTime = parseIsoDuration(r[5]);
                    // int TotalTime = parseIsoDuration(r[6]);
                    LocalDateTime DatePublished = parseTimestamp(r[7]);
                    String Description = nullify(r[8]);
                    String RecipeCategory = nullify(r[9]);
                    // Keywords r[10]
                    // RecipeIngredientParts r[11]
                    Double AggregatedRating = parseDoubleOr0(r[12]);
                    int ReviewCount = parseIntOr0(r[13]);
                    // Double Calories = parseDoubleOr0(r[14]);
                    // Double Fatcontent = parseDoubleOr0(r[15]);
                    // Double SaturatedFatContent = parseDoubleOr0(r[16]);
                    // Double CholesterolContent = parseDoubleOr0(r[17]);
                    // Double SodiumContent = parseDoubleOr0(r[18]);
                    // Double CarbohydrateContent = parseDoubleOr0(r[19]);
                    // Double FiberContent = parseDoubleOr0(r[20]);
                    // Double SugarContent = parseDoubleOr0(r[21]);
                    // Double ProteinContent = parseDoubleOr0(r[22]);
                    // int RecipServings = parseIntOr0(r[23]);
                    // String RecipeYield = nullify(r[24]);
                    String RecipeInstructions = nullify(r[25]);
                    // FavoriteUsers r[26]

                    /* recipes.csv -> Recipe */
                    psRec.setInt    (1, RecipeId);
                    psRec.setString (2, Name);
                    psRec.setInt    (3, AuthorId);
                    psRec.setObject (4, DatePublished);            // date_published
                    psRec.setString (5, Description);
                    psRec.setString (6, RecipeCategory);                   // category
                    psRec.setDouble (7, AggregatedRating);           // aggregated_rating
                    psRec.setInt    (8, ReviewCount);              // review_count
                    psRec.setString (9, RecipeInstructions);                  // instructions
                    psRec.addBatch();

                    if (++cnt % BATCH == 0) {
                        psRec.executeBatch();
                        System.out.println("已处理 " + cnt + " 行数据...");
                    }
                    
                    if (cnt % 10_000 == 0) {
                        con.commit();
                        System.out.println("中间提交点：已处理 " + cnt + " 行...");
                    }

                } catch (Exception e) { 
                    System.out.println(e);
                }
            }
            psRec.executeBatch();
            con.commit();
            System.out.println("Recipe 导入完成！总计 " + cnt + " 条食谱。");
        } catch (CsvValidationException | IOException e) {
            throw new IOException("CSV 解析失败", e);
        }
    }

    /* =================================================================
       recipes.csv -> table ingredient, recipe_ingredient
       ================================================================= */
    private static void loadIngredient(Connection con, Path file) throws IOException, SQLException {
        String ingSql = "insert into Ingredient (id,name) values (?,?) on conflict do nothing";
        String recIngSql = "insert into Recipe_Ingredient (recipe_id,ingredient_id) values (?,?) on conflict do nothing";
        
        try (
                PreparedStatement psIng  = con.prepareStatement(ingSql);
                PreparedStatement psRecIng = con.prepareStatement(recIngSql);
                CSVReader reader = new CSVReaderBuilder(Files.newBufferedReader(file))
                     .withCSVParser(new RFC4180ParserBuilder().build())
                     .build()
             ) {
            int IngredientId = 0;
            int KeywordId = 0;
            int cnt = 0;
            int relTotal = 0;
            String[] header = reader.readNext();
            String[] r;
            while ((r = reader.readNext()) != null) {
                if (r.length == 0) continue;
                try {
                    // String[] r = splitCsvLine(line);

                    int RecipeId = Integer.parseInt(r[0]);
                    // String Name = nullify(r[1]);
                    // int AuthorId = Integer.parseInt(r[2]);
                    // String AuthorName = nullify(r[3]);
                    // int CookTime = parseIsoDuration(r[4]);
                    // int PrepTime = parseIsoDuration(r[5]);
                    // int TotalTime = parseIsoDuration(r[6]);
                    // LocalDateTime DatePublished = parseTimestamp(r[7]);
                    // String Description = nullify(r[8]);
                    // String RecipeCategory = nullify(r[9]);
                    // Keywords r[10]
                    // RecipeIngredientParts r[11]
                    // Double AggregatedRating = parseDoubleOr0(r[12]);
                    // int ReviewCount = parseIntOr0(r[13]);
                    // Double Calories = parseDoubleOr0(r[14]);
                    // Double Fatcontent = parseDoubleOr0(r[15]);
                    // Double SaturatedFatContent = parseDoubleOr0(r[16]);
                    // Double CholesterolContent = parseDoubleOr0(r[17]);
                    // Double SodiumContent = parseDoubleOr0(r[18]);
                    // Double CarbohydrateContent = parseDoubleOr0(r[19]);
                    // Double FiberContent = parseDoubleOr0(r[20]);
                    // Double SugarContent = parseDoubleOr0(r[21]);
                    // Double ProteinContent = parseDoubleOr0(r[22]);
                    // int RecipServings = parseIntOr0(r[23]);
                    // String RecipeYield = nullify(r[24]);
                    // String RecipeInstructions = nullify(r[25]);
                    // FavoriteUsers r[26]

                    /* recipes.csv -> ingredient, recipe_ingredient */
                    Set<String> ingredientNames = splitParts(r[11]);                      // RecipeIngredientParts
                    if (!ingredientNames.isEmpty()) {
                        for (String name : ingredientNames) {
                            if (name.isBlank()) continue;
                            if (!ING_CACHE.containsKey(name)) {                         // 如果现有map中没有name
                                psIng.setString(2, name);                  // name传入数据库
                                IngredientId++;
                                psIng.setInt(1, IngredientId);
                                psIng.addBatch();
                                relTotal++;
                                ING_CACHE.put(name, IngredientId); // name和id增加到map
                            }
                        }

                        /* 写入表 (recipe_id , ingredient_id) */
                        for (String name : ingredientNames) {
                            if (name.isBlank()) continue;
                            psRecIng.setInt(1, RecipeId);
                            psRecIng.setInt(2, ING_CACHE.get(name));
                            psRecIng.addBatch();
                            relTotal++;
                        }
                    }

                    if (relTotal >= BATCH) {
                        psIng.executeBatch();
                        psRecIng.executeBatch();
                        relTotal = 0;
                    }
                    
                    cnt++;
                    if (cnt % 10_000 == 0) {
                        con.commit();
                        System.out.println("中间提交点：已处理 " + cnt + " 行...");
                    }

                } catch (Exception e) {
                    System.out.println(e);
                }
            }
            psIng.executeBatch();
            psRecIng.executeBatch();
            con.commit();
            System.out.println("Ingredient, RecipeIngredient导入完成！总计 " + cnt + " 条食谱。");
        } catch (CsvValidationException | IOException | SQLException e) {
            throw new IOException("CSV 解析失败", e);
        }
    }

    /* =================================================================
       recipes.csv -> table nutritional_information
       ================================================================= */
    private static void loadNutritionalInformation(Connection con, Path file) throws IOException, SQLException {
        String nutSql = "insert into Nutritional_Information (recipe_id,calories,fat_content,saturated_fat,cholesterol,sodium,carbohydrate,fiber,sugar,protein) values (?,?,?,?,?,?,?,?,?,?)";
        try (
                PreparedStatement psNut  = con.prepareStatement(nutSql);
                CSVReader reader = new CSVReaderBuilder(Files.newBufferedReader(file))
                     .withCSVParser(new RFC4180ParserBuilder().build())
                     .build()
             ) {
            int IngredientId = 0;
            int KeywordId = 0;
            int cnt = 0;
            int relTotal = 0;
            String[] header = reader.readNext();
            String[] r;
            // String line;
            while ((r = reader.readNext()) != null) {
                // if (line.isBlank()) continue;
                if (r.length == 0) continue;
                try {
                    // String[] r = splitCsvLine(line);

                    int RecipeId = Integer.parseInt(r[0]);
                    // String Name = nullify(r[1]);
                    // int AuthorId = Integer.parseInt(r[2]);
                    // String AuthorName = nullify(r[3]);
                    // int CookTime = parseIsoDuration(r[4]);
                    // int PrepTime = parseIsoDuration(r[5]);
                    // int TotalTime = parseIsoDuration(r[6]);
                    // LocalDateTime DatePublished = parseTimestamp(r[7]);
                    // String Description = nullify(r[8]);
                    // String RecipeCategory = nullify(r[9]);
                    // Keywords r[10]
                    // RecipeIngredientParts r[11]
                    // Double AggregatedRating = parseDoubleOr0(r[12]);
                    // int ReviewCount = parseIntOr0(r[13]);
                    Double Calories = parseDoubleOr0(r[14]);
                    Double Fatcontent = parseDoubleOr0(r[15]);
                    Double SaturatedFatContent = parseDoubleOr0(r[16]);
                    Double CholesterolContent = parseDoubleOr0(r[17]);
                    Double SodiumContent = parseDoubleOr0(r[18]);
                    Double CarbohydrateContent = parseDoubleOr0(r[19]);
                    Double FiberContent = parseDoubleOr0(r[20]);
                    Double SugarContent = parseDoubleOr0(r[21]);
                    Double ProteinContent = parseDoubleOr0(r[22]);
                    // int RecipServings = parseIntOr0(r[23]);
                    // String RecipeYield = nullify(r[24]);
                    // String RecipeInstructions = nullify(r[25]);
                    // FavoriteUsers r[26]

                    /* recipes.csv -> Nutritional_Information */
                    psNut.setInt    (1, RecipeId);
                    psNut.setDouble (2, Calories);
                    psNut.setDouble (3, Fatcontent);
                    psNut.setDouble (4, SaturatedFatContent);
                    psNut.setDouble (5, CholesterolContent);
                    psNut.setDouble (6, SodiumContent);
                    psNut.setDouble (7, CarbohydrateContent);
                    psNut.setDouble (8, FiberContent);
                    psNut.setDouble (9, SugarContent);
                    psNut.setDouble (10, ProteinContent);
                    psNut.addBatch();

                    if (++cnt % BATCH == 0) {
                        psNut.executeBatch();
                        System.out.println("已处理 " + cnt + " 行主数据...");
                    }
                    
                    if (cnt % 10_000 == 0) {
                        con.commit();
                        System.out.println("中间提交点：已处理 " + cnt + " 行...");
                    }

                } catch (Exception e) {
                    System.out.println(e);
                }
            }
            psNut.executeBatch();
            con.commit();
            System.out.println("NutritionalInformation 导入完成！总计 " + cnt + " 条食谱。");
        } catch (CsvValidationException | IOException e) {
            throw new IOException("CSV 解析失败", e);
        }
    }

    /* =================================================================
       recipes.csv -> table serving_information
       ================================================================= */
    private static void loadServingInformation(Connection con, Path file) throws IOException, SQLException {
        String srvSql = "insert into Serving_Information (recipe_id,servings,yield) values (?,?,?)";
        try (
                PreparedStatement psSrv  = con.prepareStatement(srvSql);
                CSVReader reader = new CSVReaderBuilder(Files.newBufferedReader(file))
                     .withCSVParser(new RFC4180ParserBuilder().build())
                     .build()
            ) {
            int IngredientId = 0;
            int KeywordId = 0;
            int cnt = 0;
            int relTotal = 0;
            String[] header = reader.readNext();
            String[] r;
            // String line;
            while ((r = reader.readNext()) != null) {
                if (r.length == 0) continue;
                try {
                    int RecipeId = Integer.parseInt(r[0]);
                    // String Name = nullify(r[1]);
                    // int AuthorId = Integer.parseInt(r[2]);
                    // String AuthorName = nullify(r[3]);
                    // int CookTime = parseIsoDuration(r[4]);
                    // int PrepTime = parseIsoDuration(r[5]);
                    // int TotalTime = parseIsoDuration(r[6]);
                    // LocalDateTime DatePublished = parseTimestamp(r[7]);
                    // String Description = nullify(r[8]);
                    // String RecipeCategory = nullify(r[9]);
                    // Keywords r[10]
                    // RecipeIngredientParts r[11]
                    // Double AggregatedRating = parseDoubleOr0(r[12]);
                    // int ReviewCount = parseIntOr0(r[13]);
                    // Double Calories = parseDoubleOr0(r[14]);
                    // Double Fatcontent = parseDoubleOr0(r[15]);
                    // Double SaturatedFatContent = parseDoubleOr0(r[16]);
                    // Double CholesterolContent = parseDoubleOr0(r[17]);
                    // Double SodiumContent = parseDoubleOr0(r[18]);
                    // Double CarbohydrateContent = parseDoubleOr0(r[19]);
                    // Double FiberContent = parseDoubleOr0(r[20]);
                    // Double SugarContent = parseDoubleOr0(r[21]);
                    // Double ProteinContent = parseDoubleOr0(r[22]);
                    int RecipServings = parseIntOr0(r[23]);
                    String RecipeYield = nullify(r[24]);
                    // String RecipeInstructions = nullify(r[25]);
                    // FavoriteUsers r[26]

                    /* recipes.csv -> Serving_Information */
                    psSrv.setInt(1, RecipeId);
                    psSrv.setInt(2, RecipServings);
                    psSrv.setString(3, RecipeYield);
                    psSrv.addBatch();

                    if (++cnt % BATCH == 0) {
                        psSrv.executeBatch();
                        System.out.println("已处理 " + cnt + " 行主数据...");
                    }
                    
                    if (cnt % 10_000 == 0) {
                        con.commit();
                        System.out.println("中间提交点：已处理 " + cnt + " 行...");
                    }

                } catch (Exception e) {
                    System.out.println(e);
                }
            }
            psSrv.executeBatch();
            con.commit();
            System.out.println("ServingInformation导入完成！总计 " + cnt + " 条食谱。");
        } catch (CsvValidationException | IOException e) {
            throw new IOException("CSV 解析失败", e);
        }
    }

    /* =================================================================
       recipes.csv -> table keyword, recipe_keyword
       ================================================================= */
    private static void loadKeyword(Connection con, Path file) throws IOException, SQLException {
        String keySql = "insert into Keyword (id,text) values (?,?) on conflict do nothing";
        String recKeySql = "insert into Recipe_Keyword (recipe_id,keyword_id) values (?,?) on conflict do nothing";
        try (
                PreparedStatement psKey  = con.prepareStatement(keySql);
                PreparedStatement psRecKey = con.prepareStatement(recKeySql);
                CSVReader reader = new CSVReaderBuilder(Files.newBufferedReader(file))
                     .withCSVParser(new RFC4180ParserBuilder().build())
                     .build()
             ) {
            int IngredientId = 0;
            int KeywordId = 0;
            int cnt = 0;
            int relTotal = 0;
            String[] header = reader.readNext();
            String[] r;
            while ((r = reader.readNext()) != null) {
                if (r.length == 0) continue;
                try {
                    int RecipeId = Integer.parseInt(r[0]);
                    // String Name = nullify(r[1]);
                    // int AuthorId = Integer.parseInt(r[2]);
                    // String AuthorName = nullify(r[3]);
                    // int CookTime = parseIsoDuration(r[4]);
                    // int PrepTime = parseIsoDuration(r[5]);
                    // int TotalTime = parseIsoDuration(r[6]);
                    // LocalDateTime DatePublished = parseTimestamp(r[7]);
                    // String Description = nullify(r[8]);
                    // String RecipeCategory = nullify(r[9]);
                    // Keywords r[10]
                    // RecipeIngredientParts r[11]
                    // Double AggregatedRating = parseDoubleOr0(r[12]);
                    // int ReviewCount = parseIntOr0(r[13]);
                    // Double Calories = parseDoubleOr0(r[14]);
                    // Double Fatcontent = parseDoubleOr0(r[15]);
                    // Double SaturatedFatContent = parseDoubleOr0(r[16]);
                    // Double CholesterolContent = parseDoubleOr0(r[17]);
                    // Double SodiumContent = parseDoubleOr0(r[18]);
                    // Double CarbohydrateContent = parseDoubleOr0(r[19]);
                    // Double FiberContent = parseDoubleOr0(r[20]);
                    // Double SugarContent = parseDoubleOr0(r[21]);
                    // Double ProteinContent = parseDoubleOr0(r[22]);
                    // int RecipServings = parseIntOr0(r[23]);
                    // String RecipeYield = nullify(r[24]);
                    // String RecipeInstructions = nullify(r[25]);
                    // FavoriteUsers r[26]

                    /* recipe.csv -> Keyword, Recipe_Keyword */
                    Set<String> keywordsSet = splitParts(r[10]);                      // Keywords
                    if (!keywordsSet.isEmpty()) {
                        for (String keyword : keywordsSet) {
                            if (keyword.isBlank()) continue;
                            if (!KEY_CACHE.containsKey(keyword)) {                         // 如果现有map中没有keyword
                                psKey.setString(2, keyword);                  // keyword传入数据库
                                KeywordId++;
                                psKey.setInt(1, KeywordId);
                                psKey.addBatch();
                                relTotal++;
                                KEY_CACHE.put(keyword, KeywordId); // keyword和id增加到map
                            }
                        }

                        /* 写入表 (recipe_id , keyword_id) */
                        for (String keyword : keywordsSet) {
                            if (keyword.isBlank()) continue;
                            psRecKey.setInt(1, RecipeId);
                            psRecKey.setInt(2, KEY_CACHE.get(keyword));
                            psRecKey.addBatch();
                            relTotal++;
                        }
                    }

                    if (relTotal >= BATCH) {
                        psKey.executeBatch();
                        psRecKey.executeBatch();
                        relTotal = 0;
                    }
                    
                    cnt++;
                    if (cnt % 10_000 == 0) {
                        con.commit();
                        System.out.println("中间提交点：已处理 " + cnt + " 行...");
                    }

                } catch (Exception e) {
                    System.out.println(e);
                }
            }
            psKey.executeBatch();
            psRecKey.executeBatch();
            con.commit();
            System.out.println("keyword, recipe_keyword导入完成！总计 " + cnt + " 条食谱。");
        } catch (CsvValidationException | IOException e) {
            throw new IOException("CSV 解析失败", e);
        }
    }

    /* =================================================================
       recipes.csv -> table favorite
       ================================================================= */
    private static void loadFavorite(Connection con, Path file) throws IOException, SQLException {
        String favSql = "insert into Favorite (author_id,recipe_id) values (?,?) on conflict do nothing";
        try (
                PreparedStatement psFav  = con.prepareStatement(favSql);
                CSVReader reader = new CSVReaderBuilder(Files.newBufferedReader(file))
                     .withCSVParser(new RFC4180ParserBuilder().build())
                     .build()
             ) {
            int IngredientId = 0;
            int KeywordId = 0;
            int cnt = 0;
            int relTotal = 0;
            String[] header = reader.readNext();
            String[] r;
            while ((r = reader.readNext()) != null) {
                if (r.length == 0) continue;
                try {
                    int RecipeId = Integer.parseInt(r[0]);
                    // String Name = nullify(r[1]);
                    // int AuthorId = Integer.parseInt(r[2]);
                    // String AuthorName = nullify(r[3]);
                    // int CookTime = parseIsoDuration(r[4]);
                    // int PrepTime = parseIsoDuration(r[5]);
                    // int TotalTime = parseIsoDuration(r[6]);
                    // LocalDateTime DatePublished = parseTimestamp(r[7]);
                    // String Description = nullify(r[8]);
                    // String RecipeCategory = nullify(r[9]);
                    // Keywords r[10]
                    // RecipeIngredientParts r[11]
                    // Double AggregatedRating = parseDoubleOr0(r[12]);
                    // int ReviewCount = parseIntOr0(r[13]);
                    // Double Calories = parseDoubleOr0(r[14]);
                    // Double Fatcontent = parseDoubleOr0(r[15]);
                    // Double SaturatedFatContent = parseDoubleOr0(r[16]);
                    // Double CholesterolContent = parseDoubleOr0(r[17]);
                    // Double SodiumContent = parseDoubleOr0(r[18]);
                    // Double CarbohydrateContent = parseDoubleOr0(r[19]);
                    // Double FiberContent = parseDoubleOr0(r[20]);
                    // Double SugarContent = parseDoubleOr0(r[21]);
                    // Double ProteinContent = parseDoubleOr0(r[22]);
                    // int RecipServings = parseIntOr0(r[23]);
                    // String RecipeYield = nullify(r[24]);
                    // String RecipeInstructions = nullify(r[25]);
                    // FavoriteUsers r[26]

                    /* recipe.csv -> Favorite */
                    Set<String> FavoriteUsersSet = splitParts(r[26]);
                    if (!FavoriteUsersSet.isEmpty()) {
                        for (String FavoriteUser : FavoriteUsersSet) {
                            if (FavoriteUser.isBlank()) continue;
                            psFav.setInt(1, Integer.parseInt(FavoriteUser));
                            psFav.setInt(2, RecipeId);
                            psFav.addBatch();
                            relTotal++;
                        }
                    }

                    if (relTotal >= BATCH) {
                        psFav.executeBatch();
                        relTotal = 0;
                    }
                    
                    cnt++;
                    if (cnt % 10_000 == 0) {
                        con.commit();
                        System.out.println("中间提交点：已处理 " + cnt + " 行...");
                    }

                } catch (Exception e) {
                    System.out.println(e);
                }
            }
            psFav.executeBatch();
            con.commit();
            System.out.println("Favorite导入完成！总计 " + cnt + " 条食谱。");
        } catch (CsvValidationException | IOException e) {
            throw new IOException("CSV 解析失败", e);
        }
    }

    /* =================================================================
       recipes.csv -> table time_information
       ================================================================= */
    private static void loadTimeInformation(Connection con, Path file) throws IOException, SQLException {
        String timeSql = "insert into Time_Information (recipe_id,cook_time_minutes,prep_time_minutes,total_time_minutes) values (?,?,?,?)";
        try (
                PreparedStatement psTime = con.prepareStatement(timeSql);
                CSVReader reader = new CSVReaderBuilder(Files.newBufferedReader(file))
                     .withCSVParser(new RFC4180ParserBuilder().build())
                     .build()
             ) {
            int IngredientId = 0;
            int KeywordId = 0;
            int cnt = 0;
            int relTotal = 0;
            String[] header = reader.readNext();
            String[] r;
            while ((r = reader.readNext()) != null) {
                if (r.length == 0) continue;
                try {
                    int RecipeId = Integer.parseInt(r[0]);
                    // String Name = nullify(r[1]);
                    // int AuthorId = Integer.parseInt(r[2]);
                    // String AuthorName = nullify(r[3]);
                    int CookTime = parseIsoDuration(r[4]);
                    int PrepTime = parseIsoDuration(r[5]);
                    int TotalTime = parseIsoDuration(r[6]);
                    // LocalDateTime DatePublished = parseTimestamp(r[7]);
                    // String Description = nullify(r[8]);
                    // String RecipeCategory = nullify(r[9]);
                    // Keywords r[10]
                    // RecipeIngredientParts r[11]
                    // Double AggregatedRating = parseDoubleOr0(r[12]);
                    // int ReviewCount = parseIntOr0(r[13]);
                    // Double Calories = parseDoubleOr0(r[14]);
                    // Double Fatcontent = parseDoubleOr0(r[15]);
                    // Double SaturatedFatContent = parseDoubleOr0(r[16]);
                    // Double CholesterolContent = parseDoubleOr0(r[17]);
                    // Double SodiumContent = parseDoubleOr0(r[18]);
                    // Double CarbohydrateContent = parseDoubleOr0(r[19]);
                    // Double FiberContent = parseDoubleOr0(r[20]);
                    // Double SugarContent = parseDoubleOr0(r[21]);
                    // Double ProteinContent = parseDoubleOr0(r[22]);
                    // int RecipServings = parseIntOr0(r[23]);
                    // String RecipeYield = nullify(r[24]);
                    // String RecipeInstructions = nullify(r[25]);
                    // FavoriteUsers r[26]

                    /* recipe.csv -> Time_Information */
                    psTime.setInt(1, RecipeId);
                    psTime.setInt(2, CookTime);
                    psTime.setInt(3, PrepTime);
                    psTime.setInt(4, TotalTime);
                    psTime.addBatch();

                    if (++cnt % BATCH == 0) {
                        psTime.executeBatch();
                        System.out.println("已处理 " + cnt + " 行主数据...");
                    }
                    
                    if (cnt % 10_000 == 0) {
                        con.commit();
                        System.out.println("中间提交点：已处理 " + cnt + " 行...");
                    }

                } catch (Exception e) {
                    System.out.println(e);
                }
            }
            psTime.executeBatch();
            con.commit();
            System.out.println("TimeInformation导入完成！总计 " + cnt + " 条食谱。");
        } catch (CsvValidationException | IOException e) {
            throw new IOException("CSV 解析失败", e);
        }
    }

    /* =================================================================
       reviews.csv -> table review
       ================================================================= */
    private static void loadReview(Connection con, Path file) throws SQLException, IOException {
        String Revsql = "INSERT INTO Review (id,recipe_id,author_id,rating,content,date_submitted,date_modified) "
                    +"VALUES (?,?,?,?,?,?,?) on conflict (id) do nothing";
        try (
                PreparedStatement psRev = con.prepareStatement(Revsql);
                CSVReader reader = new CSVReaderBuilder(Files.newBufferedReader(file))
                        .withCSVParser(new RFC4180ParserBuilder().build())
                        .build()
            ) {
            int LineCnt = 0;
            int Rel = 0;
            String[] header = reader.readNext();
            String[] r;
            while ((r = reader.readNext()) != null) {
                if (r.length == 0) continue;
                try {
                    int ReviewId = Integer.parseInt(r[0]);
                    int RecipeId = Integer.parseInt(r[1]);
                    int AuthorId = Integer.parseInt(r[2]);
                    // String AuthorName = nullify(r[3]);
                    int Rating = parseIntOr0(r[4]);
                    String Review = nullify(r[5]);
                    LocalDateTime DateSubmitted = parseTimestamp(r[6]);
                    LocalDateTime DateModified = parseTimestamp(r[7]);
                    // Likes r[8]

                    /* reviews.csv -> table review */
                    psRev.setInt    (1, ReviewId);
                    psRev.setInt    (2, RecipeId);
                    psRev.setInt    (3, AuthorId);
                    psRev.setInt    (4, Rating);
                    psRev.setString (5, Review);
                    psRev.setObject (6, DateSubmitted);
                    psRev.setObject (7, DateModified);
                    psRev.addBatch();

                    if (++LineCnt % BATCH == 0) {  // 每 BATCH 行提交一次
                        psRev.executeBatch();
                        System.out.println("Review 表已缓存 " + LineCnt + " 行数据...");
                    }

                    if (LineCnt % 10_000 == 0) {
                        con.commit();
                        System.out.println("中间提交点：已处理 " + LineCnt + " 行...");
                    }
                }
                catch (Exception e) {
                    System.out.println(e);
                }
            }
            psRev.executeBatch();   // 剩余不足一批
            con.commit();
            System.out.println("Review 导入完成");
        }
        catch (CsvValidationException | IOException e) {
            throw new IOException("CSV 解析失败", e);
        }
    }

    /* =================================================================
       reviews.csv -> table like
       ================================================================= */
    private static void loadLike(Connection con, Path file) throws SQLException, IOException {
        String Likesql = "INSERT INTO \"Like\" (author_id,review_id) VALUES (?,?) on conflict do nothing";
        try (
                PreparedStatement psLike = con.prepareStatement(Likesql);
                CSVReader reader = new CSVReaderBuilder(Files.newBufferedReader(file))
                            .withCSVParser(new RFC4180ParserBuilder().build())
                            .build()
            ) {
            int LineCnt = 0;
            int Rel = 0;
            String[] header = reader.readNext();
            String[] r;
            while ((r = reader.readNext()) != null) {
                if (r.length == 0) continue;
                try {
                    int ReviewId = Integer.parseInt(r[0]);
                    // int RecipeId = Integer.parseInt(r[1]);
                    // int AuthorId = Integer.parseInt(r[2]);
                    // String AuthorName = nullify(r[3]);
                    // int Rating = parseIntOr0(r[4]);
                    // String Review = nullify(r[5]);
                    // LocalDateTime DateSubmitted = parseTimestamp(r[6]);
                    // LocalDateTime DateModified = parseTimestamp(r[7]);
                    // Likes r[8]

                    /* reviews.csv -> table like */
                    Set<String> LikeUserSet = splitParts(r[8]);
                    if (!LikeUserSet.isEmpty()) {
                        for (String User : LikeUserSet) {
                            if (User.isBlank()) continue;
                            psLike.setInt(1, Integer.parseInt(User));
                            psLike.setInt(2, ReviewId);
                            psLike.addBatch();
                            Rel++;
                        }
                    }
                    if (Rel >= BATCH) {
                        psLike.executeBatch();
                        Rel = 0;
                    }

                    if (++LineCnt % 10_000 == 0) {
                        con.commit();
                        System.out.println("中间提交点：已处理 " + LineCnt + " 行...");
                    }
                } catch (Exception e) {
                    System.out.println(e);
                }
            }
            psLike.executeBatch();
            con.commit();
            System.out.println(" Like 表导入完成");
        } catch (IOException | CsvValidationException e) {
            e.printStackTrace();
        }
    }

    /* ===================== 工具方法 ===================== */
    private static final Map<String,Integer> ING_CACHE = new HashMap<>();
    private static final Map<String,Integer> KEY_CACHE = new HashMap<>();

    /* 拆分 CSV 行 */
    // private static String[] splitCsvLine(String line) {
    //     return line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
    // }
    // private static String[] splitCsvLine(String line) throws IOException {
    // CSVFormat format = CSVFormat.DEFAULT
    //         .builder()
    //         .setEscape('\\')
    //         .setIgnoreSurroundingSpaces(true)
    //         .build();

    //     try (CSVParser parser = CSVParser.parse(line, format)) {
    //         CSVRecord record = parser.getRecords().get(0);
    //         String[] arr = new String[record.size()];
    //         for (int i = 0; i < record.size(); i++) arr[i] = record.get(i);
    //         return arr;
    //     }
    // }

    /* 拆多值字段 */
    // private static Set<String> splitParts(String src) {
    //     if (src == null || src.isBlank()) return Set.of();
    //     // 移除最外层的引号和c( 、 )字符
    //     src = src.replaceAll("^c\\(|\\)$", "");
    //     System.out.println(src);
    //     // 移除成对的双引号
    //     src = src.replace("\"\"", "\"");
    //     return Arrays.stream(src.split(","))
    //                  .map(String::trim)
    //                  .filter(s -> !s.isEmpty())
    //                  .collect(Collectors.toCollection(LinkedHashSet::new));
    // }
    private static Set<String> splitParts(String src) {
        if (src == null || src.isBlank()) return Set.of();

        // 移除最外层的 c(...) 以及多余的引号
        src = src.replaceAll("^c\\(?(\"?)", "");
        src = src.replaceAll("(\"?)\\)$", "");

        // 移除成对的 ""
        // src = src.replace("\"\"", "\"");
        src = src.replace("\"\"", "");
        src = src.replace("\"", "");

        // 如果字符串以逗号开头或结尾，移除
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
        s = s.substring(2); // 去掉 PT
        if (s.contains("D")) { String[] sp = s.split("D"); minutes += Integer.parseInt(sp[0]) * 24 * 60; s = sp[1]; }
        if (s.contains("H")) { String[] sp = s.split("H"); minutes += Integer.parseInt(sp[0]) * 60;       s = sp.length > 1 ? sp[1] : ""; }
        if (s.contains("M")) { String[] sp = s.split("M"); minutes += Integer.parseInt(sp[0]); }
        return minutes;
    }

    private static String nullify(String s) { return (s == null || s.isBlank() || "null".equalsIgnoreCase(s)) ? null : s; }
    private static int    parseIntOr0(String s)   { try { return Integer.parseInt(s); } catch (Exception e) { return 0; } }
    private static double parseDoubleOr0(String s){ try { return Double.parseDouble(s); } catch (Exception e) { return 0.0; } }
    // private static LocalDateTime parseTimestamp(String s){ return LocalDateTime.parse(s, DateTimeFormatter.ISO_INSTANT); }
    private static LocalDateTime parseTimestamp(String s) {
       Instant instant = Instant.parse(s);
        return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }
}