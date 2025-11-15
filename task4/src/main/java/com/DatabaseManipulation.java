package com;
import java.sql.*;
import java.time.Instant;
import java.util.stream.Collectors;
import java.util.*;

public class DatabaseManipulation implements DataManipulation {
    private Connection con = null;
    private ResultSet resultSet;

    private String host = "localhost";
    private String dbname = "project1_copy";
    private String user = "postgres";
    private String pwd = "123456";
    private String port = "5432";


    private void getConnection() {
        try {
            Class.forName("org.postgresql.Driver");

        } catch (Exception e) {
            System.err.println("Cannot find the PostgreSQL driver. Check CLASSPATH.");
            System.exit(1);
        }

        try {
            String url = "jdbc:postgresql://" + host + ":" + port + "/" + dbname;
            con = DriverManager.getConnection(url, user, pwd);

        } catch (SQLException e) {
            System.err.println("Database connection failed");
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }


    private void closeConnection() {
        if (con != null) {
            try {
                con.close();
                con = null;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public String findRecipeById(int recipeId) {
        getConnection();
        StringBuilder sb = new StringBuilder();
        
        String sql = "SELECT r.id, r.name, r.author_id, r.date_published, r.description, r.category, " +
                "r.aggregated_rating, r.review_count, r.instructions, " +
                "n.calories, n.fat_content, n.saturated_fat, n.cholesterol, n.sodium, " +
                "n.carbohydrate, n.fiber, n.sugar, n.protein, " +
                "s.servings, s.yield, " +
                "t.cook_time_minutes, t.prep_time_minutes, t.total_time_minutes, " +
                "STRING_AGG(DISTINCT i.name, ';') AS ingredients, " +
                "STRING_AGG(DISTINCT k.text, ';') AS keywords, " +
                "COUNT(DISTINCT f.author_id) AS favorite_count " +
                "FROM Recipe r " +
                "LEFT JOIN Nutritional_Information n ON r.id = n.recipe_id " +
                "LEFT JOIN Serving_Information s ON r.id = s.recipe_id " +
                "LEFT JOIN Time_Information t ON r.id = t.recipe_id " +
                "LEFT JOIN Recipe_Ingredient ri ON r.id = ri.recipe_id " +
                "LEFT JOIN Ingredient i ON ri.ingredient_id = i.id " +
                "LEFT JOIN Recipe_Keyword rk ON r.id = rk.recipe_id " +
                "LEFT JOIN Keyword k ON rk.keyword_id = k.id " +
                "LEFT JOIN Favorite f ON r.id = f.recipe_id " +
                "WHERE r.id = ? " +
                "GROUP BY r.id, r.name, r.author_id, r.date_published, r.description, r.category, " +
                "r.aggregated_rating, r.review_count, r.instructions, " +
                "n.calories, n.fat_content, n.saturated_fat, n.cholesterol, n.sodium, " +
                "n.carbohydrate, n.fiber, n.sugar, n.protein, " +
                "s.servings, s.yield, " +
                "t.cook_time_minutes, t.prep_time_minutes, t.total_time_minutes";
        
        try {
            PreparedStatement pstmt = con.prepareStatement(sql);
            pstmt.setInt(1, recipeId);
            resultSet = pstmt.executeQuery();
            
            if (resultSet.next()) {
                // 基础信息
                sb.append("================== Recipe Information ==================\n");
                sb.append(String.format("Recipe ID: %d\n", resultSet.getInt("id")));
                sb.append(String.format("Name: %s\n", resultSet.getString("name")));
                sb.append(String.format("Author ID: %d\n", resultSet.getInt("author_id")));
                sb.append(String.format("Date Published: %s\n", resultSet.getTimestamp("date_published")));
                sb.append(String.format("Category: %s\n", resultSet.getString("category")));
                sb.append(String.format("Description: %s\n", resultSet.getString("description")));
                sb.append(String.format("Rating: %.2f\n", resultSet.getDouble("aggregated_rating")));
                sb.append(String.format("Review Count: %d\n", resultSet.getInt("review_count")));
                sb.append(String.format("Favorite Count: %d\n", resultSet.getInt("favorite_count")));
                
                // 时间信息
                sb.append("\n================== Time Information ==================\n");
                sb.append(String.format("Cook Time: %d minutes\n", resultSet.getInt("cook_time_minutes")));
                sb.append(String.format("Prep Time: %d minutes\n", resultSet.getInt("prep_time_minutes")));
                sb.append(String.format("Total Time: %d minutes\n", resultSet.getInt("total_time_minutes")));
                
                // 份量信息
                sb.append("\n================== Serving Information ==================\n");
                sb.append(String.format("Servings: %d\n", resultSet.getInt("servings")));
                sb.append(String.format("Yield: %s\n", resultSet.getString("yield")));
                
                // 营养信息
                sb.append("\n================== Nutritional Information ==================\n");
                sb.append(String.format("Calories: %.2f kcal\n", resultSet.getDouble("calories")));
                sb.append(String.format("Fat: %.2f g\n", resultSet.getDouble("fat_content")));
                sb.append(String.format("Saturated Fat: %.2f g\n", resultSet.getDouble("saturated_fat")));
                sb.append(String.format("Cholesterol: %.2f mg\n", resultSet.getDouble("cholesterol")));
                sb.append(String.format("Sodium: %.2f mg\n", resultSet.getDouble("sodium")));
                sb.append(String.format("Carbohydrate: %.2f g\n", resultSet.getDouble("carbohydrate")));
                sb.append(String.format("Fiber: %.2f g\n", resultSet.getDouble("fiber")));
                sb.append(String.format("Sugar: %.2f g\n", resultSet.getDouble("sugar")));
                sb.append(String.format("Protein: %.2f g\n", resultSet.getDouble("protein")));
                
                // 食材
                sb.append("\n================== Ingredients ==================\n");
                String ingredients = resultSet.getString("ingredients");
                if (ingredients != null && !ingredients.isEmpty()) {
                    for (String ing : ingredients.split(";")) {
                        sb.append(String.format("- %s\n", ing));
                    }
                }
                
                // 关键词
                sb.append("\n================== Keywords ==================\n");
                String keywords = resultSet.getString("keywords");
                if (keywords != null && !keywords.isEmpty()) {
                    for (String kw : keywords.split(";")) {
                        sb.append(String.format("- %s\n", kw));
                    }
                }
                
                // 制作步骤
                sb.append("\n================== Instructions ==================\n");
                Set<String> InstructionsParts = splitParts(resultSet.getString("instructions"));
                for (String parts : InstructionsParts) {
                    sb.append(parts);
                }
                // sb.append(resultSet.getString("instructions"));
                
            } else {
                sb.append(String.format("Recipe not found with ID: %d\n", recipeId));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            sb.append("Error: ").append(e.getMessage());
        } finally {
            closeConnection();
        }
        
        return sb.toString();
    }

    @Override
    public String findTopRatedRecipesInCategory(String category, int topN) {
        getConnection();
        StringBuilder sb = new StringBuilder();
        long startTime = System.currentTimeMillis();
        
        // SQL查询：按类别筛选，按评分降序、评论数降序排列，取前N条
        String sql = "SELECT id, name, author_id, aggregated_rating, review_count " +
                    "FROM Recipe " +
                    "WHERE category = ? " +
                    "ORDER BY aggregated_rating DESC, review_count DESC " +
                    "LIMIT ?";
        
        try (PreparedStatement pstmt = con.prepareStatement(sql)) {
            pstmt.setString(1, category);
            pstmt.setInt(2, topN);
            
            try (ResultSet rs = pstmt.executeQuery()) {
                long queryEndTime = System.currentTimeMillis();
                
                sb.append(String.format("=== Top %d Recipes in Category: '%s' ===\n\n", topN, category));
                
                int count = 0;
                while (rs.next()) {
                    count++;
                    sb.append(String.format("Rank %d:\n", count));
                    sb.append(String.format("  Recipe ID: %d\n", rs.getInt("id")));
                    sb.append(String.format("  Name: %s\n", rs.getString("name")));
                    sb.append(String.format("  Author ID: %d\n", rs.getInt("author_id")));
                    sb.append(String.format("  Rating: %.2f\n", rs.getDouble("aggregated_rating")));
                    sb.append(String.format("  Review Count: %d\n", rs.getInt("review_count")));
                    sb.append("\n");
                }
                
                if (count == 0) {
                    sb.append(String.format("No recipes found in category: '%s'\n", category));
                }
                
                long endTime = System.currentTimeMillis();
                
                sb.append("=== Performance Metrics ===\n");
                sb.append(String.format("Query Execution Time: %d ms\n", (queryEndTime - startTime)));
                sb.append(String.format("Total Processing Time: %d ms\n", (endTime - startTime)));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            sb.append("Error: ").append(e.getMessage());
        } finally {
            closeConnection();
        }
        
        return sb.toString();
    }
    
    @Override
    public String findRecipesByIngredient(String ingredientName) {
        getConnection();
        StringBuilder sb = new StringBuilder();
        long startTime = System.currentTimeMillis();
        
        // SQL查询：通过关联表查找包含指定食材的食谱，支持不区分大小写的模糊匹配
        // 使用ILIKE实现不区分大小写搜索，%通配符支持部分匹配
        // 按评分降序、评论数降序排列结果
        String sql = "SELECT r.id, r.name, r.author_id, r.category, r.aggregated_rating, r.review_count " +
                    "FROM Recipe r " +
                    "JOIN Recipe_Ingredient ri ON r.id = ri.recipe_id " +
                    "JOIN Ingredient i ON ri.ingredient_id = i.id " +
                    "WHERE i.name ILIKE ? " +
                    "ORDER BY r.aggregated_rating DESC, r.review_count DESC";
        
        try (PreparedStatement pstmt = con.prepareStatement(sql)) {
            // 使用%通配符支持模糊匹配，如"chicken"可匹配"chicken breast"
            pstmt.setString(1, "%" + ingredientName + "%");
            
            try (ResultSet rs = pstmt.executeQuery()) {
                long queryEndTime = System.currentTimeMillis();
                
                sb.append(String.format("=== Recipes Containing Ingredient: '%s' ===\n\n", ingredientName));
                
                int count = 0;
                while (rs.next()) {
                    count++;
                    sb.append(String.format("Result %d:\n", count));
                    sb.append(String.format("  Recipe ID: %d\n", rs.getInt("id")));
                    sb.append(String.format("  Name: %s\n", rs.getString("name")));
                    sb.append(String.format("  Author ID: %d\n", rs.getInt("author_id")));
                    sb.append(String.format("  Category: %s\n", rs.getString("category")));
                    sb.append(String.format("  Rating: %.2f\n", rs.getDouble("aggregated_rating")));
                    sb.append(String.format("  Review Count: %d\n", rs.getInt("review_count")));
                    sb.append("\n");
                }
                
                if (count == 0) {
                    sb.append(String.format("No recipes found containing ingredient: '%s'\n", ingredientName));
                    sb.append("Tips:\n");
                    sb.append("  1. Check the ingredient name spelling\n");
                    sb.append("  2. Try using partial name (e.g., 'chick' for chicken)\n");
                    sb.append("  3. Use more generic terms (e.g., 'salt' instead of 'sea salt')\n");
                } else {
                    // sb.append(String.format("Total recipes found: %d\n", count));
                }
                
                long endTime = System.currentTimeMillis();
                
                sb.append("\n=== Performance Metrics ===\n");
                sb.append(String.format("Query Execution Time: %d ms\n", (queryEndTime - startTime)));
                sb.append(String.format("Total Processing Time: %d ms\n", (endTime - startTime)));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            sb.append("Database Error: ").append(e.getMessage()).append("\n");
            sb.append("SQLState: ").append(e.getSQLState()).append("\n");
        } finally {
            closeConnection();
        }
        
        return sb.toString();
    }
    
    @Override
    public String findUserRecipesWithStats(int userId) {
        getConnection();
        StringBuilder sb = new StringBuilder();
        long startTime = System.currentTimeMillis();
        
        try {
            // 第一步：获取用户信息
            String userSql = "SELECT id, name FROM \"User\" WHERE id = ?";
            try (PreparedStatement userStmt = con.prepareStatement(userSql)) {
                userStmt.setInt(1, userId);
                try (ResultSet userRs = userStmt.executeQuery()) {
                    if (userRs.next()) {
                        sb.append("================== User Information ==================\n");
                        sb.append(String.format("User ID: %d\n", userRs.getInt("id")));
                        sb.append(String.format("User Name: %s\n", userRs.getString("name")));
                    } else {
                        return String.format("User not found with ID: %d\n", userId);
                    }
                }
            }
            
            long queryStartTime = System.currentTimeMillis();
            
            // 第二步：获取用户的所有食谱及统计信息
            // 计算每个食谱的真实平均评分和评论数
            String recipesSql = """
                SELECT 
                    r.id,
                    r.name,
                    r.date_published,
                    r.category,
                    r.aggregated_rating,
                    r.review_count,
                    AVG(rev.rating) as calculated_avg_rating,
                    COUNT(rev.id) as actual_review_count
                FROM Recipe r
                LEFT JOIN Review rev ON r.id = rev.recipe_id
                WHERE r.author_id = ?
                GROUP BY r.id, r.name, r.date_published, r.category, r.aggregated_rating, r.review_count
                ORDER BY actual_review_count DESC, calculated_avg_rating DESC NULLS LAST
            """;
            
            try (PreparedStatement pstmt = con.prepareStatement(recipesSql)) {
                pstmt.setInt(1, userId);
                try (ResultSet rs = pstmt.executeQuery()) {
                    long queryEndTime = System.currentTimeMillis();
                    
                    sb.append("\n================== User Recipes & Statistics ==================\n\n");
                    
                    int totalRecipes = 0;
                    double totalRatingSum = 0.0;
                    List<RecipeWithStats> recipeList = new ArrayList<>();
                    
                    while (rs.next()) {
                        totalRecipes++;
                        double avgRating = rs.getDouble("calculated_avg_rating");
                        int actualReviewCount = rs.getInt("actual_review_count");
                        
                        if (!rs.wasNull() && actualReviewCount > 0) {
                            totalRatingSum += avgRating;
                        }
                        
                        RecipeWithStats stats = new RecipeWithStats(
                            rs.getInt("id"),
                            rs.getString("name"),
                            rs.getString("category"),
                            rs.getTimestamp("date_published"),
                            rs.getInt("review_count"),
                            avgRating,
                            actualReviewCount
                        );
                        recipeList.add(stats);
                    }
                    
                    if (totalRecipes == 0) {
                        sb.append("No recipes found for this user.\n");
                    } else {
                        // 显示每个食谱的详细信息
                        for (int i = 0; i < recipeList.size(); i++) {
                            RecipeWithStats recipe = recipeList.get(i);
                            sb.append(String.format("Recipe %d:\n", i + 1));
                            sb.append(String.format("  ID: %d\n", recipe.id));
                            sb.append(String.format("  Name: %s\n", recipe.name));
                            sb.append(String.format("  Category: %s\n", recipe.category));
                            sb.append(String.format("  Date Published: %s\n", recipe.datePublished));
                            sb.append(String.format("  Aggregated Rating: %.2f\n", recipe.aggregatedRating));
                            sb.append(String.format("  Review Count (from Recipe table): %d\n", recipe.reviewCount));
                            sb.append(String.format("  Actual Review Count (calculated): %d\n", recipe.actualReviewCount));
                            sb.append(String.format("  Calculated Average Rating: %.2f%s\n", 
                                recipe.avgRating,
                                recipe.actualReviewCount == 0 ? " (No reviews)" : ""));
                            sb.append("\n");
                        }
                        
                        // 汇总统计
                        sb.append("================== Summary Statistics ==================\n");
                        sb.append(String.format("Total Recipes Published: %d\n", totalRecipes));
                        sb.append(String.format("Average Rating Across All Recipes: %.2f\n", 
                            totalRatingSum / totalRecipes));
                        sb.append(String.format("Total Reviews Received: %d\n", 
                            recipeList.stream().mapToInt(r -> r.actualReviewCount).sum()));
                    }
                    
                    long endTime = System.currentTimeMillis();
                    
                    sb.append("\n=== Performance Metrics ===\n");
                    sb.append(String.format("User Query Time: %d ms\n", (queryStartTime - startTime)));
                    sb.append(String.format("Recipe Query Execution Time: %d ms\n", (queryEndTime - queryStartTime)));
                    sb.append(String.format("Total Processing Time: %d ms\n", (endTime - startTime)));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            sb.append("Database Error: ").append(e.getMessage()).append("\n");
            sb.append("SQLState: ").append(e.getSQLState()).append("\n");
        } finally {
            closeConnection();
        }
        
        return sb.toString();
    }

    @Override
    public String searchRecipesByKeyword(String keyword) {
        getConnection();
        StringBuilder sb = new StringBuilder();
        long startTime = System.currentTimeMillis();
        
        // SQL查询：在多个字段中搜索关键词
        // 使用ILIKE实现不区分大小写的模糊匹配
        // 搜索字段包括：名称、描述、制作步骤和关键词
        // 使用DISTINCT去除重复结果
        String sql = """
            SELECT DISTINCT
                r.id,
                r.name,
                r.author_id,
                r.category,
                r.aggregated_rating,
                r.review_count,
                r.date_published
            FROM Recipe r
            LEFT JOIN Recipe_Keyword rk ON r.id = rk.recipe_id
            LEFT JOIN Keyword k ON rk.keyword_id = k.id
            WHERE r.name ILIKE ? 
            OR r.description ILIKE ?
            OR r.instructions ILIKE ?
            OR k.text ILIKE ?
            ORDER BY r.aggregated_rating DESC, r.review_count DESC
        """;
        
        try (PreparedStatement pstmt = con.prepareStatement(sql)) {
            // 为每个搜索条件设置参数，使用%通配符进行模糊匹配
            String searchPattern = "%" + keyword + "%";
            pstmt.setString(1, searchPattern);
            pstmt.setString(2, searchPattern);
            pstmt.setString(3, searchPattern);
            pstmt.setString(4, searchPattern);
            
            try (ResultSet rs = pstmt.executeQuery()) {
                long queryEndTime = System.currentTimeMillis();
                
                sb.append(String.format("=== Recipes Containing Keyword: '%s' ===\n\n", keyword));
                
                int count = 0;
                while (rs.next()) {
                    count++;
                    sb.append(String.format("Result %d:\n", count));
                    sb.append(String.format("  Recipe ID: %d\n", rs.getInt("id")));
                    sb.append(String.format("  Name: %s\n", rs.getString("name")));
                    sb.append(String.format("  Author ID: %d\n", rs.getInt("author_id")));
                    sb.append(String.format("  Category: %s\n", rs.getString("category")));
                    sb.append(String.format("  Rating: %.2f\n", rs.getDouble("aggregated_rating")));
                    sb.append(String.format("  Review Count: %d\n", rs.getInt("review_count")));
                    sb.append(String.format("  Date Published: %s\n", rs.getTimestamp("date_published")));
                    sb.append("\n");
                }
                
                if (count == 0) {
                    sb.append(String.format("No recipes found containing keyword: '%s'\n", keyword));
                    sb.append("Tips:\n");
                    sb.append("  1. Try a shorter or more general keyword\n");
                    sb.append("  2. Check for spelling errors\n");
                    sb.append("  3. Use singular form (e.g., 'cake' instead of 'cakes')\n");
                } else {
                    sb.append(String.format("Total recipes found: %d\n", count));
                }
                
                long endTime = System.currentTimeMillis();
                
                sb.append("\n=== Performance Metrics ===\n");
                sb.append(String.format("Query Execution Time: %d ms\n", (queryEndTime - startTime)));
                sb.append(String.format("Total Processing Time: %d ms\n", (endTime - startTime)));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            sb.append("Database Error: ").append(e.getMessage()).append("\n");
            sb.append("SQLState: ").append(e.getSQLState()).append("\n");
        } finally {
            closeConnection();
        }
        
        return sb.toString();
    }
    
    @Override
    public int insertUser(String userData) {
        getConnection();
        int affectedRows = 0;
        
        try {
            // 解析输入字符串: "id;name;gender;age"
            String[] fields = userData.split(";", -1);
            
            if (fields.length != 4) {
                throw new IllegalArgumentException("Invalid user data format. Expected: id;name;gender;age");
            }
            
            // 验证并转换字段
            int id = Integer.parseInt(fields[0].trim());
            String name = fields[1].trim();
            String gender = fields[2].trim();
            int age = parseIntOrDefault(fields[3].trim(), 0);
            
            // 验证gender值
            if (!gender.equalsIgnoreCase("Male") && !gender.equalsIgnoreCase("Female")) {
                System.err.println("Warning: Gender should be 'Male' or 'Female', but got: " + gender);
            }
            
            // 插入用户，followers_count 和 following_count 默认为 0
            String sql = "INSERT INTO \"User\" (id, name, gender, age, followers_count, following_count) VALUES (?, ?, ?, ?, 0, 0)";
            
            try (PreparedStatement pstmt = con.prepareStatement(sql)) {
                pstmt.setInt(1, id);
                pstmt.setString(2, name);
                pstmt.setString(3, gender);
                pstmt.setInt(4, age);
                
                affectedRows = pstmt.executeUpdate();
                
                if (affectedRows > 0) {
                    System.out.println("Successfully inserted user with ID: " + id);
                }
            }
        } catch (SQLException e) {
            System.err.println("Database error while inserting user: " + e.getMessage());
            e.printStackTrace();
        } catch (NumberFormatException e) {
            System.err.println("Invalid number format in user data: " + e.getMessage());
        } finally {
            closeConnection();
        }
        
        return affectedRows;
    }

    @Override
    public int InsertReviews(String reviewDataList) {
        getConnection();
        int affectedRows = 0;
        
        try {
            // 解析输入字符串: "id;recipeId;authorId;rating;content;date"
            String[] fields = reviewDataList.split(";", -1);
            
            if (fields.length != 6) {
                throw new IllegalArgumentException("Invalid review data format. Expected: id;recipeId;authorId;rating;content;date");
            }
            
            // 验证并转换字段
            int id = Integer.parseInt(fields[0].trim());
            int recipeId = Integer.parseInt(fields[1].trim());
            int authorId = Integer.parseInt(fields[2].trim());
            double rating = Double.parseDouble(fields[3].trim());
            String content = fields[4].trim();
            
            // 将日期字符串转换为 Timestamp
            String dateSubmittedStr = fields[5].trim();
            Timestamp dateSubmitted = null;
            try {
                // 解析 ISO 8601 格式，例如: "2023-10-27T10:30:00Z"
                Instant instant = Instant.parse(dateSubmittedStr);
                dateSubmitted = Timestamp.from(instant);
            } catch (Exception e) {
                System.err.println("Invalid date format: " + dateSubmittedStr + ", using current time");
                dateSubmitted = new Timestamp(System.currentTimeMillis());
            }
            
            // 验证rating范围
            if (rating < 0.0 || rating > 5.0) {
                System.err.println("Warning: Rating should be between 0 and 5, but got: " + rating);
            }
            
            // 插入评论
            String sql = "INSERT INTO Review (id, recipe_id, author_id, rating, content, date_submitted, date_modified) VALUES (?, ?, ?, ?, ?, ?, ?)";
            
            try (PreparedStatement pstmt = con.prepareStatement(sql)) {
                pstmt.setInt(1, id);
                pstmt.setInt(2, recipeId);
                pstmt.setInt(3, authorId);
                pstmt.setDouble(4, rating);
                pstmt.setString(5, content);
                pstmt.setTimestamp(6, dateSubmitted);
                pstmt.setTimestamp(7, dateSubmitted);
                
                affectedRows = pstmt.executeUpdate();
                
                if (affectedRows > 0) {
                    System.out.println("Successfully inserted review with ID: " + id);
                }
            }
        } catch (SQLException e) {
            System.err.println("Database error while inserting review: " + e.getMessage());
            e.printStackTrace();
        } catch (NumberFormatException e) {
            System.err.println("Invalid number format in review data: " + e.getMessage());
        } finally {
            closeConnection();
        }
        
        return affectedRows;
    }

    @Override
    public int deleteRecipeById(int recipeId) {
        getConnection();
        int affectedRows = 0;
        long startTime = System.currentTimeMillis();
        
        try {
            // Start transaction
            con.setAutoCommit(false);
            
            // Delete from Like table (via Reviews of this Recipe)
            String deleteLikesSql = "DELETE FROM \"Like\" WHERE review_id IN (SELECT id FROM Review WHERE recipe_id = ?)";
            try (PreparedStatement pstmt = con.prepareStatement(deleteLikesSql)) {
                pstmt.setInt(1, recipeId);
                int likesDeleted = pstmt.executeUpdate();
            }
            
            // Delete from Review table
            String deleteReviewsSql = "DELETE FROM Review WHERE recipe_id = ?";
            try (PreparedStatement pstmt = con.prepareStatement(deleteReviewsSql)) {
                pstmt.setInt(1, recipeId);
                int reviewsDeleted = pstmt.executeUpdate();
            }
            
            // Delete from Recipe_Ingredient table
            String deleteRecipeIngredientsSql = "DELETE FROM Recipe_Ingredient WHERE recipe_id = ?";
            try (PreparedStatement pstmt = con.prepareStatement(deleteRecipeIngredientsSql)) {
                pstmt.setInt(1, recipeId);
                pstmt.executeUpdate();
            }
            
            // Delete from Recipe_Keyword table
            String deleteRecipeKeywordsSql = "DELETE FROM Recipe_Keyword WHERE recipe_id = ?";
            try (PreparedStatement pstmt = con.prepareStatement(deleteRecipeKeywordsSql)) {
                pstmt.setInt(1, recipeId);
                pstmt.executeUpdate();
            }
            
            // Delete from Favorite table
            String deleteFavoritesSql = "DELETE FROM Favorite WHERE recipe_id = ?";
            try (PreparedStatement pstmt = con.prepareStatement(deleteFavoritesSql)) {
                pstmt.setInt(1, recipeId);
                pstmt.executeUpdate();
            }
            
            // Delete from Nutritional_Information table
            String deleteNutritionalSql = "DELETE FROM Nutritional_Information WHERE recipe_id = ?";
            try (PreparedStatement pstmt = con.prepareStatement(deleteNutritionalSql)) {
                pstmt.setInt(1, recipeId);
                pstmt.executeUpdate();
            }
            
            // Delete from Serving_Information table
            String deleteServingSql = "DELETE FROM Serving_Information WHERE recipe_id = ?";
            try (PreparedStatement pstmt = con.prepareStatement(deleteServingSql)) {
                pstmt.setInt(1, recipeId);
                pstmt.executeUpdate();
            }
            
            // Delete from Time_Information table
            String deleteTimeSql = "DELETE FROM Time_Information WHERE recipe_id = ?";
            try (PreparedStatement pstmt = con.prepareStatement(deleteTimeSql)) {
                pstmt.setInt(1, recipeId);
                pstmt.executeUpdate();
            }
            
            // Finally delete from Recipe table
            String deleteRecipeSql = "DELETE FROM Recipe WHERE id = ?";
            try (PreparedStatement pstmt = con.prepareStatement(deleteRecipeSql)) {
                pstmt.setInt(1, recipeId);
                affectedRows = pstmt.executeUpdate();
            }
            
            // Commit transaction
            con.commit();
            System.out.println("Successfully deleted recipe " + recipeId + " and all related data");
            
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                if (con != null) {
                    con.rollback();
                    System.err.println("Transaction rolled back due to error");
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            affectedRows = 0;
        } finally {
            // Reset auto commit
            try {
                if (con != null) {
                    con.setAutoCommit(true);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            closeConnection();
        }
        
        long endTime = System.currentTimeMillis();
        System.out.println("Delete operation completed in " + (endTime - startTime) + " ms");
        
        return affectedRows;
    }

    @Override
    public int updateReviewLike(String likeData) {
        getConnection();
        int affectedRows = 0;
        long startTime = System.currentTimeMillis();
        
        try {
            // 解析输入字符串: "reviewId;userId;action"
            String[] fields = likeData.split(";", -1);
            
            if (fields.length != 3) {
                throw new IllegalArgumentException("Invalid like data format. Expected: reviewId;userId;action");
            }
            
            int reviewId = Integer.parseInt(fields[0].trim());
            int userId = Integer.parseInt(fields[1].trim());
            String action = fields[2].trim().toLowerCase();
            
            if (!action.equals("like") && !action.equals("unlike")) {
                throw new IllegalArgumentException("Invalid action. Must be 'like' or 'unlike'");
            }
            
            con.setAutoCommit(false);
            
            if (action.equals("like")) {
                // 点赞：INSERT INTO "Like" 表
                String sql = "INSERT INTO \"Like\" (author_id, review_id) VALUES (?, ?)";
                try (PreparedStatement pstmt = con.prepareStatement(sql)) {
                    pstmt.setInt(1, userId);
                    pstmt.setInt(2, reviewId);
                    affectedRows = pstmt.executeUpdate();
                    System.out.println("User " + userId + " liked review " + reviewId);
                } catch (SQLException e) {
                    // 处理重复点赞（唯一约束冲突）
                    if (e.getSQLState().equals("23505")) { // PostgreSQL duplicate key error code
                        System.out.println("User " + userId + " already liked review " + reviewId);
                        affectedRows = 0; // 返回0表示没有新插入
                    } else {
                        throw e;
                    }
                }
            } else {
                // 取消点赞：DELETE FROM "Like" 表
                String sql = "DELETE FROM \"Like\" WHERE author_id = ? AND review_id = ?";
                try (PreparedStatement pstmt = con.prepareStatement(sql)) {
                    pstmt.setInt(1, userId);
                    pstmt.setInt(2, reviewId);
                    affectedRows = pstmt.executeUpdate();
                    if (affectedRows > 0) {
                        System.out.println("User " + userId + " unliked review " + reviewId);
                    } else {
                        System.out.println("User " + userId + " had not liked review " + reviewId);
                    }
                }
            }
            
            con.commit();
            
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                if (con != null) {
                    con.rollback();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            affectedRows = -1; // 返回-1表示出错
        } catch (Exception e) {
            e.printStackTrace();
            affectedRows = -1;
        } finally {
            try {
                if (con != null) {
                    con.setAutoCommit(true);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            closeConnection();
        }
        
        long endTime = System.currentTimeMillis();
        System.out.println("updateReviewLike operation completed in " + (endTime - startTime) + " ms");
        
        return affectedRows;
    }




    // 辅助方法：安全解析整数，为空时返回默认值
    private int parseIntOrDefault(String value, int defaultValue) {
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            System.err.println("Warning: Cannot parse '" + value + "' as integer, using default value " + defaultValue);
            return defaultValue;
        }
    }

    // 在 DatabaseManipulation 类中添加辅助类
    private static class RecipeWithStats {
        final int id;
        final String name;
        final String category;
        final Timestamp datePublished;
        final int reviewCount;
        final double avgRating;
        final int actualReviewCount;
        final double aggregatedRating;
        
        RecipeWithStats(int id, String name, String category, Timestamp datePublished, 
                        int reviewCount, double avgRating, int actualReviewCount) {
            this.id = id;
            this.name = name;
            this.category = category;
            this.datePublished = datePublished;
            this.reviewCount = reviewCount;
            this.avgRating = avgRating;
            this.actualReviewCount = actualReviewCount;
            this.aggregatedRating = 0.0; // 从结果集中获取
        }
    }
    
    /**
     * 解析CSV中的多值字段（支持c("a","b")或a,b格式）
     */
    private Set<String> splitParts(String src) {
        if (src == null || src.isBlank()) return new HashSet<>();
        
        src = src.replaceAll("^c\\(?(\"?)", "");
        src = src.replaceAll("(\"?)\\)$", "");
        src = src.replace("\"\"", "");
        src = src.replace("\"", "");
        src = src.trim();

        // 检查并移除 c( 和 )
        if (src.startsWith("c(") && src.endsWith(")")) {
            src = src.substring(2, src.length() - 1);
        }
        
        if (src.startsWith(",")) src = src.substring(1).trim();
        if (src.endsWith(",")) src = src.substring(0, src.length() - 1).trim();
        
        if (src.isEmpty()) return new HashSet<>();
        
        return Arrays.stream(src.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }
    
}
