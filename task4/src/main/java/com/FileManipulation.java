package com;
import java.io.*;
import java.util.*;
import java.util.stream.*;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180ParserBuilder;
import com.opencsv.exceptions.CsvValidationException;

public class FileManipulation implements DataManipulation {
    
    // ===================== 配置常量 =====================
    private static final String CSV_BASE_PATH = "./csv/";
    private static final String RECIPES_FILE = CSV_BASE_PATH + "recipes_checktime.csv";
    private static final String USERS_FILE = CSV_BASE_PATH + "user_fixed.csv";
    private static final String REVIEWS_FILE = CSV_BASE_PATH + "reviews_recipeid2int.csv";
    
    // ===================== 核心方法实现 =====================
    
    @Override
    public String findRecipeById(int recipeId) {
        StringBuilder sb = new StringBuilder();
        
        try (CSVReader reader = new CSVReaderBuilder(new FileReader(RECIPES_FILE))
                .withCSVParser(new RFC4180ParserBuilder().build())
                .build()) {
            
            String[] header = reader.readNext(); // 跳过表头
            String[] fields;
            
            while ((fields = reader.readNext()) != null) {
                if (fields.length == 0) continue;
                
                // 检查ID是否匹配
                if (fields[0].equals(String.valueOf(recipeId))) {
                    // 找到记录，开始格式化输出
                    sb.append("================== Recipe Information ==================\n");
                    sb.append(String.format("Recipe ID: %s\n", fields[0]));
                    sb.append(String.format("Name: %s\n", fields[1]));
                    sb.append(String.format("Author ID: %s\n", fields[2]));
                    sb.append(String.format("Date Published: %s\n", fields[7]));
                    sb.append(String.format("Category: %s\n", fields[9]));
                    sb.append(String.format("Description: %s\n", fields[8]));
                    sb.append(String.format("Rating: %s\n", fields[12]));
                    sb.append(String.format("Review Count: %s\n", fields[13]));
                    
                    // 解析收藏用户并计算数量
                    Set<String> favoriteUsers = splitParts(fields[26]);
                    sb.append(String.format("Favorite Count: %d\n", favoriteUsers.size()));
                    
                    // 时间信息
                    sb.append("\n================== Time Information ==================\n");
                    sb.append(String.format("Cook Time: %d minutes\n", parseIsoDuration(fields[4])));
                    sb.append(String.format("Prep Time: %d minutes\n", parseIsoDuration(fields[5])));
                    sb.append(String.format("Total Time: %d minutes\n", parseIsoDuration(fields[6])));
                    
                    // 份量信息
                    sb.append("\n================== Serving Information ==================\n");
                    sb.append(String.format("Servings: %s\n", fields[23]));
                    sb.append(String.format("Yield: %s\n", fields[24]));
                    
                    // 营养信息
                    sb.append("\n================== Nutritional Information ==================\n");
                    sb.append(String.format("Calories: %s kcal\n", fields[14]));
                    sb.append(String.format("Fat: %s g\n", fields[15]));
                    sb.append(String.format("Saturated Fat: %s g\n", fields[16]));
                    sb.append(String.format("Cholesterol: %s mg\n", fields[17]));
                    sb.append(String.format("Sodium: %s mg\n", fields[18]));
                    sb.append(String.format("Carbohydrate: %s g\n", fields[19]));
                    sb.append(String.format("Fiber: %s g\n", fields[20]));
                    sb.append(String.format("Sugar: %s g\n", fields[21]));
                    sb.append(String.format("Protein: %s g\n", fields[22]));
                    
                    // 食材列表
                    sb.append("\n================== Ingredients ==================\n");
                    Set<String> ingredients = splitParts(fields[11]);
                    for (String ing : ingredients) {
                        if (!ing.isEmpty()) {
                            sb.append(String.format("- %s\n", ing));
                        }
                    }
                    
                    // 关键词列表
                    sb.append("\n================== Keywords ==================\n");
                    Set<String> keywords = splitParts(fields[10]);
                    for (String kw : keywords) {
                        if (!kw.isEmpty()) {
                            sb.append(String.format("- %s\n", kw));
                        }
                    }
                    
                    // 制作步骤
                    sb.append("\n================== Instructions ==================\n");
                    Set<String> InstructionsParts = splitParts(fields[25]);
                    for (String parts : InstructionsParts) {
                        sb.append(parts);
                    }
                    // sb.append(fields[25]);
                    
                    return sb.toString();
                }
            }
            
            // 未找到食谱
            sb.append(String.format("Recipe not found with ID: %d\n", recipeId));
            
        } catch (IOException | CsvValidationException e) {
            e.printStackTrace();
            sb.append("Error: ").append(e.getMessage());
        }
        
        return sb.toString();
    }
    
    @Override
    public String findTopRatedRecipesInCategory(String category, int topN) {
        StringBuilder sb = new StringBuilder();
        long startTime = System.currentTimeMillis();
        
        // 使用最小堆维护topN个食谱，按评分和评论数排序
        PriorityQueue<RecipeInfo> minHeap = new PriorityQueue<>((a, b) -> {
            if (a.rating != b.rating) {
                return Double.compare(a.rating, b.rating);
            }
            return Integer.compare(a.reviewCount, b.reviewCount);
        });
        
        try (CSVReader reader = new CSVReaderBuilder(new FileReader(RECIPES_FILE))
                .withCSVParser(new RFC4180ParserBuilder().build())
                .build()) {
            
            String[] header = reader.readNext(); // 跳过表头
            String[] fields;
            int processedCount = 0;
            int matchedCount = 0;
            
            while ((fields = reader.readNext()) != null) {
                if (fields.length == 0) continue;
                processedCount++;
                
                // 检查类别是否匹配（忽略大小写并去除空格）
                String recipeCategory = fields[9].trim();
                if (!recipeCategory.equalsIgnoreCase(category.trim())) {
                    continue;
                }
                
                matchedCount++;
                
                // 解析字段，处理可能的空值
                try {
                    int recipeId = Integer.parseInt(fields[0]);
                    String name = fields[1];
                    int authorId = fields[2].isBlank() ? 0 : Integer.parseInt(fields[2]);
                    double rating = fields[12].isBlank() ? 0.0 : Double.parseDouble(fields[12]);
                    int reviewCount = fields[13].isBlank() ? 0 : Integer.parseInt(fields[13]);
                    
                    // 添加到最小堆
                    RecipeInfo recipe = new RecipeInfo(recipeId, name, authorId, rating, reviewCount);
                    minHeap.offer(recipe);
                    
                    // 如果堆大小超过topN，移除评分最低的
                    if (minHeap.size() > topN) {
                        minHeap.poll();
                    }
                } catch (NumberFormatException e) {
                    System.err.println("跳过格式错误的行 (ID: " + fields[0] + "): " + e.getMessage());
                } catch (Exception e) {
                    System.err.println("跳过行: " + Arrays.toString(fields) + " - " + e.getMessage());
                }
                
                // if (processedCount % 10000 == 0) {
                //     System.out.println("已处理 " + processedCount + " 行，匹配 " + matchedCount + " 行...");
                // }
            }
            
            long fileReadEndTime = System.currentTimeMillis();
            
            // 将堆内容转换为列表并按评分降序排序
            List<RecipeInfo> topRecipes = new ArrayList<>(minHeap);
            topRecipes.sort((a, b) -> {
                if (a.rating != b.rating) {
                    return Double.compare(b.rating, a.rating); // 降序
                }
                return Integer.compare(b.reviewCount, a.reviewCount); // 降序
            });
            
            sb.append(String.format("=== Top %d Recipes in Category: '%s' ===\n\n", topN, category));
            
            int resultCount = topRecipes.size();
            for (int i = 0; i < resultCount; i++) {
                RecipeInfo recipe = topRecipes.get(i);
                sb.append(String.format("Rank %d:\n", i + 1));
                sb.append(String.format("  Recipe ID: %d\n", recipe.id));
                sb.append(String.format("  Name: %s\n", recipe.name));
                sb.append(String.format("  Author ID: %d\n", recipe.authorId));
                sb.append(String.format("  Rating: %.2f\n", recipe.rating));
                sb.append(String.format("  Review Count: %d\n", recipe.reviewCount));
                sb.append("\n");
            }
            
            if (resultCount == 0) {
                sb.append(String.format("No recipes found in category: '%s'\n", category));
            }
            
            long endTime = System.currentTimeMillis();
            
            sb.append("=== Performance Metrics ===\n");
            sb.append(String.format("File Read & Filter Time: %d ms\n", (fileReadEndTime - startTime)));
            sb.append(String.format("Total Processing Time: %d ms\n", (endTime - startTime)));
            // sb.append(String.format("Total Rows Processed: %d\n", processedCount));
            // sb.append(String.format("Matched Rows: %d\n", matchedCount));
            
        } catch (IOException | CsvValidationException e) {
            e.printStackTrace();
            sb.append("Error: ").append(e.getMessage());
        }
        
        return sb.toString();
    }

    @Override
    public String findRecipesByIngredient(String ingredientName) {
        StringBuilder sb = new StringBuilder();
        long startTime = System.currentTimeMillis();
        String targetIngredient = ingredientName.toLowerCase().trim();
        
        // 使用列表存储匹配结果（文件I/O需要全量扫描）
        List<RecipeIngredientMatch> matches = new ArrayList<>();
        
        try (CSVReader reader = new CSVReaderBuilder(new FileReader(RECIPES_FILE))
                .withCSVParser(new RFC4180ParserBuilder().build())
                .build()) {
            
            String[] header = reader.readNext(); // 跳过表头
            String[] fields;
            int processedCount = 0;
            int matchedCount = 0;
            
            while ((fields = reader.readNext()) != null) {
                if (fields.length < 12) continue; // 确保有足够列
                processedCount++;
                
                // 检查食材字段是否匹配
                String ingredientField = fields[11].trim(); // RecipeIngredientParts列
                if (ingredientField.isEmpty()) continue;
                
                // 解析食材列表并检查是否包含目标食材（不区分大小写）
                Set<String> ingredients = splitParts(ingredientField);
                boolean found = false;
                
                for (String ing : ingredients) {
                    if (ing.toLowerCase().contains(targetIngredient)) {
                        found = true;
                        break;
                    }
                }
                
                if (!found) continue;
                
                matchedCount++;
                
                // 解析匹配的食谱信息
                try {
                    int recipeId = Integer.parseInt(fields[0]);
                    String name = fields[1];
                    int authorId = fields[2].isBlank() ? 0 : Integer.parseInt(fields[2]);
                    String category = fields[9];
                    double rating = fields[12].isBlank() ? 0.0 : Double.parseDouble(fields[12]);
                    int reviewCount = fields[13].isBlank() ? 0 : Integer.parseInt(fields[13]);
                    
                    matches.add(new RecipeIngredientMatch(recipeId, name, authorId, category, rating, reviewCount, ingredients));
                } catch (NumberFormatException e) {
                    System.err.println("跳过格式错误的行 (ID: " + fields[0] + "): " + e.getMessage());
                }
                
                // if (processedCount % 10000 == 0) {
                //     System.out.println("已处理 " + processedCount + " 行，匹配 " + matchedCount + " 行...");
                // }
            }
            
            long fileReadEndTime = System.currentTimeMillis();
            
            // 按评分降序、评论数降序排序
            matches.sort((a, b) -> {
                if (a.rating != b.rating) {
                    return Double.compare(b.rating, a.rating); // 降序
                }
                return Integer.compare(b.reviewCount, a.reviewCount); // 降序
            });
            
            sb.append(String.format("=== Recipes Containing Ingredient: '%s' ===\n\n", ingredientName));
            
            int resultCount = matches.size();
            for (int i = 0; i < resultCount; i++) {
                RecipeIngredientMatch recipe = matches.get(i);
                sb.append(String.format("Result %d:\n", i + 1));
                sb.append(String.format("  Recipe ID: %d\n", recipe.id));
                sb.append(String.format("  Name: %s\n", recipe.name));
                sb.append(String.format("  Author ID: %d\n", recipe.authorId));
                sb.append(String.format("  Category: %s\n", recipe.category));
                sb.append(String.format("  Rating: %.2f\n", recipe.rating));
                sb.append(String.format("  Review Count: %d\n", recipe.reviewCount));
                sb.append(String.format("  Matched Ingredients: %s\n", 
                    recipe.ingredients.stream()
                        .filter(ing -> ing.toLowerCase().contains(targetIngredient))
                        .collect(Collectors.joining(", "))));
                sb.append("\n");
            }
            
            if (resultCount == 0) {
                sb.append(String.format("No recipes found containing ingredient: '%s'\n", ingredientName));
                sb.append("Tips:\n");
                sb.append("  1. Check the ingredient name spelling\n");
                sb.append("  2. Try using partial name (e.g., 'chick' for chicken)\n");
                sb.append("  3. Available ingredients can be found in recipes.csv column 12\n");
            } else {
                // sb.append(String.format("Total recipes found: %d\n", resultCount));
            }
            
            long endTime = System.currentTimeMillis();
            
            sb.append("\n=== Performance Metrics ===\n");
            sb.append(String.format("File Read & Filter Time: %d ms\n", (fileReadEndTime - startTime)));
            sb.append(String.format("Total Processing Time: %d ms\n", (endTime - startTime)));
            // sb.append(String.format("Total Rows Processed: %d\n", processedCount));
            // sb.append(String.format("Matched Rows: %d\n", matchedCount));
            
        } catch (IOException | CsvValidationException e) {
            e.printStackTrace();
            sb.append("File Error: ").append(e.getMessage()).append("\n");
        }
        
        return sb.toString();
    }

    @Override
    public String findUserRecipesWithStats(int userId) {
        StringBuilder sb = new StringBuilder();
        long startTime = System.currentTimeMillis();
        
        try {
            // 第一步：获取用户信息
            String userName = getUserNameById(userId);
            
            sb.append("================== User Information ==================\n");
            sb.append(String.format("User ID: %d\n", userId));
            sb.append(String.format("User Name: %s\n", userName));
            
            long userReadTime = System.currentTimeMillis();
            
            // 第二步：从 recipes.csv 中获取用户的所有食谱
            List<UserRecipe> userRecipes = new ArrayList<>();
            try (CSVReader recipeReader = new CSVReaderBuilder(new FileReader(RECIPES_FILE))
                    .withCSVParser(new RFC4180ParserBuilder().build())
                    .build()) {
                
                String[] header = recipeReader.readNext(); // 跳过表头
                String[] fields;
                int processedRecipes = 0;
                
                while ((fields = recipeReader.readNext()) != null) {
                    if (fields.length < 14) continue;
                    processedRecipes++;
                    
                    String authorIdStr = fields[2].trim();
                    if (authorIdStr.isEmpty()) continue;
                    
                    int authorId;
                    try {
                        authorId = Integer.parseInt(authorIdStr);
                    } catch (NumberFormatException e) {
                        continue;
                    }
                    
                    if (authorId == userId) {
                        try {
                            int recipeId = Integer.parseInt(fields[0]);
                            String name = fields[1];
                            String category = fields[9];
                            String datePublished = fields[7];
                            int reviewCount = fields[13].isBlank() ? 0 : Integer.parseInt(fields[13]);
                            double aggregatedRating = fields[12].isBlank() ? 0.0 : Double.parseDouble(fields[12]);
                            
                            userRecipes.add(new UserRecipe(recipeId, name, category, datePublished, reviewCount, aggregatedRating));
                        } catch (Exception e) {
                            System.err.println("Error parsing recipe: " + Arrays.toString(fields) + " - " + e.getMessage());
                        }
                    }
                    
                    // if (processedRecipes % 10000 == 0) {
                    //     // System.out.println("已扫描 " + processedRecipes + " 条食谱记录...");
                    // }
                }
            }
            
            long recipeScanEndTime = System.currentTimeMillis();
            
            if (userRecipes.isEmpty()) {
                return String.format("No recipes found for user ID: %d\n", userId);
            }
            
            sb.append("\n================== User Recipes & Statistics ==================\n\n");
            
            // 第三步：从 reviews.csv 中计算每个食谱的评分统计
            // 优化：一次性加载所有 reviews 到内存中，按 recipeId 分组
            Map<Integer, List<Double>> recipeRatingsMap = loadAllReviewsByRecipe();
            
            long reviewLoadEndTime = System.currentTimeMillis();
            
            int totalRecipes = userRecipes.size();
            double totalRatingSum = 0.0;
            
            for (int i = 0; i < totalRecipes; i++) {
                UserRecipe recipe = userRecipes.get(i);
                List<Double> ratings = recipeRatingsMap.getOrDefault(recipe.id, new ArrayList<>());
                
                double avgRating = 0.0;
                int actualReviewCount = ratings.size();
                
                if (actualReviewCount > 0) {
                    avgRating = ratings.stream().mapToDouble(Double::doubleValue).average().getAsDouble();
                    totalRatingSum += avgRating;
                }
                
                sb.append(String.format("Recipe %d:\n", i + 1));
                sb.append(String.format("  ID: %d\n", recipe.id));
                sb.append(String.format("  Name: %s\n", recipe.name));
                sb.append(String.format("  Category: %s\n", recipe.category));
                sb.append(String.format("  Date Published: %s\n", recipe.datePublished));
                sb.append(String.format("  Aggregated Rating: %.2f\n", recipe.aggregatedRating));
                sb.append(String.format("  Review Count (from Recipe table): %d\n", recipe.reviewCount));
                sb.append(String.format("  Actual Review Count (calculated): %d\n", actualReviewCount));
                sb.append(String.format("  Calculated Average Rating: %.2f%s\n", 
                    avgRating, actualReviewCount == 0 ? " (No reviews)" : ""));
                sb.append("\n");
            }
            
            // 汇总统计
            sb.append("================== Summary Statistics ==================\n");
            sb.append(String.format("Total Recipes Published: %d\n", totalRecipes));
            sb.append(String.format("Average Rating Across All Recipes: %.2f\n", 
                totalRatingSum / totalRecipes));
            // sb.append(String.format("Total Reviews Received: %d\n", 
                // recipeRatingsMap.values().stream().mapToInt(List::size).sum()));
            
            long endTime = System.currentTimeMillis();
            
            sb.append("\n=== Performance Metrics ===\n");
            sb.append(String.format("User Data Load Time: %d ms\n", (userReadTime - startTime)));
            sb.append(String.format("Recipe Scan Time: %d ms\n", (recipeScanEndTime - userReadTime)));
            sb.append(String.format("Reviews Load Time: %d ms\n", (reviewLoadEndTime - recipeScanEndTime)));
            sb.append(String.format("Total Processing Time: %d ms\n", (endTime - startTime)));
            // sb.append(String.format("Recipes Found: %d\n", totalRecipes));
            
        } catch (IOException | CsvValidationException e) {
            e.printStackTrace();
            sb.append("File Error: ").append(e.getMessage()).append("\n");
        }
        
        return sb.toString();
    }

    @Override
    public String searchRecipesByKeyword(String keyword) {
        StringBuilder sb = new StringBuilder();
        long startTime = System.currentTimeMillis();
        String targetKeyword = keyword.toLowerCase().trim();
        
        // 使用列表存储匹配结果
        List<RecipeSearchResult> matches = new ArrayList<>();
        
        try (CSVReader reader = new CSVReaderBuilder(new FileReader(RECIPES_FILE))
                .withCSVParser(new RFC4180ParserBuilder().build())
                .build()) {
            
            String[] header = reader.readNext(); // 跳过表头
            String[] fields;
            int processedCount = 0;
            int matchedCount = 0;
            
            while ((fields = reader.readNext()) != null) {
                if (fields.length < 14) continue; // 确保有足够列
                processedCount++;
                
                // 检查关键词是否匹配（不区分大小写）
                String name = fields[1].toLowerCase();
                String description = fields[8].toLowerCase();
                String instructions = fields[25].toLowerCase();
                
                // 解析关键词列表
                Set<String> keywords = splitParts(fields[10]);
                boolean keywordMatch = keywords.stream().anyMatch(k -> k.toLowerCase().contains(targetKeyword));
                
                // 检查是否在名称、描述或制作步骤中包含关键词
                boolean found = name.contains(targetKeyword) 
                            || description.contains(targetKeyword) 
                            || instructions.contains(targetKeyword)
                            || keywordMatch;
                
                if (!found) continue;
                
                matchedCount++;
                
                // 解析匹配的食谱信息
                try {
                    int recipeId = Integer.parseInt(fields[0]);
                    String recipeName = fields[1];
                    int authorId = fields[2].isBlank() ? 0 : Integer.parseInt(fields[2]);
                    String category = fields[9];
                    double rating = fields[12].isBlank() ? 0.0 : Double.parseDouble(fields[12]);
                    int reviewCount = fields[13].isBlank() ? 0 : Integer.parseInt(fields[13]);
                    String datePublished = fields[7];
                    
                    matches.add(new RecipeSearchResult(recipeId, recipeName, authorId, category, rating, reviewCount, datePublished));
                } catch (NumberFormatException e) {
                    System.err.println("跳过格式错误的行 (ID: " + fields[0] + "): " + e.getMessage());
                }
                
                // if (processedCount % 10000 == 0) {
                //     System.out.println("已处理 " + processedCount + " 行，匹配 " + matchedCount + " 行...");
                // }
            }
            
            long fileReadEndTime = System.currentTimeMillis();
            
            // 按评分降序、评论数降序排序
            matches.sort((a, b) -> {
                if (a.rating != b.rating) {
                    return Double.compare(b.rating, a.rating); // 降序
                }
                return Integer.compare(b.reviewCount, a.reviewCount); // 降序
            });
            
            sb.append(String.format("=== Recipes Containing Keyword: '%s' ===\n\n", keyword));
            
            int resultCount = matches.size();
            for (int i = 0; i < resultCount; i++) {
                RecipeSearchResult recipe = matches.get(i);
                sb.append(String.format("Result %d:\n", i + 1));
                sb.append(String.format("  Recipe ID: %d\n", recipe.id));
                sb.append(String.format("  Name: %s\n", recipe.name));
                sb.append(String.format("  Author ID: %d\n", recipe.authorId));
                sb.append(String.format("  Category: %s\n", recipe.category));
                sb.append(String.format("  Rating: %.2f\n", recipe.rating));
                sb.append(String.format("  Review Count: %d\n", recipe.reviewCount));
                sb.append(String.format("  Date Published: %s\n", recipe.datePublished));
                sb.append("\n");
            }
            
            if (resultCount == 0) {
                sb.append(String.format("No recipes found containing keyword: '%s'\n", keyword));
                sb.append("Tips:\n");
                sb.append("  1. Try a shorter or more general keyword\n");
                sb.append("  2. Check for spelling errors\n");
                sb.append("  3. Use singular form (e.g., 'cake' instead of 'cakes')\n");
            } else {
                sb.append(String.format("Total recipes found: %d\n", resultCount));
            }
            
            long endTime = System.currentTimeMillis();
            
            sb.append("\n=== Performance Metrics ===\n");
            sb.append(String.format("File Read & Filter Time: %d ms\n", (fileReadEndTime - startTime)));
            sb.append(String.format("Total Processing Time: %d ms\n", (endTime - startTime)));
            sb.append(String.format("Total Rows Processed: %d\n", processedCount));
            sb.append(String.format("Matched Rows: %d\n", matchedCount));
            
        } catch (IOException | CsvValidationException e) {
            e.printStackTrace();
            sb.append("File Error: ").append(e.getMessage()).append("\n");
        }
        
        return sb.toString();
    }

    @Override
    public int insertUser(String userData) {
        int affectedRows = 0;
        
        try {
            // 解析输入字符串: "id;name;gender;age"
            String[] fields = userData.split(";", -1);
            
            if (fields.length != 4) {
                throw new IllegalArgumentException("Invalid user data format. Expected: id;name;gender;age");
            }
            
            // 提取并验证字段
            String id = fields[0].trim();
            String name = fields[1].trim();
            String gender = fields[2].trim();
            String age = fields[3].trim();
            
            // 验证必填字段
            if (id.isEmpty() || name.isEmpty() || gender.isEmpty()) {
                throw new IllegalArgumentException("ID, Name, and Gender cannot be empty");
            }
            
            // 验证gender值
            if (!gender.equalsIgnoreCase("Male") && !gender.equalsIgnoreCase("Female")) {
                System.err.println("Warning: Gender should be 'Male' or 'Female', but got: " + gender);
            }
            
            // CSV文件有8列: AuthorId,AuthorName,Gender,Age,Followers,Following,FollowerUsers,FollowingUsers
            // 新用户后四列: Followers=0, Following=0, FollowerUsers空列表, FollowingUsers空列表
            String[] csvRow = new String[] {
                id,         // AuthorId
                name,       // AuthorName
                gender,     // Gender
                age,        // Age
                "0",        // Followers (默认为0)
                "0",        // Following (默认为0)
                "",         // FollowerUsers空列表
                ""          // FollowingUsers空列表
            };
            
            // 写入CSV文件
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(USERS_FILE, true))) {
                writeCsvRow(writer, csvRow);
                affectedRows = 1;
                System.out.println("Successfully appended user with ID: " + id + " to " + USERS_FILE);
            }
            
        } catch (IOException e) {
            System.err.println("File I/O error while inserting user: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("Unexpected error: " + e.getMessage());
            e.printStackTrace();
        }
        
        return affectedRows;
    }

    @Override
    public int InsertReviews(String reviewDataList) {
        int affectedRows = 0;
        
        try {
            // 解析输入字符串: "id;recipeId;authorId;rating;content;date"
            String[] fields = reviewDataList.split(";", -1);
            
            if (fields.length != 6) {
                throw new IllegalArgumentException("Invalid review data format. Expected: id;recipeId;authorId;rating;content;date");
            }
            
            // 提取并验证字段
            String id = fields[0].trim();
            String recipeId = fields[1].trim();
            String authorId = fields[2].trim();
            String rating = fields[3].trim();
            String content = fields[4].trim();
            String dateSubmitted = fields[5].trim();
            
            // 验证必填字段
            if (id.isEmpty() || recipeId.isEmpty() || authorId.isEmpty()) {
                throw new IllegalArgumentException("ID, Recipe ID, and Author ID cannot be empty");
            }
            
            // 验证rating范围
            double ratingVal = Double.parseDouble(rating);
            if (ratingVal < 0.0 || ratingVal > 5.0) {
                System.err.println("Warning: Rating should be between 0 and 5, but got: " + ratingVal);
            }
            
            // CSV文件有9列: ReviewId,RecipeId,AuthorId,AuthorName,Rating,Review,DateSubmitted,DateModified,Likes
            // 新评论: AuthorName空, DateModified空, Likes空列表c()
            String[] csvRow = new String[] {
                id,             // ReviewId
                recipeId,       // RecipeId
                authorId,       // AuthorId
                "",             // AuthorName (空)
                rating,         // Rating
                content,        // Review
                dateSubmitted,  // DateSubmitted
                dateSubmitted,             // DateModified
                "c()"           // Likes (空列表)
            };
            
            // 写入CSV文件
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(REVIEWS_FILE, true))) {
                writeCsvRow(writer, csvRow);
                affectedRows = 1;
                System.out.println("Successfully appended review with ID: " + id + " to " + REVIEWS_FILE);
            }
            
        } catch (IOException e) {
            System.err.println("File I/O error while inserting review: " + e.getMessage());
            e.printStackTrace();
        } catch (NumberFormatException e) {
            System.err.println("Invalid number format: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Unexpected error: " + e.getMessage());
            e.printStackTrace();
        }
        
        return affectedRows;
    }

    @Override
    public int deleteRecipeById(int recipeId) {
        int affectedRows = 0;
        String recipeIdStr = String.valueOf(recipeId);
        long startTime = System.currentTimeMillis();
        
        try {
            // 1. Delete from recipes.csv
            File recipesFile = new File(RECIPES_FILE);
            File recipesTempFile = new File(RECIPES_FILE + ".tmp");
            
            try (CSVReader reader = new CSVReaderBuilder(new FileReader(recipesFile))
                    .withCSVParser(new RFC4180ParserBuilder().build())
                    .build();
                BufferedWriter writer = new BufferedWriter(new FileWriter(recipesTempFile))) {
                
                String[] header = reader.readNext();
                if (header != null) {
                    writeCsvRow(writer, header);
                }
                
                String[] fields;
                while ((fields = reader.readNext()) != null) {
                    if (fields.length == 0) continue;
                    
                    // Skip the recipe to delete
                    if (fields[0].equals(recipeIdStr)) {
                        affectedRows++;
                        continue;
                    }
                    
                    writeCsvRow(writer, fields);
                }
            }
            
            // Replace original file
            if (!recipesFile.delete() || !recipesTempFile.renameTo(recipesFile)) {
                throw new IOException("Failed to replace recipes.csv file");
            }
            
            // 2. Delete related reviews from reviews.csv
            File reviewsFile = new File(REVIEWS_FILE);
            File reviewsTempFile = new File(REVIEWS_FILE + ".tmp");
            
            try (CSVReader reader = new CSVReaderBuilder(new FileReader(reviewsFile))
                    .withCSVParser(new RFC4180ParserBuilder().build())
                    .build();
                BufferedWriter writer = new BufferedWriter(new FileWriter(reviewsTempFile))) {
                
                String[] header = reader.readNext();
                if (header != null) {
                    writeCsvRow(writer, header);
                }
                
                String[] fields;
                int deletedReviews = 0;
                while ((fields = reader.readNext()) != null) {
                    if (fields.length < 2) continue;
                    
                    // Skip reviews for the deleted recipe
                    if (fields[1].equals(recipeIdStr)) {
                        deletedReviews++;
                        continue;
                    }
                    
                    writeCsvRow(writer, fields);
                }
                
                // if (deletedReviews > 0) {
                //     System.out.println("Deleted " + deletedReviews + " related reviews");
                // }
            }
            
            // Replace original file
            if (!reviewsFile.delete() || !reviewsTempFile.renameTo(reviewsFile)) {
                throw new IOException("Failed to replace reviews.csv file");
            }
            
            long endTime = System.currentTimeMillis();
            System.out.println("Successfully deleted recipe " + recipeId + " and related data from CSV files in " + (endTime - startTime) + " ms");
            
        } catch (IOException | CsvValidationException e) {
            e.printStackTrace();
            affectedRows = 0;
        }
        
        return affectedRows;
    }

    @Override
    public int updateReviewLike(String likeData) {
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
            String userIdStr = String.valueOf(userId);
            
            if (!action.equals("like") && !action.equals("unlike")) {
                throw new IllegalArgumentException("Invalid action. Must be 'like' or 'unlike'");
            }
            
            File reviewsFile = new File(REVIEWS_FILE);
            File tempFile = new File(REVIEWS_FILE + ".tmp");
            
            try (CSVReader reader = new CSVReaderBuilder(new FileReader(reviewsFile))
                    .withCSVParser(new RFC4180ParserBuilder().build())
                    .build();
                BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))) {
                
                String[] header = reader.readNext();
                if (header != null) {
                    writeCsvRow(writer, header);
                }
                
                String[] row;
                boolean found = false;
                
                while ((row = reader.readNext()) != null) {
                    if (row.length < 9) {
                        writeCsvRow(writer, row);
                        continue;
                    }
                    
                    // 查找目标评论行 (ReviewId在第0列)
                    if (!row[0].equals(String.valueOf(reviewId))) {
                        writeCsvRow(writer, row);
                        continue;
                    }
                    
                    found = true;
                    
                    // 解析当前Likes字段（第8列）
                    Set<String> currentLikes = splitParts(row[8]);
                    Set<String> originalLikes = new HashSet<>(currentLikes);
                    
                    // 执行点赞/取消点赞操作
                    if (action.equals("like")) {
                        currentLikes.add(userIdStr);
                    } else {
                        currentLikes.remove(userIdStr);
                    }
                    
                    // 检查是否有变化
                    if (currentLikes.equals(originalLikes)) {
                        writeCsvRow(writer, row);
                        affectedRows = 0;
                        System.out.println("No change for review " + reviewId + " (user " + userId + " " + action + ")");
                    } else {
                        // 重建Likes列（保持c()格式）
                        row[8] = formatLikesColumn(currentLikes);
                        writeCsvRow(writer, row);
                        affectedRows = 1;
                        System.out.println("Updated review " + reviewId + " (user " + userId + " " + action + ")");
                    }
                }
                
                if (!found) {
                    System.err.println("Review not found with ID: " + reviewId);
                    affectedRows = 0;
                }
                
            }
            
            // 替换原文件
            if (!reviewsFile.delete() || !tempFile.renameTo(reviewsFile)) {
                throw new IOException("Failed to replace reviews.csv file");
            }
            
            long endTime = System.currentTimeMillis();
            System.out.println("updateReviewLike file operation completed in " + (endTime - startTime) + " ms");
            
        } catch (IOException | CsvValidationException | NumberFormatException e) {
            e.printStackTrace();
            affectedRows = -1;
        }
        
        return affectedRows;
    }

    // 辅助方法：将点赞用户集合格式化为c()格式
    private String formatLikesColumn(Set<String> userIds) {
        if (userIds == null || userIds.isEmpty()) {
            return "c()";
        }
        
        StringBuilder sb = new StringBuilder("c(");
        boolean first = true;
        for (String userId : userIds) {
            if (!first) {
                sb.append(",");
            }
            sb.append("\"").append(userId).append("\"");
            first = false;
        }
        sb.append(")");
        return sb.toString();
    }



        // 在 FileManipulation 类中添加辅助类
    private static class RecipeSearchResult {
        final int id;
        final String name;
        final int authorId;
        final String category;
        final double rating;
        final int reviewCount;
        final String datePublished;
        
        RecipeSearchResult(int id, String name, int authorId, String category, double rating, int reviewCount, String datePublished) {
            this.id = id;
            this.name = name;
            this.authorId = authorId;
            this.category = category;
            this.rating = rating;
            this.reviewCount = reviewCount;
            this.datePublished = datePublished;
        }
    }
    
    // 在 FileManipulation 类中添加辅助方法和类
    /**
     * 根据用户ID从 users.csv 获取用户名
     */
    private String getUserNameById(int userId) throws IOException, CsvValidationException {
        try (CSVReader reader = new CSVReaderBuilder(new FileReader(USERS_FILE))
                .withCSVParser(new RFC4180ParserBuilder().build())
                .build()) {
            
            String[] header = reader.readNext();
            String[] fields;
            
            while ((fields = reader.readNext()) != null) {
                if (fields.length > 1 && fields[0].equals(String.valueOf(userId))) {
                    return fields[1];
                }
            }
        }
        return "Unknown User";
    }

    /**
     * 加载所有 reviews，按 recipeId 分组存储评分
     * 优化：避免为每个食谱重复扫描 reviews.csv
     */
    private Map<Integer, List<Double>> loadAllReviewsByRecipe() throws IOException, CsvValidationException {
        Map<Integer, List<Double>> recipeRatingsMap = new HashMap<>();
        
        try (CSVReader reader = new CSVReaderBuilder(new FileReader(REVIEWS_FILE))
                .withCSVParser(new RFC4180ParserBuilder().build())
                .build()) {
            
            String[] header = reader.readNext();
            String[] fields;
            int processedReviews = 0;
            
            while ((fields = reader.readNext()) != null) {
                if (fields.length < 5) continue;
                
                String recipeIdStr = fields[1].trim();
                String ratingStr = fields[4].trim();
                
                if (recipeIdStr.isEmpty() || ratingStr.isEmpty()) continue;
                
                try {
                    int recipeId = Integer.parseInt(recipeIdStr);
                    double rating = Double.parseDouble(ratingStr);
                    
                    recipeRatingsMap.computeIfAbsent(recipeId, k -> new ArrayList<>()).add(rating);
                } catch (NumberFormatException e) {
                    // 跳过格式错误的行
                }
                
                // processedReviews++;
                // if (processedReviews % 50000 == 0) {
                //     System.out.println("已加载 " + processedReviews + " 条评论记录...");
                // }
            }
        }
        
        return recipeRatingsMap;
    }

    // 辅助方法：将字符串数组写入CSV行
    private void writeCsvRow(BufferedWriter writer, String[] fields) throws IOException {
        for (int i = 0; i < fields.length; i++) {
            String field = fields[i];
            
            if (field == null || field.isEmpty()) {
                // 空字段：不写入内容（但保留逗号分隔符）
            } else if (field.contains(",") || field.contains("\"") || field.contains("\n") || field.contains("\r")) {
                // 需要引号包裹的字段
                field = field.replace("\"", "\"\"");
                writer.write("\"" + field + "\"");
            } else {
                writer.write(field);
            }
            
            // 添加逗号分隔符（如果不是最后一个字段）
            if (i < fields.length - 1) {
                writer.write(",");
            }
        }
        writer.newLine();
        writer.flush();
    }
    
    /**
     * 存储用户食谱信息的辅助类
     */
    private static class UserRecipe {
        final int id;
        final String name;
        final String category;
        final String datePublished;
        final int reviewCount;
        final double aggregatedRating;
        
        UserRecipe(int id, String name, String category, String datePublished, int reviewCount, double aggregatedRating) {
            this.id = id;
            this.name = name;
            this.category = category;
            this.datePublished = datePublished;
            this.reviewCount = reviewCount;
            this.aggregatedRating = aggregatedRating;
        }
    }
    
    // 辅助类：存储食谱和食材匹配信息
    private static class RecipeIngredientMatch {
        final int id;
        final String name;
        final int authorId;
        final String category;
        final double rating;
        final int reviewCount;
        final Set<String> ingredients;
        
        RecipeIngredientMatch(int id, String name, int authorId, String category, double rating, int reviewCount, Set<String> ingredients) {
            this.id = id;
            this.name = name;
            this.authorId = authorId;
            this.category = category;
            this.rating = rating;
            this.reviewCount = reviewCount;
            this.ingredients = ingredients;
        }
    }
    
    // 辅助类：存储食谱信息
    private static class RecipeInfo {
        final int id;
        final String name;
        final int authorId;
        final double rating;
        final int reviewCount;
        
        RecipeInfo(int id, String name, int authorId, double rating, int reviewCount) {
            this.id = id;
            this.name = name;
            this.authorId = authorId;
            this.rating = rating;
            this.reviewCount = reviewCount;
        }
    }

    // ===================== 辅助方法 =====================
    
    /**
     * 解析CSV中的多值字段（支持c("a","b")或a,b格式）
     * 处理形式如: c("item1","item2") 或 item1,item2
     */
    private static Set<String> splitParts(String src) {
        if (src == null || src.isBlank()) return Set.of();
        
        // 移除c()包装和引号
        src = src.replaceAll("^c\\(?(\"?)", "");
        src = src.replaceAll("(\"?)\\)$", "");
        src = src.replace("\"\"", "");
        src = src.replace("\"", "");
        src = src.trim();

        // 检查并移除 c( 和 )
        if (src.startsWith("c(") && src.endsWith(")")) {
            src = src.substring(2, src.length() - 1);
        }

        // 处理开头结尾的逗号
        if (src.startsWith(",")) src = src.substring(1).trim();
        if (src.endsWith(",")) src = src.substring(0, src.length() - 1).trim();
        
        if (src.isEmpty()) return Set.of();
        
        return Arrays.stream(src.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }
    
    /**
     * 解析ISO-8601时长格式到分钟
     * 示例: PT1H30M -> 90, P1DT5H -> 1200, P3M -> 3
     */
    private static int parseIsoDuration(String s) {
        if (s == null || s.isBlank()) return 0;
        
        int minutes = 0;
        
        // 确保格式正确
        if (!s.startsWith("P")) return 0;
        s = s.substring(1); // 移除开头的P
        
        // 处理天数部分 (如: P1D)
        if (s.contains("D")) {
            String[] parts = s.split("D", 2);
            minutes += Integer.parseInt(parts[0]) * 24 * 60;
            s = parts.length > 1 ? parts[1] : "";
        }
        
        // 移除时间前缀T (如: PT1H30M)
        if (s.startsWith("T")) {
            s = s.substring(1);
        }
        
        // 处理小时
        if (s.contains("H")) {
            String[] parts = s.split("H", 2);
            minutes += Integer.parseInt(parts[0]) * 60;
            s = parts.length > 1 ? parts[1] : "";
        }
        
        // 处理分钟
        if (s.contains("M")) {
            String[] parts = s.split("M", 2);
            if (!parts[0].isEmpty()) {
                minutes += Integer.parseInt(parts[0]);
            }
        }
        
        return minutes;
    }
}