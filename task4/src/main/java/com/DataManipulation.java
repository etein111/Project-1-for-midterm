package com;
public interface DataManipulation {
    // public int addOneMovie(String str);
    // public String allContinentNames();
    // public String continentsWithCountryCount();
    // public String FullInformationOfMoviesRuntime(int min, int max);
    // public String findMovieById(int id);

    // 根据ID查找食谱完整信息
    // 涉及表: Recipe + 关联表JOIN
    public String findRecipeById(int recipeId);
    
    // 查找某类别下评分前N的食谱
    // 涉及表: Recipe (category, aggregated_rating, review_count)
    public String findTopRatedRecipesInCategory(String category, int topN);
    
    // 根据食材名称查找包含该食材的所有食谱
    // 涉及表: Ingredient + Recipe_Ingredient + Recipe
    public String findRecipesByIngredient(String ingredientName);
    
    // 查找某用户的所有食谱及其平均评分
    // 涉及表: User + Recipe + Review
    public String findUserRecipesWithStats(int userId);

    // 在搜索包含特定关键词的食谱
    // 涉及表: Recipe (description, instructions)
    public String searchRecipesByKeyword(String keyword);

    // 插入新用户记录
    // 涉及表: User
    public int insertUser(String userData); // 格式: "id;name;gender;age"

    // 插入一条评论记录
    // 涉及表: Review
    public int InsertReviews(String reviewDataList); // 格式: "id;recipeId;authorId;rating;content;date"
    
    // 级联删除食谱及其所有关联数据
    // 涉及表: Recipe + Recipe_Ingredient + Recipe_Keyword + Review + Favorite + Nutritional_Information等
    public int deleteRecipeById(int recipeId);
    
    // 更新评论的点赞状态（用户点赞或取消点赞）
    // 涉及表: Like (数据库) 或 reviews.csv (文件)
    // 参数格式: "reviewId;userId;action"
    // action: "like" 表示点赞, "unlike" 表示取消点赞
    public int updateReviewLike(String likeData);
}
