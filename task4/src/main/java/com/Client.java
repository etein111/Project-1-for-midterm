package com;

public class Client {

    public static void main(String[] args) {
        try {
            DataManipulation dm = new DataFactory().createDataManipulation(args[0]);

            // findRecipeById
            // int recipeId = 5;
            // System.out.println("=== Testing findRecipeById (ID: " + recipeId + ") ===");
            // long startTime_1 = System.currentTimeMillis();
            // String result = dm.findRecipeById(recipeId);
            // long endTime_1 = System.currentTimeMillis();
            // System.out.println("Execution time: " + (endTime_1 - startTime_1) + " ms\n");
            // System.out.println(result);

            // findTopRatedRecipesInCategory
            // String category = "Frozen Desserts";
            // int topN = 3;
            // System.out.println("=== Testing findTopRatedRecipesInCategory (category: " + category + ", topN: " + topN + ") ===");
            // String result = dm.findTopRatedRecipesInCategory(category, topN);
            // System.out.println(result);

            // findRecipesByIngredient
            // String ingredientName = "milk";
            // System.out.println("=== Testing findRecipesByIngredient (Ingredient: '" + ingredientName + "') ===");
            // String result = dm.findRecipesByIngredient(ingredientName);
            // System.out.println(result);

            // findUserRecipesWithStats
            // int userId = 5;
            // System.out.println("=== Testing findUserRecipesWithStats (User ID: " + userId + ") ===");
            // String result = dm.findUserRecipesWithStats(userId);
            // System.out.println(result);

            // searchRecipesByKeyword
            // String keyword = "chocolate";
            // System.out.println("=== Testing searchRecipesByKeyword (Keyword: '" + keyword + "') ===");
            // String result = dm.searchRecipesByKeyword(keyword);
            // System.out.println(result);

            // insertUser
            // String newUserData = "300000;TestUser;Male;25";
            // System.out.println("=== Testing insertUser ===");
            // long StartTime = System.currentTimeMillis();
            // int rowsAffected = dm.insertUser(newUserData);
            // long EndTime = System.currentTimeMillis();
            // System.out.println("Rows affected: " + rowsAffected);
            // System.out.println("Execution time: " + (EndTime - StartTime) + " ms\n");

            // InsertReviews, 格式: "id;recipeId;authorId;rating;content;date"
            // String newReviewData = "1500000;2;299877;4.5;This is a test review;2023-10-27T10:30:00Z";
            // System.out.println("=== Testing InsertReviews ===");
            // long startTimeReview = System.currentTimeMillis();
            // int reviewRowsAffected = dm.InsertReviews(newReviewData);
            // long endTimeReview = System.currentTimeMillis();
            // System.out.println("Rows affected: " + reviewRowsAffected);
            // System.out.println("Execution time: " + (endTimeReview - startTimeReview) + " ms\n");

            // deleteRecipeById
            // int recipeIdToDelete = 10;
            // System.out.println("=== Testing deleteRecipeById (ID: " + recipeIdToDelete + ") ===");
            // long deleteStartTime = System.currentTimeMillis();
            // int deleteRowsAffected = dm.deleteRecipeById(recipeIdToDelete);
            // long deleteEndTime = System.currentTimeMillis();
            // System.out.println("Execution time: " + (deleteEndTime - deleteStartTime) + " ms\n");

            // updateReviewLike
            // 测试点赞功能
            // String likeData = "2;16;like"; // reviewId=2, userId=16, 点赞
            // System.out.println("=== Testing updateReviewLike (Like) ===");
            // long likeStart = System.currentTimeMillis();
            // int likeResult = dm.updateReviewLike(likeData);
            // long likeEnd = System.currentTimeMillis();
            // System.out.println("Operation result: " + likeResult);
            // System.out.println("Execution time: " + (likeEnd - likeStart) + " ms\n");

            // 测试取消点赞
            // String unlikeData = "2;16;unlike"; // 取消点赞
            // System.out.println("=== Testing updateReviewLike (Unlike) ===");
            // long unlikeStart = System.currentTimeMillis();
            // int unlikeResult = dm.updateReviewLike(unlikeData);
            // long unlikeEnd = System.currentTimeMillis();
            // System.out.println("Operation result: " + unlikeResult);
            // System.out.println("Execution time: " + (unlikeEnd - unlikeStart) + " ms\n");

        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
        }
    }
}

