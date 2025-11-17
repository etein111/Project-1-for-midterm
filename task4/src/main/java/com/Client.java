package com;

import java.util.Random;

public class Client {

    public static void main(String[] args) {
        try {
            // 通过 launch.json 的 "args" 来选择 "database" 、"file"、"database_mysql"
            if (args.length == 0) {
                System.err.println("错误: 请在 launch.json 中提供 'database' 或 'file' 作为参数!");
                return;
            }
            DataManipulation dm = new DataFactory().createDataManipulation(args[0]);
            System.out.println("===== 当前测试模式: " + args[0].toUpperCase() + " =====");

            // testFindById(dm);
            // testFindTopRated(dm);
            // testFindByIngredient(dm);

            // testUpdateLike(dm); 
            testDeleteRecipe(dm);
            // testInsertUser(dm); 


        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
        }
    }

    /**个
     * 测试场景 1: 高频单点查询 (findRecipeById)
     */
    private static void testFindById(DataManipulation dm) {
        System.out.println("\n--- 开始测试: findRecipeById (高频单点查询) ---");
        int numberOfRuns = 100000; // 循环次数
        int maxRecipeId = 522515; // 请根据您的数据调整最大 Recipe ID
        Random random = new Random();
  
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < numberOfRuns; i++) {
            int randomId = random.nextInt(maxRecipeId) + 1;
            // 我们只调用方法，不打印结果，以减少控制台输出对性能的影响
            dm.findRecipeById(randomId);
        }
        long endTime = System.currentTimeMillis();

        long totalTime = endTime - startTime;
        double avgTime = (double) totalTime / numberOfRuns;

        System.out.printf("执行 %d 次查询，总耗时: %d ms%n", numberOfRuns, totalTime);
        System.out.printf("平均每次查询耗时: %.4f ms%n", avgTime);
        System.out.println("--- 测试结束 ---\n");
    }

    /**
     * 测试场景 2: 复杂排序查询 (findTopRatedRecipesInCategory)
     */
    private static void testFindTopRated(DataManipulation dm) {
        System.out.println("\n--- 开始测试: findTopRatedRecipesInCategory (复杂排序查询) ---");
        int numberOfRuns = 1000; // 对于慢查询，减少循环次数
        String category = "Frozen Desserts";
        int topN = 10;

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < numberOfRuns; i++) {
            dm.findTopRatedRecipesInCategory(category, topN);
        }
        long endTime = System.currentTimeMillis();

        long totalTime = endTime - startTime;
        double avgTime = (double) totalTime / numberOfRuns;

        System.out.printf("执行 %d 次查询，总耗时: %d ms%n", numberOfRuns, totalTime);
        System.out.printf("平均每次查询耗时: %.2f ms%n", avgTime);
        System.out.println("--- 测试结束 ---\n");
    }

    /**
     * 测试场景 3: 模糊匹配查询 (findRecipesByIngredient)
     */
    private static void testFindByIngredient(DataManipulation dm) {
        System.out.println("\n--- 开始测试: findRecipesByIngredient (模糊匹配查询) ---");
        int numberOfRuns = 100;
        String ingredient = "milk";

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < numberOfRuns; i++) {
            dm.findRecipesByIngredient(ingredient);
        }
        long endTime = System.currentTimeMillis();

        long totalTime = endTime - startTime;
        double avgTime = (double) totalTime / numberOfRuns;

        System.out.printf("执行 %d 次查询，总耗时: %d ms%n", numberOfRuns, totalTime);
        System.out.printf("平均每次查询耗时: %.2f ms%n", avgTime);
        System.out.println("--- 测试结束 ---\n");
    }

    /**
     * 测试场景 4: 写入操作 (insertUser)
     */
    private static void testInsertUser(DataManipulation dm) {
        System.out.println("\n--- 开始测试: insertUser (写入操作) ---");
        int numberOfRuns = 1000;
        
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < numberOfRuns; i++) {
            // 每次插入不同的用户以避免主键冲突 (对于数据库)
            String userData = (300000 + i) + ";TestUser" + i + ";Male;25";
            dm.insertUser(userData);
        }
        long endTime = System.currentTimeMillis();

        long totalTime = endTime - startTime;
        double avgTime = (double) totalTime / numberOfRuns;

        System.out.printf("执行 %d 次插入，总耗时: %d ms%n", numberOfRuns, totalTime);
        System.out.printf("平均每次插入耗时: %.2f ms%n", avgTime);
        System.out.println("--- 测试结束 ---\n");
    }

    /**
     * 测试场景 5: 删除操作 (deleteRecipeById)
     */
    private static void testDeleteRecipe(DataManipulation dm) {
        System.out.println("\n--- 开始测试: deleteRecipeById (删除操作) ---");
        System.out.println("注意: 此测试会修改数据源，请在测试数据库中运行！");
        int numberOfRuns = 10;
 
        // 警告：这个测试会真实地删除数据！
        // 确保您在测试前有数据备份，或者只在可丢弃的测试数据库中运行。
        long startTime = System.currentTimeMillis(); 
        for (int i = 0; i < numberOfRuns; i++) { 
            // 每次删除不同的食谱
            dm.deleteRecipeById(1000 + i);
        }
        long endTime = System.currentTimeMillis();

        long totalTime = endTime - startTime;
        double avgTime = (double) totalTime / numberOfRuns;

        System.out.printf("执行 %d 次删除，总耗时: %d ms%n", numberOfRuns, totalTime);
        System.out.printf("平均每次删除耗时: %.2f ms%n", avgTime);
        System.out.println("--- 测试结束 ---\n");
    }

    /** 
     * 测试场景 6: 更新操作 (updateReviewLike)
     */
    private static void testUpdateLike(DataManipulation dm) {
        System.out.println("\n--- 开始测试: updateReviewLike (更新操作) ---");
        int numberOfRuns = 10000;
        
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < numberOfRuns; i++) {
            // 交替点赞和取消点赞
            String likeData = "2;" + (100 + i) + ";like"; 
            dm.updateReviewLike(likeData);
            String unlikeData = "2;" + (100 + i) + ";unlike";
            dm.updateReviewLike(unlikeData);
        }
        long endTime = System.currentTimeMillis();

        long totalTime = endTime - startTime;
        double avgTime = (double) totalTime / (numberOfRuns * 2);

        System.out.printf("执行 %d 次点赞/取消点赞，总耗时: %d ms%n", numberOfRuns * 2, totalTime);
        System.out.printf("平均每次操作耗时: %.2f ms%n", avgTime);
        System.out.println("--- 测试结束 ---\n");
    }
}