package com.example.datapreprocessor.recipe;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.*;

public class RecipePreprocessor {
    public static void main(String[] args) {
        Path inputPath = Paths.get("Project-1-for-midterm/recipes.csv");
        // Path tempPath = Paths.get("src/main/resources/recipes_cleaned.csv");

        // 检查是否有重复的 RecipeId
        CheckRecipeIdDuplication.check(inputPath);

        // 将 ReviewCount 转换为整数类型
        try {
            ReviewCount2Int.convert2int(inputPath);
        } catch (IOException e) {
            System.err.println("处理文件时出错: " + e.getMessage());
            e.printStackTrace();
        }

        // 将 RecipeServing 转换为整数类型
        inputPath = inputPath.getParent().resolve("recipes_reviewcount2int.csv");
        try {
            RecipeServing2Int.convert2int(inputPath);
        } catch (IOException e) {
            System.err.println("处理文件时出错: " + e.getMessage());
            e.printStackTrace();
        }

        // 检查Time的正确性
        inputPath = inputPath.getParent().resolve("recipes_recipeserving2int.csv");
        try {
            CheckTime.validate(inputPath);
        } catch (IOException e) {
            System.err.println("处理文件时出错: " + e.getMessage());
            e.printStackTrace();
        }
    }
}