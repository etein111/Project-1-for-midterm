package com.example.datapreprocessor.review;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
// import java.io.*;

public class ReviewPreprocessor {
    public static void main(String[] args) {
        Path inputPath = Paths.get("Project-1-for-midterm/reviews.csv");

        // 检查是否有重复的 ReviewId
        CheckReviewIdDuplication.check(inputPath);

        // 将 RecipeId 转换为整数类型
        try {
            RecipeId2Int.convert2int(inputPath);
        } catch (IOException e) {
            System.err.println("处理文件时出错: " + e.getMessage());
            e.printStackTrace();
        }

        // 检查 DateSubmitted ≤ DateModified
        try {
            CheckDate.validate(inputPath);
        } catch (IOException e) {
            System.err.println("处理文件时出错: " + e.getMessage());
            e.printStackTrace();
        }
    }
}