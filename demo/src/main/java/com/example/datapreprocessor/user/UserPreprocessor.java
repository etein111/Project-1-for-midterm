package com.example.datapreprocessor.user;

// import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
// import java.io.*;

public class UserPreprocessor {
    public static void main(String[] args) {
        Path inputPath = Paths.get("Project-1-for-midterm/user.csv");

        // 检查是否有重复的 UserId
        CheckUserIdDuplication.check(inputPath);

        try {
            FollowCountValidator.validate(inputPath);
        }
        catch (Exception e) {
            System.err.println("处理文件时出错: " + e.getMessage());
            e.printStackTrace();
        }

        try {
            UserFollowFixer.fix(inputPath);
        }
        catch (Exception e) {
            System.err.println("处理文件时出错: " + e.getMessage());
            e.printStackTrace();
        }

        Path out = inputPath.getParent().resolve("user_fixed.csv");
        try {
            FollowCountValidator.validate(out);
        }
        catch (Exception e) {
            System.err.println("处理文件时出错: " + e.getMessage());
            e.printStackTrace();
        }
    }
}