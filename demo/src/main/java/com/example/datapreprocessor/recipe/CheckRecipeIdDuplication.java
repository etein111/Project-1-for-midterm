package com.example.datapreprocessor.recipe;

import com.opencsv.CSVReader;
import com.opencsv.RFC4180Parser;
import com.opencsv.RFC4180ParserBuilder;
import com.opencsv.CSVReaderBuilder;

// import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.nio.charset.StandardCharsets;

public final class CheckRecipeIdDuplication {
    public static void check(Path csvPath) {
        // String path = "Project-1-for-midterm/recipes.csv";
        Set<String> seenIds = new HashSet<>();
        Set<String> duplicateIds = new LinkedHashSet<>();

        RFC4180Parser rfc4180Parser = new RFC4180ParserBuilder().build();

        try (CSVReader reader =
                 new CSVReaderBuilder(Files.newBufferedReader(csvPath, StandardCharsets.UTF_8))
                     .withCSVParser(rfc4180Parser)
                     .build()) {

            String[] line;
            reader.readNext(); // 跳过表头

            while ((line = reader.readNext()) != null) {
                if (line.length == 0) continue;
                String recipeId = line[0].trim();
                // System.out.println(recipeId);
                if (seenIds.contains(recipeId)) {
                    duplicateIds.add(recipeId);
                } else {
                    seenIds.add(recipeId);
                }
            }

            if (duplicateIds.isEmpty()) {
                System.out.println("所有 RecipeId 唯一，无重复。");
            } else {
                System.out.println("发现重复的 RecipeId 共 " + duplicateIds.size() + " 个：");
                duplicateIds.forEach(System.out::println);
            }

        } catch (Exception e) {
            System.err.println("读取文件失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
}