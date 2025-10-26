package com.example.datapreprocessor.review;

import com.opencsv.CSVReader;
import com.opencsv.RFC4180Parser;
import com.opencsv.RFC4180ParserBuilder;
import com.opencsv.CSVReaderBuilder;

import java.nio.file.*;
import java.util.*;

public class CheckReviewIdDuplication {
    public static void check(Path csvPath) {
        String path = "Project-1-for-midterm\\reviews.csv";
        Set<String> seenIds = new HashSet<>();
        Set<String> duplicateIds = new LinkedHashSet<>();

        RFC4180Parser rfc4180Parser = new RFC4180ParserBuilder().build();

        try (CSVReader reader =
                 new CSVReaderBuilder(Files.newBufferedReader(Paths.get(path)))
                     .withCSVParser(rfc4180Parser)
                     .build()) {

            String[] line;
            reader.readNext(); // 跳过表头

            while ((line = reader.readNext()) != null) {
                if (line.length == 0) continue;
                String reviewId = line[0].trim();
                // System.out.println(reviewId);
                if (seenIds.contains(reviewId)) {
                    duplicateIds.add(reviewId);
                } else {
                    seenIds.add(reviewId);
                }
            }

            if (duplicateIds.isEmpty()) {
                System.out.println("所有 ReviewId 唯一，无重复。");
            } else {
                System.out.println("发现重复的 ReviewId 共 " + duplicateIds.size() + " 个：");
                duplicateIds.forEach(System.out::println);
            }

        } catch (Exception e) {
            System.err.println("读取文件失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
