package com.example.datapreprocessor.recipe;

import com.opencsv.CSVReader;
import com.opencsv.RFC4180Parser;
import com.opencsv.RFC4180ParserBuilder;
import com.opencsv.exceptions.CsvValidationException;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public final class CheckTime {

    public static void validate(Path csvPath) throws IOException {
        Path target = csvPath.getParent().resolve("recipes_checktime.csv");
        
        RFC4180Parser parser = new RFC4180ParserBuilder().build();
        int cookTimeIndex = 4;    // CookTime
        int prepTimeIndex = 5;    // PrepTime
        int totalTimeIndex = 6;   // TotalTime
        int recipeIdIndex = 0;    // RecipeId

        try (CSVReader reader = new CSVReaderBuilder(Files.newBufferedReader(csvPath, StandardCharsets.UTF_8))
                .withCSVParser(parser)
                .build();
            CSVWriter writer = new CSVWriter(Files.newBufferedWriter(target, StandardCharsets.UTF_8,
                     StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING))) {

            String[] header = reader.readNext();
            System.out.println("CSV 列: " + String.join(" | ", header));
            writer.writeNext(header, false);

            String[] line;

            while ((line = reader.readNext()) != null) {
                String recipeId = line[recipeIdIndex];
                String cookTime = line[cookTimeIndex];
                String prepTime = line[prepTimeIndex];
                String totalTime = line[totalTimeIndex];

                try {
                    long actualTotalTime = parseTimeToSeconds(totalTime);
                    long expectedTotalTime = parseTimeToSeconds(cookTime) + parseTimeToSeconds(prepTime);

                    if (actualTotalTime != expectedTotalTime) {
                        System.out.println("RecipeId: " + recipeId + " TotalTime 不等于 CookTime + PrepTime");
                        System.out.println("   CookTime: " + cookTime + " (" + parseTimeToSeconds(cookTime) + " 秒)");
                        System.out.println("   PrepTime: " + prepTime + " (" + parseTimeToSeconds(prepTime) + " 秒)");
                        System.out.println("   实际 TotalTime: " + totalTime + " (" + actualTotalTime + " 秒)");
                        System.out.println("   预期 TotalTime: " + expectedTotalTime + " 秒");
                    }

                    // 空或0 都视为缺失
                    long cookSec = cookTime.isEmpty() || cookTime.equals("PT0S")   ? 0 : parseTimeToSeconds(cookTime);
                    long prepSec = prepTime.isEmpty() || prepTime.equals("PT0S")   ? 0 : parseTimeToSeconds(prepTime);
                    long totSec  = totalTime.isEmpty() || totalTime.equals("PT0S") ? 0 : parseTimeToSeconds(totalTime);

                    int miss = 0;
                    if (cookSec == 0) miss++;
                    if (prepSec == 0) miss++;
                    if (totSec  == 0) miss++;

                    // 如果三个值齐全，验证并纠正TotalTime
                    if (miss == 0) {
                        long expect = cookSec + prepSec;
                        if (totSec != expect) {
                            line[totalTimeIndex] = formatSeconds(expect);   // 改成正确值
                            totSec = expect;
                            System.out.printf("RecipeId=%s 修正 TotalTime：%s -> %s%n",
                                    recipeId, totalTime, line[totalTimeIndex]);
                        }
                        if (cookSec == 0) line[cookTimeIndex] = "";
                        if (prepSec == 0) line[prepTimeIndex] = "";
                        if (totSec  == 0) line[totalTimeIndex] = "";
                        writer.writeNext(line, false);
                        continue;   // 处理完毕，继续下一行
                    }

                    // 如果恰好缺失1个
                    if (miss == 1) {
                        try {
                            if (cookSec == -1) {          // 缺 CookTime
                                cookSec = totSec - prepSec;
                                if (cookSec < 0) throw new IllegalArgumentException("CookTime 为负");
                                line[cookTimeIndex] = formatSeconds(cookSec);
                            } else if (prepSec == -1) {   // 缺 PrepTime
                                prepSec = totSec - cookSec;
                                if (prepSec < 0) throw new IllegalArgumentException("PrepTime 为负");
                                line[prepTimeIndex] = formatSeconds(prepSec);
                            } else {                      // 缺 TotalTime
                                totSec = cookSec + prepSec;
                                line[totalTimeIndex] = formatSeconds(totSec);
                            }
                            if (cookSec == 0) line[cookTimeIndex] = "";
                            if (prepSec == 0) line[prepTimeIndex] = "";
                            if (totSec  == 0) line[totalTimeIndex] = "";
                            writer.writeNext(line, false);
                            System.out.printf("RecipeId=%s 补全缺失值：CookTime=%s  PrepTime=%s  TotalTime=%s%n",
                                    recipeId, line[cookTimeIndex], line[prepTimeIndex], line[totalTimeIndex]);
                        } catch (IllegalArgumentException e) {
                            // 出现负数 → 认为数据矛盾，删除该行
                            System.out.printf("RecipeId=%s 数据矛盾（%s），丢弃本行%n", recipeId, e.getMessage());
                            continue;   // 不写入结果文件，直接跳过
                        }
                    }

                    // 如果缺失≥2个
                    else if (miss >= 2) {
                        if (cookSec == 0) line[cookTimeIndex] = "";
                        if (prepSec == 0) line[prepTimeIndex] = "";
                        if (totSec  == 0) line[totalTimeIndex] = "";
                        writer.writeNext(line, false);
                    }

                } catch (IllegalArgumentException e) {
                    System.out.println("RecipeId: " + recipeId + " 格式错误");
                    System.out.println("   CookTime: " + cookTime);
                    System.out.println("   PrepTime: " + prepTime);
                    System.out.println("   TotalTime: " + totalTime);
                }
            }

            System.out.println("验证完成");

        } catch (CsvValidationException e) {
            throw new IOException("CSV 格式错误", e);
        }
    }

    private static long parseTimeToSeconds(String time) {
        if (time == null || time.isEmpty()) {
            return 0;
        }

        long totalSeconds = 0;
        boolean inPeriod = false;
        boolean inTime = false;
        int number = 0;

        for (int i = 0; i < time.length(); i++) {
            char c = time.charAt(i);

            if (Character.isDigit(c)) {
                number = number * 10 + (c - '0');
            } else if (c == 'P') {
                inPeriod = true;
            } else if (c == 'T') {
                inTime = true;
            } else if (inPeriod || inTime) {
                if (c == 'W') {
                    totalSeconds += number * 7 * 24 * 60 * 60;
                } else if (c == 'D') {
                    totalSeconds += number * 24 * 60 * 60;
                } else if (c == 'H') {
                    totalSeconds += number * 60 * 60;
                } else if (c == 'M') {
                    totalSeconds += number * 60;
                } else if (c == 'S') {
                    totalSeconds += number;
                } else {
                    throw new IllegalArgumentException("时间格式错误");
                }
                number = 0;
            } else {
                throw new IllegalArgumentException("时间格式错误");
            }
        }

        return totalSeconds;
    }

    /** 秒 -> PTxxHxxM 最简形式 */
    private static String formatSeconds(long sec) {
        if (sec == 0) return "PT0S";
        long h = sec / 3600;
        long m = (sec % 3600) / 60;
        long s = sec % 60;
        StringBuilder sb = new StringBuilder("PT");
        if (h > 0) sb.append(h).append('H');
        if (m > 0) sb.append(m).append('M');
        if (s > 0) sb.append(s).append('S');
        return sb.toString();
    }
}