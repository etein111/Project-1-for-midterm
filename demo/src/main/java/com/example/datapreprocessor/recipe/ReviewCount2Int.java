package com.example.datapreprocessor.recipe;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.RFC4180Parser;
import com.opencsv.RFC4180ParserBuilder;
import com.opencsv.exceptions.CsvValidationException;
import com.opencsv.CSVReaderBuilder;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public final class ReviewCount2Int {

    public static void convert2int(Path source) throws IOException {
        Path target = source.getParent().resolve("recipes_reviewcount2int.csv");

        RFC4180Parser parser = new RFC4180ParserBuilder().build();

        try (BufferedReader br = Files.newBufferedReader(source, StandardCharsets.UTF_8);
             CSVReader reader = new CSVReaderBuilder(br).withCSVParser(parser).build();
             BufferedWriter bw = Files.newBufferedWriter(target, StandardCharsets.UTF_8,
                     StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
             CSVWriter writer = new CSVWriter(bw)) {

            String[] line;

            if ((line = reader.readNext()) != null) {
                writer.writeNext(line, false);
            }

            while ((line = reader.readNext()) != null) {
                if (line.length < 14) {   // 如果列数不足，直接原样写回
                    writer.writeNext(line);
                    continue;
                }

                // 取出 ReviewCount（第 14 列，索引 13）
                String raw = line[13].trim();
                try {
                    double dv = Double.parseDouble(raw);
                    long iv = Math.round(dv);
                    line[13] = String.valueOf(iv);
                } catch (NumberFormatException e) {
                }

                // 写回文件
                writer.writeNext(line, false);
            }

            System.out.println("ReviewCount 类型已转换为int，已输出到: " + target.toAbsolutePath());

        } catch (CsvValidationException e) {
            throw new IOException("CSV 格式错误", e);
        }
    }
}