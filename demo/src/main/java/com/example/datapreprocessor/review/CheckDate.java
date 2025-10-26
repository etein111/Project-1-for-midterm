package com.example.datapreprocessor.review;

import com.opencsv.CSVReader;
// import com.opencsv.RFC4180Parser;
import com.opencsv.RFC4180ParserBuilder;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Arrays;

public final class CheckDate {
    public static long validate(Path csvPath) throws IOException {
        long invalidRows = 0;

        try (CSVReader reader =
                 new CSVReaderBuilder(Files.newBufferedReader(csvPath))
                     .withCSVParser(new RFC4180ParserBuilder().build())
                     .build()) {

            String[] header = reader.readNext();
            int submittedIdx = Arrays.asList(header).indexOf("DateSubmitted");
            int modifiedIdx  = Arrays.asList(header).indexOf("DateModified");
            if (submittedIdx < 0 || modifiedIdx < 0) {
                throw new IllegalArgumentException("CSV 缺少 DateSubmitted 或 DateModified 列");
            }

            String[] line;
            long row = 1;          // 表头后第一行
            while ((line = reader.readNext()) != null) {
                row++;
                if (line.length <= Math.max(submittedIdx, modifiedIdx)) continue;

                String submitted = line[submittedIdx].trim();
                String modified  = line[modifiedIdx].trim();

                // 缺失不校验
                if (submitted.isEmpty() || modified.isEmpty()) continue;

                try {
                    Instant sub = Instant.parse(submitted);
                    Instant mod = Instant.parse(modified);

                    System.out.printf(submitted + modified + "\n");
                    if (sub.isAfter(mod)) {
                        invalidRows++;
                        System.out.printf("Row %d 逆序: ReviewId=%s  DateSubmitted=%s  DateModified=%s%n",
                                row, line[0], submitted, modified);
                    }
                } catch (DateTimeParseException e) {
                    System.out.printf("Row %d 日期格式非法: %s / %s%n", row, submitted, modified);
                }
            }

            if (invalidRows == 0) {
                System.out.println("所有 DateSubmitted ≤ DateModified");
            } else {
                System.out.printf("共 %d 行逆序%n", invalidRows);
            }
            return invalidRows;

        } catch (CsvValidationException e) {
            throw new IOException("CSV 解析失败", e);
        }
    }
}