package com.example.datapreprocessor.user;

import com.opencsv.CSVReader;
// import com.opencsv.RFC4180Parser;
import com.opencsv.RFC4180ParserBuilder;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.regex.Pattern;

public final class FollowCountValidator {

    private static final Pattern SPLIT = Pattern.compile(",");

    public static long validate(Path csvPath) throws IOException {
        long badLines = 0;

        try (CSVReader reader =
                 new CSVReaderBuilder(Files.newBufferedReader(csvPath))
                     .withCSVParser(new RFC4180ParserBuilder().build())
                     .build()) {

            String[] header = reader.readNext();
            int followersIdx   = Arrays.asList(header).indexOf("Followers");
            int followingIdx   = Arrays.asList(header).indexOf("Following");
            int followerUIdx   = Arrays.asList(header).indexOf("FollowerUsers");
            int followingUIdx  = Arrays.asList(header).indexOf("FollowingUsers");
            if (followersIdx < 0 || followingIdx < 0 || followerUIdx < 0 || followingUIdx < 0)
                throw new IllegalArgumentException("CSV 缺少所需列");

            String[] line;
            long row = 1;
            while ((line = reader.readNext()) != null) {
                row++;
                if (line.length <= Math.max(followingUIdx, followerUIdx)) continue;

                String followers   = line[followersIdx].trim();
                String following   = line[followingIdx].trim();
                String followerU   = line[followerUIdx].trim();
                String followingU  = line[followingUIdx].trim();

                // if (followers.isEmpty() || followerU.isEmpty() ||
                //     following.isEmpty() || followingU.isEmpty()) continue;

                try {
                    int followersN = Integer.parseInt(followers);
                    int followingN = Integer.parseInt(following);

                    int actualFollower = splitIds(followerU);
                    int actualFollowing = splitIds(followingU);

                    if (followersN != actualFollower) {
                        badLines++;
                        System.out.printf("Row %d  Followers 不匹配: AuthorId=%s  预期=%d  实际=%d%n",
                                row, line[0], followersN, actualFollower);
                    }
                    if (followingN != actualFollowing) {
                        badLines++;
                        System.out.printf("Row %d  Following 不匹配: AuthorId=%s  预期=%d  实际=%d%n",
                                row, line[0], followingN, actualFollowing);
                    }

                } catch (NumberFormatException e) {
                    System.out.printf("Row %d 数字格式非法: %s / %s%n", row, followers, following);
                }
            }

            if (badLines == 0) {
                System.out.println("所有 Followers/Following 计数与列表一致");
            } else {
                System.out.printf("共 %d 行计数不符%n", badLines);
            }
            return badLines;

        } catch (CsvValidationException e) {
            throw new IOException("CSV 解析失败", e);
        }
    }

    /** 把 c("id1,id2,...") 或裸列表拆成 ID 个数 */
    private static int splitIds(String cell) {
    if (cell.isEmpty()) return 0;

    // 去掉 c(...) 外壳
    if (cell.startsWith("c(\"") && cell.endsWith("\")")) {
        cell = cell.substring(3, cell.length() - 2);
    }

    // 去掉所有引号
    cell = cell.replace("\"", "");

    // 按逗号拆分，过滤空串
    return (int) SPLIT.splitAsStream(cell)
                      .filter(s -> !s.trim().isEmpty())
                      .count();
    }

    private FollowCountValidator() {}
}