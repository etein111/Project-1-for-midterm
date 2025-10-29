package com.example.datapreprocessor.user;

import com.opencsv.*;
import com.opencsv.exceptions.CsvValidationException;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Pattern;

public final class UserFollowFixer {

    private static final Pattern SPLIT = Pattern.compile(",");

    public static Path fix(Path in) throws IOException {
        Path out = in.getParent().resolve("user_fixed.csv");

        // 建立索引
        Map<String, Set<String>> followersOf = new HashMap<>();
        Map<String, Set<String>> followingOf = new HashMap<>();
        List<User> users = loadUsers(in);

        for (User u : users) {
            for (String id : splitIds(u.followerUsers)) {
                followersOf.computeIfAbsent(u.authorId, k -> new HashSet<>()).add(id);
                followingOf.computeIfAbsent(id, k -> new HashSet<>()).add(u.authorId);
            }
        }

        for (User u : users) {
            for (String id : splitIds(u.followingUsers)) {
                followingOf.computeIfAbsent(u.authorId, k -> new HashSet<>()).add(id);
                followersOf.computeIfAbsent(id, k -> new HashSet<>()).add(u.authorId);
            }
        }

        // 修正补全
        for (User u : users) {
            // 原始列表
            List<String> origFollowers = new ArrayList<>(splitIds(u.followerUsers));
            List<String> origFollowing = new ArrayList<>(splitIds(u.followingUsers));

            // 拿到followersOf和followingOf表里的集合
            Set<String> realFollowerSet = followersOf.getOrDefault(u.authorId, Set.of());
            Set<String> realFollowingSet = followingOf.getOrDefault(u.authorId, Set.of());

            // 把缺失的ID加到尾部
            for (String id : realFollowerSet) {
                if (!origFollowers.contains(id)) origFollowers.add(id);
            }
            for (String id : realFollowingSet) {
                if (!origFollowing.contains(id)) origFollowing.add(id);
            }

            // 写回字符串 & 计数
            u.followers      = origFollowers.size();
            u.following      = origFollowing.size();
            u.followerUsers  = origFollowers.isEmpty() ? "" : "c(\"" + String.join(",", origFollowers) + "\")";
            u.followingUsers = origFollowing.isEmpty() ? "" : "c(\"" + String.join(",", origFollowing) + "\")";
        }

        // 写回 CSV
        try (CSVWriter writer = new CSVWriter(Files.newBufferedWriter(out),
                CSVWriter.DEFAULT_SEPARATOR, '"', '"', CSVWriter.DEFAULT_LINE_END)) {

            writer.writeNext(new String[]{
                    "AuthorId","AuthorName","Gender","Age",
                    "Followers","Following","FollowerUsers","FollowingUsers"
            });

            for (User u : users) {
                writer.writeNext(new String[]{
                        u.authorId,
                        u.authorName,
                        u.gender,
                        String.valueOf(u.age),
                        String.valueOf(u.followers),
                        String.valueOf(u.following),
                        u.followerUsers,   // 字符串
                        u.followingUsers   // 字符串
                });
            }
        }

        System.out.println("修复完成 → " + out.toAbsolutePath());
        return out;
    }

    private static List<User> loadUsers(Path in) throws IOException {
        List<User> list = new ArrayList<>();
        try (CSVReader reader = new CSVReaderBuilder(Files.newBufferedReader(in))
                     .withCSVParser(new RFC4180ParserBuilder().build())
                     .build()) {

            String[] h = reader.readNext();
            Map<String, Integer> idx = headerMap(h);

            String[] line;
            // long row = 1;
            while ((line = reader.readNext()) != null) {
                // row++;
                if (line.length < 8) continue;
                User u = new User();
                u.authorId       = getCell(line, idx, "AuthorId");
                u.authorName     = getCell(line, idx, "AuthorName");
                u.gender         = getCell(line, idx, "Gender");
                u.age            = Integer.parseInt(getCell(line, idx, "Age"));
                u.followers      = Integer.parseInt(getCell(line, idx, "Followers"));
                u.following      = Integer.parseInt(getCell(line, idx, "Following"));
                u.followerUsers  = getCell(line, idx, "FollowerUsers");
                u.followingUsers = getCell(line, idx, "FollowingUsers");
                list.add(u);
            }
        } catch (CsvValidationException e) {
            throw new IOException("CSV 错误", e);
        }
        return list;
    }

    private static Map<String, Integer> headerMap(String[] h) {
        Map<String, Integer> m = new HashMap<>();
        for (int i = 0; i < h.length; i++) m.put(h[i], i);
        return m;
    }

    private static String getCell(String[] line, Map<String, Integer> idx, String col) {
        Integer i = idx.get(col);
        return i == null || i >= line.length ? "" : line[i].trim();
    }

    private static List<String> splitIds(String cell) {
        if (cell.isEmpty()) return List.of();
        if (cell.startsWith("c(\"") && cell.endsWith("\")")) {
            cell = cell.substring(3, cell.length() - 2);
        }
        cell = cell.replace("\"", "");
        return new ArrayList<>(Arrays.asList(SPLIT.split(cell)))
                   .stream().filter(s -> !s.trim().isEmpty())
                   .toList();
    }

    private static final class User {
        String authorId;
        String authorName;
        String gender;
        int age;
        int followers;
        int following;
        String followerUsers;
        String followingUsers;
    }

    private UserFollowFixer() {}
}