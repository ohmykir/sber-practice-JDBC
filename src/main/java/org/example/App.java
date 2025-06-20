package org.example;

import java.io.*;
import java.sql.*;
import java.util.Locale;
import java.util.Properties;

public class App {
    //private static final File file = new File("src/main/resources/config.properties");

    public static void main(String[] args) {
        Connection connection = connectToDb();

        boolean quit = false;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
            while (!quit) {
                String query = reader.readLine();
                if (query.trim().equalsIgnoreCase("QUIT")) {
                    quit = true;
                } else {
                    processQuery(connection, query);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    private static Connection connectToDb() {
        Connection connection = null;

        Properties properties = new Properties();
        try (InputStream input = App.class.getResourceAsStream("/config.properties")) {
            properties.load(input);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        String url = properties.getProperty("url");
        String username = properties.getProperty("username");
        String password = properties.getProperty("password");

        try {
            connection = DriverManager.getConnection(url, username, password);
            System.out.println(DriverManager.getDriver(url));
            System.out.println("Подключение установлено, введите SQL выражение");
            return connection;
        } catch (SQLException e) {
            throw new RuntimeException("Ошибка при подключении к базе данных", e);
        }
    }

    private static void processQuery(Connection connection, String query) throws SQLException {
        Statement statement = connection.createStatement();
        query = query.trim();
        String operation = query.split(" ")[0];

        if (operation.equalsIgnoreCase("INSERT") ||
                operation.equalsIgnoreCase("UPDATE") ||
                operation.equalsIgnoreCase("DELETE")) {
            try {
                statement.executeUpdate(query);
                System.out.println("БД успешно обновлена");
                System.out.println();
            } catch (SQLException e) {
                Locale.setDefault(new Locale("ru", "RU"));
                System.out.println(e.getMessage());
                System.out.println();
            }
        } else if (operation.equalsIgnoreCase("SELECT")) {
            try {
                String tableName = getTableName(query);
                ResultSet countRS = statement.executeQuery("SELECT COUNT(*) FROM " + tableName);
                int count = 0;
                if (countRS.next()) {
                    count = countRS.getInt(1);
                }
                if (count > 10) {
                    query = limitQuery(query);
                }

                ResultSet resultSet = statement.executeQuery(query);
                printResultSet(resultSet);
            } catch (SQLSyntaxErrorException e) {
                System.out.println(e.getMessage());
                System.out.println();
            }
        } else {
            System.out.println("Доступны только DML команды");
            System.out.println();
        }
    }

    private static String getTableName(String query) {
        String[] words = query.split(" ");
        for (int i = 0; i < words.length; i++) {
            if (words[i].equalsIgnoreCase("FROM")) {
                try {
                    return words[i + 1];
                } catch (IndexOutOfBoundsException e) {
                    //пропускаем ошибку, ловим sqlSyntaxError дальше
                }
            }
        }
        return null;
    }

    private static String limitQuery(String query) {
        if (query.endsWith(";")) {
            query = query.substring(0, query.length() - 1);
        }
        String[] words = query.split(" ");

        boolean limitExists = false;
        for (int i = 0; i < words.length; i++) {
            if (words[i].equalsIgnoreCase("LIMIT")) {
                limitExists = true;

                try {
                    if (Integer.parseInt(words[i + 1]) > 11) {
                        words[i + 1] = "11";
                        return String.join(" ", words);
                    }
                } catch (ArrayIndexOutOfBoundsException e) {
                    //пропускаем ошибку, ловим sqlSyntaxError дальше
                }
            }
        }

        if (!limitExists) {
            return query + " LIMIT 11";
        }

        return query;
    }

    private static void printResultSet(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();

        int columnCount = metaData.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            System.out.print(metaData.getColumnName(i) + "\t");
        }
        System.out.println();

        int rowNum = 0;
        while (resultSet.next()) {
            rowNum++;
            if (rowNum == 11) {
                System.out.println("В БД еще есть записи");
                System.out.println();
                break;
            }
            for (int i = 1; i <= columnCount; i++) {
                System.out.print(resultSet.getString(i) + "\t");
            }
            System.out.println();
        }
    }
}