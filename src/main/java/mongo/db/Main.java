package mongo.db;

import java.security.InvalidParameterException;
import java.util.Iterator;
import java.util.Scanner;


public class Main {
    public static void main(String[] args) {
        MongoLogHandler mongo = null;
        try {
            mongo = new MongoLogHandler();
            Scanner input = new Scanner(System.in);
            while (true) {
                System.out.println("Enter a csv log");
                String csvLog = input.nextLine();
                if (csvLog.equals("exit"))
                    break;
                try {
                    mongo.insertLog(csvLog);
                }
                catch (InvalidParameterException e) {
                    System.out.println(e.toString());
                }
                askFindQueries(mongo);

                getMapReduceCollections(mongo);
            }
        }
        catch (com.mongodb.MongoSocketOpenException e) {
            System.out.println("Can not connect to db");
            System.exit(0);
        }
        finally {
            if (mongo != null)
                mongo.close();
        }
    }

    private static void askFindQueries(MongoLogHandler mongo) {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter an url");
        IpsFind(mongo, scanner.nextLine().trim());
        System.out.println("Enter an ip address");
        UrlsFind(mongo, scanner.nextLine().trim());
        System.out.println("Enter two Dates in format yyyy-mm-ddThh:mm:ss");
        String input = scanner.nextLine().trim();
        if (input.contains(",")) {
            String dateTime1 = input.substring(0, input.indexOf(',')).trim();
            String dateTime2 = input.substring(input.indexOf(',') + 1).trim();
            byTimeDateRangeFind(mongo, dateTime1, dateTime2);
        }
    }

    private static void IpsFind(MongoLogHandler mongo, String url) {
        printDocuments(mongo.findIpsByUrl(url).iterator());
    }
    private static void UrlsFind(MongoLogHandler mongo, String ip) {
        printDocuments(mongo.findUrlsOfIp(ip).iterator());
    }
    private static void byTimeDateRangeFind(MongoLogHandler mongo, String dateTime1, String dateTime2) {
            printDocuments(mongo.findUrlsInTime(dateTime1, dateTime2).iterator());
    }

    private static void getMapReduceCollections(MongoLogHandler mongo) {
        System.out.println("Urls and their total visits");
        mapReduceUrlsCount(mongo);

        System.out.println("Urls and their total visits' duration");
        mapReduceUrlsDuration(mongo);

        System.out.println("Ids and their total visits and duration");
        mapReduceIdsCountDuration(mongo);

        System.out.println("Enter two Dates in format yyyy-mm-ddThh:mm:ss");
        Scanner scanner = new Scanner(System.in);
        String input = scanner.nextLine().trim();
        if (input.contains(",")) {
            String dateTime1 = input.substring(0, input.indexOf(',')).trim();
            String dateTime2 = input.substring(input.indexOf(',') + 1).trim();
            System.out.println("Urls and their total visits within time range");
            mapReduceUrlsCountByTime(mongo, dateTime1, dateTime2);
        }
    }

    private static void mapReduceUrlsCount(MongoLogHandler mongo) {
        printDocuments(mongo.findUrlsCount().iterator());
    }
    private static void mapReduceUrlsDuration(MongoLogHandler mongo) {
        printDocuments(mongo.findUrlsDuration().iterator());
    }
    private static void mapReduceIdsCountDuration(MongoLogHandler mongo) {
        printDocuments(mongo.findIdsDurationCount().iterator());
    }
    private static void mapReduceUrlsCountByTime(MongoLogHandler mongo, String dateTime1, String dateTime2) {
        printDocuments(mongo.findUrlsCountByTime(dateTime1, dateTime2).iterator());
    }

    private static void printDocuments(Iterator it) {
        while (it.hasNext())
            System.out.println(it.next());
    }
}
