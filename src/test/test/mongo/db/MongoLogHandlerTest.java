package test.mongo.db;

import mongo.db.MongoLogHandler;
import com.mongodb.*;
import com.mongodb.client.*;
import static com.mongodb.client.model.Filters.*;
import org.bson.Document;
import java.security.InvalidParameterException;
import java.text.SimpleDateFormat;
import java.util.*;
import org.junit.*;
import static org.junit.Assert.*;
import static org.assertj.core.api.Assertions.*;


public class MongoLogHandlerTest {
    private MongoLogHandler handler = new MongoLogHandler();
    private MongoDatabase testDatabase = new MongoClient("localhost", 27017).getDatabase("serverlogs");
    private MongoCollection<Document> collection = testDatabase.getCollection("logs");
    private static Map<String, String> incorrectLogs, correctLogs;

    @BeforeClass
    public static void init() {
        MongoLogHandler handler = new MongoLogHandler();
        handler.insertLog("43.13.111.66, 2017-10-31T21:33:06, 10, stackoverflow.com/questions/3778428/best-way-to-store-date-time-in-mongodb");
        handler.insertLog("123.43.2.8, 2017-10-26T09:09:51, 2.1, www.tutorialspoint.com/mongodb/mongodb_query_document.htm");
        handler.insertLog("123.23.13.2, 2017-10-15T11:50:08, 20, www.tutorialspoint.com/mongodb/mongodb_java.htm");
        handler.insertLog("9.13.77.62, 2017-10-23T14:22:10, 4.3, http://www.stijit.com/web-tips/dry-kiss-solid-yagni");
        handler.insertLog("123.23.13.2, 2017-10-23T15:41:18, 7, http://www.stijit.com/web-tips/dry-kiss-solid-yagni");
        handler.insertLog("123.23.13.2, 2017-10-23T15:45:18, 4.7, www.tutorialspoint.com/mongodb/mongodb_java.htm");
        handler.insertLog("223.23.13.2, 2017-11-03T10:00:28, 46, www.tutorialspoint.com/mongodb/mongodb_java.htm");
        handler.insertLog("123.23.13.2, 2017-10-23T15:45:18, 4.7, www.tutorialspoint.com/mongodb/mongodb_java.htm");
        handler.insertLog("123.23.13.2, 2017-10-23T15:45:18, 4.7, www.tutorialspoint.com/mongodb/mongodb_java.htm");
        handler.close();

        initLogs();
    }
    public static void initLogs() {
        incorrectLogs = new HashMap<>();
        incorrectLogs.put("", "not enough values");
        incorrectLogs.put("111.35.120.105, https://regex101.com, 50.9", "not enough values");
        incorrectLogs.put("111.35.120.105, dfhhjhg, 50.9, 111.35.120.105", "incorrect value");
        incorrectLogs.put("111.35.300.105, https://regex101.com, 50.9, 111.35.120.105" , "incorrect value");

        correctLogs = new HashMap<>();
        correctLogs.put("      111.35.120.105, https://regex101.com, 50.9  , 2017-3-2T21:22:7", "https://regex101.com");
        correctLogs.put("https://drive.google.com/drive/folders/0B4FNpNP6fq8LaE9iODAyY3pHZGM, 12, 2017-09-04T5:10:04, 02.66.01.00",
                "https://drive.google.com/drive/folders/0B4FNpNP6fq8LaE9iODAyY3pHZGM");
    }
    @AfterClass
    public static void finish() {
        MongoLogHandler handler = new MongoLogHandler();
        MongoCollection<Document> collection = handler.getCollection();
        collection.drop();
        handler.close();
    }

    @Test
    public void isValid() throws InvalidParameterException {
        for (String log: correctLogs.keySet())
            assertTrue(handler.isValid(log));
    }
    @Test
    public void isInvalid() throws Exception {
        for (String log: incorrectLogs.keySet()) {
            try {
                handler.isValid(log);
                throw new Exception();
            }
            catch (InvalidParameterException ex) {
                assertEquals(incorrectLogs.get(log), ex.getMessage());
            }
        }
    }

    @Test
    public void insertIncorrectLogs() throws Exception {
        for (String log: incorrectLogs.keySet()) {
            try {
                handler.insertLog(log);
                throw new Exception();
            }
            catch (InvalidParameterException ex) {
                assertEquals(incorrectLogs.get(log), ex.getMessage());
            }
        }
    }
    @Test
    public void insertLogs() throws Exception {
        long initCount = collection.count();
        for (String log: correctLogs.keySet()) {
            handler.insertLog(log);
            assertThat(collection.count()).isEqualTo(initCount + 1);
            assertThat(collection.find(eq("url", correctLogs.get(log)))).isNotNull();
            collection.deleteOne(eq("url", correctLogs.get(log)));
        }
    }

    //region Find
    @Test
    public void findIpsByUrl() throws Exception {
        String url = "http://www.stijit.com/web-tips/dry-kiss-solid-yagni";
        long expectedCount = collection.count(eq("url", url));
        long actualCount = getActualCount(handler.findIpsByUrl(url));
        assertEquals(expectedCount, actualCount);
    }
    @Test
    public void findUrlsOfIp() throws Exception {
        String ip = "123.23.13.2";
        long expectedCount = collection.count(eq("ip", ip));
        long actualCount = getActualCount(handler.findUrlsOfIp(ip));
        assertEquals(expectedCount, actualCount);
    }
    @Test
    public void findUrlsInTime() throws Exception {
        String dateTime1 = "2017-10-25T10:45:18";
        String dateTime2 = "2017-11-13T00:00:00";
        Date date1 = handler.getDateTime(dateTime1);
        Date date2 = handler.getDateTime(dateTime2);
        SimpleDateFormat parser = new SimpleDateFormat("EEE MMM d HH:mm:ss zzz yyyy", Locale.ENGLISH);
        long expectedCount = collection.count(and(gt("datetime", parser.parse(date1.toString())),
                lt("datetime",  parser.parse(date2.toString()))));
        long actualCount = getActualCount(handler.findUrlsInTime(dateTime1, dateTime2));
        assertEquals(expectedCount, actualCount);
    }
    //endregion
    private long getActualCount(FindIterable<Document> docs) {
        long count = 0;
        Iterator iterator = docs.iterator();
        while (iterator.hasNext() && iterator.next() != null)
            count++;
        return count;
    }

    //region MapReduce
    @Test
    public void findUrlsCount() throws Exception {
        List<Document> expectedDocs = getExpectedDocsForUrlsCount();
        List<Document> actualDocs = new ArrayList<>();
        for (Document document : handler.findUrlsCount())
            actualDocs.add(document);
        assertEquals(expectedDocs, actualDocs);
    }
    private List<Document> getExpectedDocsForUrlsCount() {
        List<Document> docs = new ArrayList<>();
        Map<String, Object> urlVisits = new HashMap<>();
        urlVisits.put("_id", "www.tutorialspoint.com/mongodb/mongodb_java.htm");
        urlVisits.put("value", 5.0);
        docs.add(new Document(urlVisits));
        urlVisits.clear();
        urlVisits.put("_id", "http://www.stijit.com/web-tips/dry-kiss-solid-yagni");
        urlVisits.put("value", 2.0);
        docs.add(new Document(urlVisits));
        urlVisits.clear();
        urlVisits.put("_id", "stackoverflow.com/questions/3778428/best-way-to-store-date-time-in-mongodb");
        urlVisits.put("value", 1.0);
        docs.add(new Document(urlVisits));
        urlVisits.clear();
        urlVisits.put("_id", "www.tutorialspoint.com/mongodb/mongodb_query_document.htm");
        urlVisits.put("value", 1.0);
        docs.add(new Document(urlVisits));
        urlVisits.clear();
        return docs;
    }
    @Test
    public void findUrlsDuration() throws Exception {
        List<Document> expectedDocs = getExpectedDocsForUrlsDuration();
        List<Document> actualDocs = new ArrayList<>();
        for (Document document : handler.findUrlsDuration())
            actualDocs.add(document);
        assertEquals(expectedDocs, actualDocs);
    }
    private List<Document> getExpectedDocsForUrlsDuration() {
        List<Document> docs = new ArrayList<>();
        Map<String, Object> urlDuration = new HashMap<>();
        urlDuration.put("_id", "www.tutorialspoint.com/mongodb/mongodb_java.htm");
        urlDuration.put("value", 80.09999942779541);
        docs.add(new Document(urlDuration));
        urlDuration.clear();
        urlDuration.put("_id", "http://www.stijit.com/web-tips/dry-kiss-solid-yagni");
        urlDuration.put("value", 11.300000190734863);
        docs.add(new Document(urlDuration));
        urlDuration.clear();
        urlDuration.put("_id", "stackoverflow.com/questions/3778428/best-way-to-store-date-time-in-mongodb");
        urlDuration.put("value", 10.0);
        docs.add(new Document(urlDuration));
        urlDuration.clear();
        urlDuration.put("_id", "www.tutorialspoint.com/mongodb/mongodb_query_document.htm");
        urlDuration.put("value", 2.0999999046325684);
        docs.add(new Document(urlDuration));
        urlDuration.clear();
        return docs;
    }
    @Test
    public void findIdsDurationCount() throws Exception {
        List<Document> expectedDocs = getExpectedDocsForIdsDurationCount();
        List<Document> actualDocs = new ArrayList<>();
        for (Document document : handler.findIdsDurationCount())
            actualDocs.add(document);
        assertEquals(expectedDocs, actualDocs);
    }
    private List<Document> getExpectedDocsForIdsDurationCount() {
        List<Document> docs = new ArrayList<>();
        Map<String, Object> durationCount = new HashMap<>();
        Map<String, Object> idsDurationCount = new HashMap<>();
        durationCount.put("count", 5.0);
        durationCount.put("duration", 41.09999942779541);
        idsDurationCount.put("_id", "123.23.13.2");
        idsDurationCount.put("value", new Document(durationCount));
        docs.add(new Document(idsDurationCount));
        durationCount.clear();
        idsDurationCount.clear();
        durationCount.put("count", 1.0);
        durationCount.put("duration", 2.0999999046325684);
        idsDurationCount.put("_id", "123.43.2.8");
        idsDurationCount.put("value", new Document(durationCount));
        docs.add(new Document(idsDurationCount));
        durationCount.clear();
        idsDurationCount.clear();
        durationCount.put("count", 1.0);
        durationCount.put("duration", 46.0);
        idsDurationCount.put("_id", "223.23.13.2");
        idsDurationCount.put("value", new Document(durationCount));
        docs.add(new Document(idsDurationCount));
        durationCount.clear();
        idsDurationCount.clear();
        durationCount.put("count", 1.0);
        durationCount.put("duration", 10.0);
        idsDurationCount.put("_id", "43.13.111.66");
        idsDurationCount.put("value", new Document(durationCount));
        docs.add(new Document(idsDurationCount));
        durationCount.clear();
        idsDurationCount.clear();
        durationCount.put("count", 1.0);
        durationCount.put("duration", 4.300000190734863);
        idsDurationCount.put("_id", "9.13.77.62");
        idsDurationCount.put("value", new Document(durationCount));
        docs.add(new Document(idsDurationCount));
        durationCount.clear();
        idsDurationCount.clear();
        return docs;
    }
    @Test
    public void findUrlsCountByTime() throws Exception {
        String dateTime1 = "2017-10-23T10:45:18";
        String dateTime2 = "2017-10-31T00:00:00";
        List<Document> expectedDocs = getExpectedDocsForUrlsCountByTime();
        List<Document> actualDocs = new ArrayList<>();
        for (Document document : handler.findUrlsCountByTime(dateTime1, dateTime2))
            actualDocs.add(document);
        assertEquals(expectedDocs, actualDocs);
    }
    private List<Document> getExpectedDocsForUrlsCountByTime() {
        List<Document> docs = new ArrayList<>();
        Map<String, Object> urlVisits = new HashMap<>();
        urlVisits.put("_id", "www.tutorialspoint.com/mongodb/mongodb_java.htm");
        urlVisits.put("value", 3.0);
        docs.add(new Document(urlVisits));
        urlVisits.clear();
        urlVisits.put("_id", "http://www.stijit.com/web-tips/dry-kiss-solid-yagni");
        urlVisits.put("value", 2.0);
        docs.add(new Document(urlVisits));
        urlVisits.clear();
        urlVisits.put("_id", "www.tutorialspoint.com/mongodb/mongodb_query_document.htm");
        urlVisits.put("value", 1.0);
        docs.add(new Document(urlVisits));
        urlVisits.clear();
        return docs;
    }
    //endregion
}
