package mongo.db;

import com.mongodb.*;
import com.mongodb.client.*;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Sorts.*;
import org.bson.*;
import java.security.InvalidParameterException;
import java.text.*;
import java.util.*;
import java.util.regex.*;


/**
 * Class handles queries for 'serverlogs' database,
 * 'logs' collection
 */
public class MongoLogHandler {
    public MongoDatabase getDatabase() {
        return database;
    }
    public MongoCollection<Document> getCollection() {
        return collection;
    }
    private MongoDatabase database;
    private MongoCollection<Document> collection;
    private Map<String, Object> documentContent;
    private StringBuilder jsonLog;

    public MongoLogHandler() throws com.mongodb.MongoSocketOpenException {
        MongoClient mongo = new MongoClient("localhost", 27017);
        database = mongo.getDatabase("serverlogs");
        collection = database.getCollection("logs");
        documentContent = new HashMap<>();
        jsonLog = new StringBuilder("{}");
    }

    public void insertLog(String csvLog) throws InvalidParameterException {
        if (isValid(csvLog)) {
            getMapFromJsonLog();
        /*Document document = new Document("url", documentContent.get("url"))
                    .append("ip", documentContent.get("ip"))
                    .append("datetime", getDateTime())
                    .append("duration", documentContent.get("duration"));*/
            collection.insertOne(new Document(documentContent));
            System.out.println("Document inserted successfully");
        }
    }
    private void getMapFromJsonLog() {
        String partLog = jsonLog.toString();
        String set = partLog.substring(1 ,partLog.indexOf(","));
        String attr, value;
        while (partLog.length() > 0) {
            attr = set.substring(0, set.indexOf(":"));
            value = set.substring(set.indexOf(":") + 2);
            try {
                // why so strange float ?
                documentContent.put(removeOddQuotes(attr), Float.parseFloat(value));
            }
            catch (NumberFormatException e) {
                try {
                    SimpleDateFormat parser = new SimpleDateFormat("EEE MMM d HH:mm:ss zzz yyyy", Locale.ENGLISH);
                    documentContent.put(removeOddQuotes(attr), parser.parse(value));
                }
                catch (ParseException e1) {
                    documentContent.put(removeOddQuotes(attr),removeOddQuotes(value));
                }
            }
            if (partLog.contains(",")) {
                partLog = partLog.substring(partLog.indexOf(",") + 1);
                if (partLog.contains(","))
                    set = partLog.substring(0, partLog.indexOf(","));
                else
                    set = partLog.substring(0, partLog.indexOf("}"));

            }
            else
                partLog = "";
        }
    }
    private String removeOddQuotes(String attr) {
        String newAttr = attr;
        if (attr.contains("\""))
            newAttr = attr.substring(1,attr.length() - 1);
        return newAttr;
    }

    public Date getDateTime(String dateTime) {
        String date = dateTime.substring(0, dateTime.indexOf("T"));
        String time = dateTime.substring(dateTime.indexOf("T") + 1);

        List<String> dateValues = getValues(date, '-');
        int year = Integer.parseInt(dateValues.get(0));
        int month = Integer.parseInt(dateValues.get(1));
        int day = Integer.parseInt(dateValues.get(2));

        List<String> timeValues = getValues(time, ':');
        int hours = Integer.parseInt(timeValues.get(0));
        int minutes = Integer.parseInt(timeValues.get(1));
        int seconds = timeValues.get(2).contains(".") ?
                Integer.parseInt(timeValues.get(2).
                        substring(0, timeValues.get(2).indexOf("."))): Integer.parseInt(timeValues.get(2));

        return new Date(year - 1900, month - 1, day, hours, minutes, seconds);
    }

    public boolean isValid(String csvLog) throws InvalidParameterException {
        documentContent.clear();
        jsonLog = new StringBuilder("{}");
        return isFull(csvLog) && matchPattern(csvLog);
    }

    private boolean isFull(String csvLog) {
        String partLog = csvLog;
        int i = 0;
        while (partLog.contains(",")) {
            i++;
            partLog = partLog.substring(partLog.indexOf(",") + 1);
        }
        if (i !=3 )
            throw new InvalidParameterException("not enough values");

        return true;
    }

    /**
     * The method checks if the count of values and if all required values present
     * Also forms jsonString
     * @param csvLog string in comma separated format
     * @return true if csvLog contains all needed values
     * @throws InvalidParameterException else
     */
    private boolean matchPattern(String csvLog) throws InvalidParameterException {
        List<String> values = getValues(csvLog, ',');
        String value;
        for (int i= 0; i < values.size(); i++) {
            value = values.get(i);
            if (i > 0 && i < values.size())
                jsonLog.insert(jsonLog.length()-1,",");

            if (!jsonLog.toString().contains("ip") && isIP(value)) {
                appendJsonLog("ip", value);
                continue;
            }
            if (!jsonLog.toString().contains("duration") && isDuration(value)) {
                appendJsonLog("duration", Float.parseFloat(value));
                continue;
            }
            if (!jsonLog.toString().contains("datetime") && isDateTime(value)) {
                appendJsonLog("datetime", getDateTime(value));
                continue;
            }
            if (!jsonLog.toString().contains("url") && isUrl(value)) {
                appendJsonLog("url",value);
                continue;
            }
            throw new InvalidParameterException("incorrect value");
        }
        return isFull(jsonLog.toString());
    }

    private boolean isDateTime(String value) {
        int currentYear = Calendar.getInstance().get(Calendar.YEAR);
        Pattern datetimePattern = Pattern.
                compile("^(200\\d|201[0-7])" +
                        "-(0?[1-9]|1[012])-(0?[1-9]|[12]\\d|3[01])" +
                        "T([01]?\\d|2[0-3]):([0-5]?\\d):([0-5]?\\d)$");
        Matcher datetimeMatcher = datetimePattern.matcher(value);
        return datetimeMatcher.matches();
    }
    private boolean isDuration(String value) {
        int integerDuration = 0;
        float floatDuration = 0;
        try {
            integerDuration = Integer.parseInt(value);
        }
        catch (NumberFormatException e) {
            try {
                floatDuration = Float.parseFloat(value);
            }
            catch (NumberFormatException e1) {
                return  false;
            }
        }
        return true;
    }
    private boolean isIP(String value) {
        Pattern durationPattern = Pattern.
                compile("^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$");
        Matcher durationMatcher = durationPattern.matcher(value);
        return durationMatcher.matches();
    }
    private boolean isUrl(String value) {
        Pattern durationPattern = Pattern.
                compile("^(http://|https://|www.)?((((\\S+/)+\\S)(\\S*))|(\\S*\\.\\S*))$");
        Matcher durationMatcher = durationPattern.matcher(value);
        return durationMatcher.matches();
    }

    private void appendJsonLog(String attr, Object value) {
        appendJsonLogWithString(attr);
        jsonLog.insert(jsonLog.length()-1, ": ");
        if (value instanceof String)
            appendJsonLogWithString(value.toString());
        else
            jsonLog.insert(jsonLog.length()-1, value);
    }
    private void appendJsonLogWithString(String attr) {
        jsonLog.insert(jsonLog.length()-1,"\"");
        jsonLog.insert(jsonLog.length()-1, attr);
        jsonLog.insert(jsonLog.length()-1,"\"");
    }

    private List<String> getValues(String csvLog, char separator) {
        String partLog = csvLog;
        List<String> values = new ArrayList<>();
        while (partLog.indexOf(separator) != -1) {
            values.add((partLog.substring(0, partLog.indexOf(separator))).trim());
            partLog = partLog.substring(partLog.indexOf(separator) + 1);
        }
        values.add(partLog.trim());
        return values;
    }

    public FindIterable<Document> findIpsByUrl(String url) {
        return collection.find(eq("url", url)).sort(descending("ip")).
                projection(fields(include("ip"), excludeId()));
    }
    public FindIterable<Document> findUrlsOfIp(String ip) {
        return collection.find(eq("ip", ip)).sort(descending("url")).
                projection(fields(include("url"), excludeId()));
    }
    public FindIterable<Document> findUrlsInTime(String dateTime1, String dateTime2) {
        try {
            Date date1 = getDateTime(dateTime1);
            Date date2 = getDateTime(dateTime2);
            SimpleDateFormat parser = new SimpleDateFormat("EEE MMM d HH:mm:ss zzz yyyy", Locale.ENGLISH);
            return collection.find(and(gt("datetime", parser.parse(date1.toString())),
                                        lt("datetime",  parser.parse(date2.toString())))).
                    projection(fields(include("url"), excludeId())).sort(descending("url"));
        }
        catch (ParseException e) {
            return null;
        }
    }

    public FindIterable<Document> findUrlsCount() {
        String collectionName = "urls_count";
        String map = "function (){ emit(this.url, 1); }";
        String reduce = "function(key, values) {" + "var count = 0;"
                + "for (var i in values) {"
                + "count += values[i]; }"
                + "return count; }";
        com.mongodb.MapReduceCommand cmd = new com.mongodb.MapReduceCommand(
                new Mongo("localhost", 27017).getDB("serverlogs").getCollection("logs"),
                map, reduce, collectionName, MapReduceCommand.OutputType.REDUCE, null);
        new Mongo("localhost", 27017).getDB("serverlogs").getCollection("logs").mapReduce(cmd);
        //collection.mapReduce(map, reduce).collectionName(collectionName).toCollection();
        return database.getCollection(collectionName).find().sort(descending("value"));
    }
    public FindIterable<Document> findUrlsDuration() {
        String collectionName = "urls_duration";
        String map = "function (){ emit(this.url, this.duration); }";
        String reduce = "function(key, values) {" + "var duration = 0;"
                + "for (var i in values) {"
                + "duration += values[i]; }"
                + "return duration; }";
        collection.mapReduce(map, reduce).collectionName(collectionName).toCollection();
        return database.getCollection(collectionName).find().sort(descending("value"));
    }
    public FindIterable<Document> findIdsDurationCount() {
        String collectionName = "ips_count_duration";
        String map = "function (){ emit(this.ip, {count:1, duration:this.duration}); }";
        String reduce = "function(key, values) {"
                + "var count = 0; var duration = 0;"
                + "for (var i in values) {"
                + "count += values[i].count;"
                + "duration += values[i].duration;}"
                + "return {count:count, duration:duration}; }";
        collection.mapReduce(map, reduce).collectionName(collectionName).toCollection();
        return database.getCollection(collectionName).find().sort(descending("count", "duration"));
    }
    public FindIterable<Document> findUrlsCountByTime(String dateTime1, String dateTime2) {
        String collectionName = "urls_count_date";
        String map = "function () {"
                + "if (this.datetime > ISODate"
                + "(\"" + dateTime1 + "\")"
                + " && this.datetime < ISODate"
                + "(\"" + dateTime2 + "\"))"
                + " emit(this.url, 1); }";
        String reduce = "function(key, values) { return Array.sum(values); }";
        collection.mapReduce(map, reduce).collectionName(collectionName).toCollection();
        return database.getCollection(collectionName).find().sort(descending("value"));
    }
}
