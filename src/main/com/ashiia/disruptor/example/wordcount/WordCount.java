package com.ashiia.disruptor.example.wordcount;

import java.io.*;

public class WordCount {

    public static final int SIZE = 1024 * 32;
    public static final int ITERS = 5;
    public static final int MAPPERS = 3;

    public static void main(String[] args) throws Exception {
        String[] fileContents = getFileContents();
        for (int numberOfMappers = 1; numberOfMappers <= MAPPERS; numberOfMappers++) {
            run(fileContents, numberOfMappers);
        }

    }

    private static void run(String[] fileContents, int numberOfMappers) throws InterruptedException {
        QueueCommand queueCommand = new QueueCommand(numberOfMappers);
        DisruptorCommand disruptorCommand = new DisruptorCommand(numberOfMappers);

        long disruptorTimeTaken = 0L;
        long queuesTimeTaken = 0L;
        for (int i = 0; i < ITERS; i++) {
            System.gc();
            disruptorTimeTaken += disruptorCommand.execute(fileContents);
            disruptorCommand.reset();
            queuesTimeTaken += queueCommand.execute(fileContents);
            queueCommand.reset();

        }
        disruptorCommand.halt();
        queueCommand.halt();

        System.out.format("number_mappers: %d, disruptor_avg_time: %d, queue_avg_time: %d\n", numberOfMappers, disruptorTimeTaken / ITERS, queuesTimeTaken / ITERS);
    }

    private static String[] getFileContents() throws IOException {
        FilenameFilter filenameFilter = new TextFilenameFilter();
        // current directory
        File dir = new File(".");
        String[] textFiles = dir.list(filenameFilter);
        String[] textFileContents = new String[textFiles.length];
        for (int i = 0; i < textFiles.length; i++) {
            System.out.println(textFiles[i]);
            textFileContents[i] = readFileAsString(textFiles[i]);
        }
        return textFileContents;
    }

    private static String readFileAsString(String filePath) throws java.io.IOException {
        StringBuffer fileData = new StringBuffer(1000);
        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        char[] buf = new char[1024];
        int numRead = 0;
        while ((numRead = reader.read(buf)) != -1) {
            String readData = String.valueOf(buf, 0, numRead);
            fileData.append(readData);
            buf = new char[1024];
        }
        reader.close();
        return fileData.toString();
    }

    private static class TextFilenameFilter implements FilenameFilter {
        public boolean accept(File file, String name) {
            if (name.endsWith("*.txt")) return true;
            else return false;
        }
    }
}
