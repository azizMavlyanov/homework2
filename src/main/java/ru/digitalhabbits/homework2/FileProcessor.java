package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.*;

import static java.lang.Runtime.getRuntime;
import static java.nio.charset.Charset.defaultCharset;
import static org.slf4j.LoggerFactory.getLogger;

public class FileProcessor {
    private static final Logger logger = getLogger(FileProcessor.class);
    public static final int CHUNK_SIZE = 2 * getRuntime().availableProcessors();

    public void process(@Nonnull String processingFileName, @Nonnull String resultFileName) {
        checkFileExists(processingFileName);

        final File file = new File(processingFileName);
        final Phaser phaser = new Phaser(CHUNK_SIZE + 1);
        final ExecutorService executorService = Executors.newFixedThreadPool(CHUNK_SIZE);
        final Exchanger<List<Pair<String, Integer>>> exchanger = new Exchanger<>();
        final FileWriter fileWriter = new FileWriter(resultFileName, exchanger);
        final Thread fileWriterThread = new Thread(fileWriter);

//        NotImplemented: запускаем FileWriter в отдельном потоке
        fileWriterThread.start();

        try (final Scanner scanner = new Scanner(file, defaultCharset())) {
            while (scanner.hasNext()) {
//                NotImplemented: вычитываем CHUNK_SIZE строк для параллельной обработки
                final List<String> listOfLines = readByLine(scanner);
//                NotImplemented: обрабатывать строку с помощью LineProcessor. Каждый поток обрабатывает свою строку.
                final List<Pair<String, Integer>> processedLines = processLines(listOfLines, executorService, phaser);
//                NotImplemented: добавить обработанные данные в результирующий файл
                sendProcessedLinesToFileWriter(processedLines, exchanger);
            }
        } catch (IOException exception) {
            logger.error("", exception);
        }

//        NotImplemented: остановить поток writerThread
        fileWriterThread.interrupt();
        executorService.shutdown();
        logger.info("Finish main thread {}", Thread.currentThread().getName());
    }

    private void sendProcessedLinesToFileWriter(List<Pair<String, Integer>> processedLines,
                                                Exchanger<List<Pair<String, Integer>>> exchanger) {
        try {
            exchanger.exchange(processedLines);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private List<String> readByLine(Scanner scanner) {
        final List<String> result = new ArrayList<>();

        int counter = 0;
        while (counter < CHUNK_SIZE && scanner.hasNextLine()) {
            String line = scanner.nextLine();
            result.add(line);
            counter++;
        }

        return result;
    }

    private void checkFileExists(@Nonnull String fileName) {
        final File file = new File(fileName);
        if (!file.exists() || file.isDirectory()) {
            throw new IllegalArgumentException("File '" + fileName + "' not exists");
        }
    }

    private List<Pair<String, Integer>> processLines(List<String> listOfLines,
                                                     ExecutorService executorService,
                                                     Phaser phaser) {
        final List<Pair<String, Integer>> resultList = new ArrayList<>();

        for (String line : listOfLines) {
            executorService.submit(() -> {
               Pair<String, Integer> pair = new LineCounterProcessor().process(line);
               resultList.add(pair);
               phaser.arrive();
            });
        }
        phaser.arriveAndAwaitAdvance();

        return resultList;
    }
}
