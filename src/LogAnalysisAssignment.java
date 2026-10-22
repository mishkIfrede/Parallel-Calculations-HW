import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/*
 * ПРАКТИЧЕСКОЕ ЗАДАНИЕ: МНОГОПОТОЧНЫЙ АНАЛИЗ ЛОГОВ ВЕБ-СЕРВЕРА
 *
 * Крупные части задания:
 * 1. Сгенерировать набор настоящих файлов с логами. +
 * 2. Реализовать разбор строк и последовательный отчёт.
 * 3. Распараллелить обработку нескольких файлов через ExecutorService.
 * 4. Сделать общую статистику потокобезопасной.
 * 5. Ограничить число одновременно читаемых файлов через Semaphore.
 * 6. Добавить Atomic-счётчики, CAS и корректное ожидание завершения.
 * 7. Реализовать досрочную остановку анализа через volatile.
 * 8. Сформулировать и доказать инварианты корректности и отсутствия дедлоков.
 * 9. Реализовать обработку повреждённых файлов и частичный отчёт.
 *
 * ФАЙЛ МОЖНО ЗАПУСТИТЬ САМОСТОЯТЕЛЬНО:
 *   java LogAnalysisAssignment.java generate logs 20 1000
 *   java LogAnalysisAssignment check
 *
 * Первая команда создаёт каталог logs и файлы access-000.log, access-001.log и т.д.
 * Аргументы: каталог, количество файлов, количество строк в каждом файле.
 * Вторая команда выполняет встроенную проверку генератора и итератора строк.
 *
 * ФОРМАТ СТРОКИ:
 *   METHOD PATH STATUS RESPONSE_BYTES DURATION_MILLIS
 * Например:
 *   GET /api/users 200 820 4
 *
 * Поля разделены одним или несколькими пробелами.
 * Успешный запрос имеет статус от 200 до 399 включительно.
 * Ошибочный запрос имеет статус от 400 и выше.
 *
 * ГОТОВЫЕ СТРУКТУРЫ:
 *   LogEntry       — одна разобранная запись.
 *   LogFile        — путь к одному файлу.
 *   PathStatistics — неизменяемая статистика одного URL.
 *   Report         — итоговый отчёт.
 *   LogFileLines   — итератор строк файла.
 *   GenerateLogs   — генератор входных файлов.
 *
 * ВАЖНО ПРО LogFileLines:
 * Итератор потоконебезопасен. Один экземпляр Iterator<String> нельзя читать
 * из нескольких потоков одновременно. Каждый поток должен получить собственный
 * итератор для своего файла. Нельзя разделить один итератор между задачами.
 * Сам файл можно читать параллельно разными потоками только при условии, что
 * каждый поток создаёт отдельный итератор.
 *
 * ЧАСТИ ЗАДАНИЯ:
 *
 * 1. РАЗБОР СТРОКИ +
 * Реализуйте преобразование одной строки в LogEntry. Проверьте количество
 * полей, преобразуйте числовые значения и выбрасывайте понятную ошибку для
 * повреждённой строки.
 *
 * 2. ПОСЛЕДОВАТЕЛЬНЫЙ ОТЧЁТ +
 * Напишите обработку всех файлов одним потоком. Для каждого URL собирайте
 * количество запросов, количество успешных запросов и ошибок, объём ответов,
 * суммарное и максимальное время обработки. Этот вариант станет эталоном для
 * проверки многопоточной версии.
 *
 * 3. ПАРАЛЛЕЛЬНАЯ ОБРАБОТКА ФАЙЛОВ +
 * Создайте фиксированный ExecutorService. Каждый файл обрабатывается отдельной
 * задачей. Рабочий поток создаёт собственный LogFileLines.Iterator, читает свой
 * файл и передаёт записи в общий аккумулятор. Главный поток должен дождаться
 * завершения всех файлов и только потом построить Report.
 *
 * 4. ПОТОКОБЕЗОПАСНАЯ СТАТИСТИКА +
 * Реализуйте аккумулятор статистики. Сначала допустимо использовать один общий
 * synchronized-метод. Затем реализуйте вариант с отдельным synchronized-аккумулятором
 * для каждого URL и ConcurrentHashMap. Сравните эти варианты на данных, где почти
 * все запросы относятся к одному URL, и на данных с большим числом URL.
 *
 * 5. ОГРАНИЧЕНИЕ ЧТЕНИЯ ФАЙЛОВ +
 * Добавьте Semaphore, ограничивающий число файлов, которые могут обрабатываться
 * одновременно. Рабочих потоков может быть больше разрешений семафора. Разрешение
 * нужно получить перед чтением файла и гарантированно вернуть после завершения.
 * Добавьте наблюдение за текущим и максимальным числом активных файлов.
 *
 * 6. ATOMIC-СЧЁТЧИКИ И CAS
 * Добавьте счётчики обработанных файлов, прочитанных строк, корректных записей,
 * повреждённых записей, ошибок чтения и активных файлов. Для максимального числа
 * активных файлов используйте AtomicInteger и compareAndSet. Обычные изменяемые
 * int/long для этих глобальных счётчиков не используйте.
 *
 * 7. ОЖИДАНИЕ ЗАВЕРШЕНИЯ +
 * Организуйте ожидание всех файлов через CountDownLatch либо через набор Future.
 * Завершение задачи должно учитываться даже при исключении. ExecutorService нужно
 * корректно закрыть и дождаться его завершения.
 *
 * CountDownLatch — это одноразовый счётчик-сигнал. Его создают с начальным
 * числом, например равным количеству файлов. Главный поток вызывает await()
 * и блокируется, пока счётчик не станет равен нулю. Каждая завершившаяся задача
 * один раз вызывает countDown(), уменьшая счётчик. Когда последний счётчик
 * уменьшен до нуля, все ожидающие потоки продолжают работу. Увеличить latch
 * обратно или использовать его повторно нельзя, поэтому для нового запуска
 * создаётся новый объект. Вызов countDown() нужно помещать в finally, чтобы
 * исключение в задаче не оставило главный поток ждать бесконечно.
 *
 * 8. ДОСРОЧНАЯ ОСТАНОВКА +
 * Добавьте volatile-флаг остановки. После остановки новые строки не начинают
 * обрабатываться, а уже начатая операция может завершиться. Опишите, какие данные
 * входят в частичный отчёт. Не оставляйте рабочие потоки и разрешения семафора
 * в подвешенном состоянии.
 *
 * 9. ПОВРЕЖДЁННЫЕ ФАЙЛЫ И ЧАСТИЧНЫЙ ОТЧЁТ +
 * Сделайте так, чтобы ошибка одного файла не отменяла обработку остальных файлов.
 * В итоговом результате отдельно храните список или число файлов, которые не удалось
 * обработать. Решите, что происходит при повреждённой строке: прекращается обработка
 * всего файла или пропускается только эта строка. Выберите один вариант и явно
 * зафиксируйте его в документации. Для полного запуска корректные файлы должны
 * давать полный отчёт, а ошибка должна быть видна пользователю.
 *
 * ИНВАРИАНТЫ, КОТОРЫЕ НУЖНО ДОКАЗАТЬ:
 * - каждая строка полного запуска обработана ровно один раз; (должно работать)
 * - для каждого URL requests = successful + errors; (работает)
 * - maxDuration <= totalDuration при наличии запросов; (должно работать)
 * - 0 <= activeFiles <= semaphoreLimit; (работает)
 * - после завершения анализа activeFiles равно нулю; (работает)
 * - один повреждённый файл не мешает обработке остальных; (работает)
 * - в программе нет дедлока.
 *
 * В доказательстве отсутствия дедлока укажите порядок захвата мониторов и
 * семафоров. Отдельно объясните, может ли поток ждать acquire(), удерживая
 * synchronized-монитор, и почему разрешение семафора возвращается при исключении.
 * Объясните, почему volatile нужен для флага остановки.
 *
 * Порядок блокировок: Semaphore {syncronized(...)}. Синхронизированный метод вложен
 * в семафор, поэтому дэдлока быть не должно.
 * Примерная схема вышеописанной ситуации: synchronized (acc) { limiter.acquire(); }.
 * Такого построения кода в программе нет (см. ответ 1), поэтому в этой программе такой
 * ситуации не должно возникнуть.
 * Разрешение семафора возвращается при исключении, так как сама суть блокировки - во временном
 * захвате ресурса, вне зависимости от того, как закончилась работа с ресурсом, доступ к нему должен
 * быть освобождён для других потоков.
 * Volatile нужен для флага остановки для видимости изменения его значения всеми потоками.
 * Рабочие потоки могут закэшировать значение флага, и при его изменении не увидеть правильного
 * значения, выставленного специальным потоком.
 *
 * НЕБОЛЬШИЕ ЭКСПЕРИМЕНТЫ ПОСЛЕ РЕАЛИЗАЦИИ:
 * - сравнить 1 и 4 рабочих потока;
 * - сравнить лимит семафора 1 и 4;
 * - сравнить глобальную блокировку и блокировки по URL;
 * - добавить одну повреждённую строку или один повреждённый файл и проверить,
 *   что остальные данные всё равно попадают в отчёт.
 * Эксперименты не требуют дополнительной архитектуры или новых классов.
 */
public class LogAnalysisAssignment {
    // разобранная строка запроса
    public record LogEntry(
            String method,
            String path,
            int status,
            long responseBytes,
            long durationMillis) {}


    public record LogFile(Path path) {}

    // статистика данных
    public record PathStatistics(
            long requests,
            long successful,
            long errors,
            long responseBytes,
            long totalDurationMillis,
            long maxDurationMillis
    ) {}

    // отчёт (статистика по всем файлам)
    public record Report(
            long totalRequests,
            long successfulRequests,
            long errorRequests,
            long responseBytes,
            long maxDurationMillis
    ) {}

    public record AnalysisResult(
            Map<String, PathStatistics> perPath,
            Report total
    ) {}

    /**
     * str.split("//s+") - массив строк
     * Возвращает отдельный итератор для чтения строк одного файла.
     *
     * ВАЖНО: Iterator<String> потоконебезопасен. Один экземпляр нельзя
     * одновременно читать из нескольких потоков. Для каждого потока нужен
     * отдельный вызов LogFileLines.iterator(path).
     */
    public static final class LogFileLines {
        private LogFileLines() {}

        public static Iterator<String> iterator(Path path) {
            try {
                BufferedReader reader = Files.newBufferedReader(
                        path,
                        StandardCharsets.UTF_8
                );

                return new Iterator<>() {
                    private String nextLine = readNext();
                    private boolean closed;

                    private String readNext() {
                        try {
                            String line = reader.readLine();
                            if (line == null) {
                                close();
                            }
                            return line;
                        } catch (IOException exception) {
                            close();
                            throw new RuntimeException(
                                    "Cannot read " + path,
                                    exception
                            );
                        }
                    }
                    private void close() {
                        if (closed) {
                            return;
                        }
                        closed = true;
                        try {
                            reader.close();
                        } catch (IOException exception) {
                            throw new RuntimeException(
                                    "Cannot close " + path,
                                    exception
                            );
                        }
                    }

                    @Override
                    public boolean hasNext() {
                        return nextLine != null;
                    }

                    @Override
                    public String next() {
                        if (nextLine == null) {
                            throw new NoSuchElementException();
                        }
                        String result = nextLine;
                        nextLine = readNext();
                        return result;
                    }
                };
            } catch (IOException exception) {
                throw new RuntimeException(
                        "Cannot open " + path,
                        exception
                );
            }
        }
    }

    /** Генерирует реальные текстовые файлы с детерминированными данными. */
    public static final class GenerateLogs {
        private static final String[] METHODS = {
                "GET", "POST", "PUT", "DELETE"
        };
        private static final String[] PATHS = {
                "/", "/index.html", "/login", "/api/users",
                "/api/orders", "/api/products", "/search", "/health"
        };
        private static final int[] STATUSES = {
                200, 200, 200, 201, 204,
                301, 400, 401, 404, 500
        };

        private GenerateLogs() {}

        public static void generate(
                Path outputDirectory,
                int fileCount,
                int linesPerFile
        ) throws IOException {
            if (fileCount < 0 || linesPerFile < 0) {
                throw new IllegalArgumentException(
                        "Counts must be non-negative"
                );
            }

            Files.createDirectories(outputDirectory);
            Random random = new Random(42);

            for (int fileId = 0; fileId < fileCount; fileId++) {
                Path file = outputDirectory.resolve(
                        "access-%03d.log".formatted(fileId)
                );

                try (BufferedWriter writer = Files.newBufferedWriter(
                        file,
                        StandardCharsets.UTF_8
                )) {
                    for (int line = 0; line < linesPerFile; line++) {
                        String method =
                                METHODS[random.nextInt(METHODS.length)];
                        String path =
                                PATHS[random.nextInt(PATHS.length)];
                        int status =
                                STATUSES[random.nextInt(STATUSES.length)];
                        long responseBytes =
                                100L + random.nextInt(100_000);
                        long durationMillis =
                                1L + random.nextInt(500);

                        writer.write(
                                "%s %s %d %d %d%n".formatted(
                                        method,
                                        path,
                                        status,
                                        responseBytes,
                                        durationMillis
                                )
                        );
                    }
                }
            }
        }
    }

    /** Проверяет генератор и чтение строк без участия студенческого анализатора. */
    private static void selfCheck() throws IOException {
        Path directory = Files.createTempDirectory("log-assignment-");
        GenerateLogs.generate(directory, 3, 7);

        int files = 0;
        int lines = 0;

        try (var paths = Files.list(directory)) {
            for (Path path : paths.toList()) {
                files++;
                Iterator<String> iterator = LogFileLines.iterator(path);

                while (iterator.hasNext()) {
                    String line = iterator.next();
                    String[] fields = line.trim().split("\\s+");
                    if (fields.length != 5) {
                        throw new AssertionError(
                                "Line: " + line + "is damaged (has unexpected number of fields)"
                        );
                    }
                    LogEntry logEntry = new LogEntry(fields[0], fields[1], Integer.parseInt(fields[2]), Long.parseLong(fields[3]), Long.parseLong(fields[4]));
                    lines++;
                }
            }
        }
        if (files != 3 || lines != 21) {
            throw new AssertionError(
                    "Expected 3 files and 21 lines, got "
                            + files + " files and " + lines + " lines"
            );
        }

        System.out.println(
                "OK: generated " + files
                        + " files and read " + lines + " lines"
        );
    }

    // аккумулятор все нужных показателей, до внесенияя их в статистику
    private static final class StatsAccumulator {
        long requests;
        long successful;
        long errors;
        long responseBytes;
        long totalDurationMillis;
        long maxDurationMillis;

        void add(LogEntry entry) { // добавление строки запроса в аккумулятор
            requests++;
            if (entry.status() >= 200 && entry.status() < 300) {
                successful++;
            } else if (entry.status() >= 400) {
                errors++;
            }
            responseBytes += entry.responseBytes();
            totalDurationMillis += entry.durationMillis();
            if (entry.durationMillis() > maxDurationMillis) {
                maxDurationMillis = entry.durationMillis();
            }
        }

        void merge(StatsAccumulator other) { // склеивание двух аккумуляторов
            requests += other.requests;
            successful += other.successful;
            errors += other.errors;
            responseBytes += other.responseBytes;
            totalDurationMillis += other.totalDurationMillis;
            if (other.maxDurationMillis > maxDurationMillis) {
                maxDurationMillis = other.maxDurationMillis;
            }
        }

        PathStatistics toRecord() { // создание статистики
            return new PathStatistics(
                    requests,
                    successful,
                    errors,
                    responseBytes,
                    totalDurationMillis,
                    maxDurationMillis
            );
        }
    }
    // Обработка всех файлов одним потоком
    public static AnalysisResult analyzeSingleThread(Path directory) throws IOException {
        Map<String, StatsAccumulator> perPath = new LinkedHashMap<>(); // карта аккумуляторов по каждому url
        directory = Path.of("C:/Users/mishka_frede/IdeaProjects/parallel_calculations_homeworks/logs");
        try (var paths = Files.list(directory)) {
            for (Path path : paths.toList()) {
                Iterator<String> iterator = LogFileLines.iterator(path);
                while (iterator.hasNext()) {
                    String line = iterator.next();
                    String[] fields = line.trim().split("\\s+");
                    if (fields.length != 5) {
                        throw new AssertionError(
                                "Line: " + line + "is damaged (has unexpected number of fields)"
                        );
                    }
                    LogEntry logEntry = new LogEntry(fields[0], fields[1], Integer.parseInt(fields[2]), Long.parseLong(fields[3]), Long.parseLong(fields[4]));
                    StatsAccumulator acc = perPath.get(logEntry.path());
                    if (acc == null) {
                        acc = new StatsAccumulator();
                        perPath.put(logEntry.path(), acc);
                    }
                    acc.add(logEntry);
                }
            }
        }
        Map<String, PathStatistics> statistics = new LinkedHashMap<>(); // карта статистик по каждому url
        StatsAccumulator total = new StatsAccumulator(); // общий аккумулятор по всем файлам

        perPath.forEach((url, acc) -> {
            statistics.put(url, acc.toRecord());
            total.merge(acc);
        });

        Report report = new Report( // создаём отчёт по общему аккумулятору
                total.requests,
                total.successful,
                total.errors,
                total.responseBytes,
                total.maxDurationMillis
        );

        return new AnalysisResult(statistics, report);
    }
// ________________________________________________________________________________________________________________
    // Карта статистик для обработки всех файлов несколькими потоками
    public static final class ConcurrentStats {
        private final ConcurrentMap<String, StatsAccumulator> perPath =
                new ConcurrentHashMap<>();
        // добавление записи
        public void add(LogEntry entry) {
            StatsAccumulator acc = perPath.computeIfAbsent(
                    entry.path(),
                    k -> new StatsAccumulator()
            );
            synchronized (acc) { // синхронизированный метод - только один поток может изменить статистику
                acc.add(entry);
            }
        }

        // снимок текущего состояния карты статистик по каждому url
        public Map<String, PathStatistics> snapshot() {
            Map<String, PathStatistics> result = new LinkedHashMap<>();
            perPath.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(e -> result.put(e.getKey(), e.getValue().toRecord()));
            return result;
        }

        // составление отчёта по всем файлам
        public Report toReport() {
            StatsAccumulator total = new StatsAccumulator(); // общий аккумулятор по всем файлам
            perPath.values().forEach(total::merge);
            return new Report(
                    total.requests,
                    total.successful,
                    total.errors,
                    total.responseBytes,
                    total.maxDurationMillis
            );
        }
    }
    // фильтр log-файлов из указанной папки
    private static List<Path> listLogFiles(Path directory) throws IOException {
        try (var paths = Files.list(directory)) {
            return paths
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(".log"))
                    .sorted()
                    .toList();
        }
    }

    // ________________________________________________________________________________________________________________
    // Часть с семафором
    public static final class FileConcurrencyLimiter { // ограничител числа одновременно обрабатываемых файлов

        private final Semaphore permits;
        private final AtomicInteger active = new AtomicInteger(); // текущее число активных файлов
        private final AtomicInteger maxActive = new AtomicInteger(); // пиковое число активных файлов
        private final AtomicLong totalAcquired = new AtomicLong();

        public FileConcurrencyLimiter(int maxConcurrentFiles) { // конструктор семафора
            if (maxConcurrentFiles <= 0) {
                throw new IllegalArgumentException(
                        "maxConcurrentFiles must be positive"
                );
            }
            this.permits = new Semaphore(maxConcurrentFiles, true); // true отвечает за порядок FIFO в семафоре
        }
        public boolean tryAcquire(long timeout, TimeUnit unit) // попытка захватить разрешение
                throws InterruptedException {
            if (!permits.tryAcquire(timeout, unit)) return false;
            int now = active.incrementAndGet();
            totalAcquired.incrementAndGet();
            maxActive.accumulateAndGet(now, Math::max); // атомарно увеличиваем пиковое число, если текущее значение его превысило
            return true;
        }

        public void restorePermit() { // возврат разрешение при случайном захвате
            permits.release();
        }

        public void release() { // возврат разрешения
            active.decrementAndGet();
            permits.release();
        }
        // наблюдаемые значения
        public int active()            { return active.get(); } // текущее число активных файлов
        public int maxActive()         { return maxActive.get(); } // пиковое число активных файлов
        public int availablePermits()  { return permits.availablePermits(); } // число разрешений
        public long totalAcquired()    { return totalAcquired.get(); } // кол-во обработанных файлов
    }
// ________________________________________________________________________________________________________________
    // Часть с Atomic-счётчиками
    public static final class ProcessingCounters {

    private final AtomicLong filesProcessed = new AtomicLong(); // кол-во обработанных файлов
    private final AtomicLong filesSkipped = new AtomicLong();   // кол-во файлов, не начатых из-за флага
    private final AtomicLong linesRead = new AtomicLong(); // кол-во прочитанных строк
    private final AtomicLong validRecords = new AtomicLong(); // кол-во корректных записей
    private final AtomicLong damagedRecords = new AtomicLong(); // кол-во повреждённых записей
    private final AtomicLong readErrors = new AtomicLong(); // кол-во ошибок чтения

    // инкременты
    public void incFilesProcessed() { filesProcessed.incrementAndGet(); }
    public void incFilesSkipped() { filesSkipped.incrementAndGet(); }
    public void incLinesRead() { linesRead.incrementAndGet(); }
    public void incValidRecords() { validRecords.incrementAndGet(); }
    public void incDamagedRecords() { damagedRecords.incrementAndGet(); }
    public void incReadErrors() { readErrors.incrementAndGet(); }

    // чтение
    public long filesProcessed() { return filesProcessed.get(); }
    public long filesSkipped() { return filesSkipped.get(); }
    public long linesRead() { return linesRead.get(); }
    public long validRecords() { return validRecords.get(); }
    public long damagedRecords() { return damagedRecords.get(); }
    public long readErrors() { return readErrors.get(); }

    public CountersSnapshot snapshot() {
        return new CountersSnapshot(
                filesProcessed(), filesSkipped(), linesRead(),
                validRecords(), damagedRecords(), readErrors()
        );
    }

    public record CountersSnapshot(
            long filesProcessed,
            long filesSkipped,
            long linesRead,
            long validRecords,
            long damagedRecords,
            long readErrors
    ) {
        @Override
        public String toString() {
            return "filesProcessed=" + filesProcessed
                    + ", filesSkipped=" + filesSkipped
                    + ", linesRead=" + linesRead
                    + ", validRecords=" + validRecords
                    + ", damagedRecords=" + damagedRecords
                    + ", readErrors=" + readErrors;
        }
    }
}
// ________________________________________________________________________________________________________________
    // Часть с volatile-флагом - имеется один поток, который может выставить true, при этом все начатые задачи
    // завершаются, а новые не начинаются
    public static final class StopFlag {
        private volatile boolean stopped;
        public void requestStop() { stopped = true; }
        public boolean isStopped() { return stopped; }
    }
// ________________________________________________________________________________________________________________
    // Сама обработка всех файлов несколькими потоками
    public static AnalysisResult analyzeMultiThreaded(
            Path directory,
            int threadCount,
            int maxConcurrentFiles,
            long stopAfterMillis // без остановки - 0
    ) throws IOException, InterruptedException {

        List<Path> files = listLogFiles(directory);
        ConcurrentStats shared = new ConcurrentStats();
        FileConcurrencyLimiter limiter =
                new FileConcurrencyLimiter(maxConcurrentFiles);
        ProcessingCounters counters = new ProcessingCounters();
        StopFlag stop = new StopFlag();
        ExecutorService pool = Executors.newFixedThreadPool(
                Math.max(1, threadCount)
        );

        Thread stopper = null; // сздаём отдельный поток, ответственный за флаг
        if (stopAfterMillis > 0) {
            stopper = new Thread(() -> {
                try {
                    Thread.sleep(stopAfterMillis);
                    stop.requestStop();
                    System.out.println("[stopper] stop requested after "
                            + stopAfterMillis + " ms");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            stopper.setDaemon(true);
            stopper.start();
        }

        try {
            List<Future<?>> futures = new ArrayList<>(files.size());

            for (Path file : files) {
                futures.add(pool.submit(() -> {
                    if (stop.isStopped()) { // не начинаем новый файл, если есть остановка
                        counters.incFilesSkipped();
                        return null;
                    }

                    if (!acquireOrAbort(limiter, stop, counters)) { // разрешение семафора с учётом флага
                        counters.incFilesSkipped();
                        return null;
                    }
                    try { // начинаем обработку файла
                        processFile(file, shared, counters, stop);
                    } finally {
                        limiter.release(); // отпускаем разрешение
                        counters.incFilesProcessed();
                    }
                    return null;
                }));
            }

            for (Future<?> future : futures) { // ожидание завершения всех задач главным потоком через future
                try {
                    future.get();
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof RuntimeException re) throw re;
                    throw new RuntimeException("Worker failed", cause);
                }
            }
        } finally {
            if (stopper != null) {
                stopper.interrupt();
                stopper.join(1000);
            }
            pool.shutdown();
            if (!pool.awaitTermination(1, TimeUnit.MINUTES)) {
                pool.shutdownNow();
            }
            while (limiter.availablePermits() < maxConcurrentFiles) {
                limiter.restorePermit();
            }
        }
        System.out.println("Number of active files: " + limiter.active());
        ProcessingCounters.CountersSnapshot snap = counters.snapshot(); // подготовка частичного отчёта
        System.out.println("[result] " + snap);

        return new AnalysisResult(shared.snapshot(), shared.toReport());
    }

    private static boolean acquireOrAbort(
            FileConcurrencyLimiter limiter,
            StopFlag stop,
            ProcessingCounters counters
    ) {
        while (!stop.isStopped()) {
            try {
                if (limiter.tryAcquire(100, TimeUnit.MILLISECONDS)) {
                    return true;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                counters.incReadErrors(); // прерывание - файл не обработан
                return false;
            }
        }
        return false; // дождались флага - разрешение не берём
    }

    private static void processFile(
            Path file,
            ConcurrentStats shared,
            ProcessingCounters counters,
            StopFlag stop
    ) {
        Iterator<String> iterator;
        try {
            iterator = LogFileLines.iterator(file);
        } catch (RuntimeException e) {
            counters.incReadErrors();
            return; // файл открыть не удалось - ошибка
        }
        while (!stop.isStopped()) { // проверяем остановку до прочтения каждой строки
            String line;
            try {
                if (!iterator.hasNext()) break;
                line = iterator.next();
            } catch (RuntimeException e) {
                counters.incReadErrors();
                break;
            }
            counters.incLinesRead();

            if (line.isBlank()) continue;

            LogEntry entry = tryParse(line, file);
            if (entry == null) {
                counters.incDamagedRecords();
            } else {
                counters.incValidRecords();
                shared.add(entry);
            }
        }
    }

    private static LogEntry tryParse(String line, Path source) {
        String[] fields = line.trim().split("\\s+");
        if (fields.length != 5) {
            return null;
        }
        try {
            return new LogEntry(
                    fields[0], fields[1],
                    Integer.parseInt(fields[2]),
                    Long.parseLong(fields[3]),
                    Long.parseLong(fields[4])
            );
        } catch (NumberFormatException e) {
            return null;
        }
    }

    // красивый вывод статистики и отчёта на экран
    public static void printResult(AnalysisResult result) {
        System.out.println("Per-URL statistics:");
        result.perPath().forEach((url, stat) ->
                System.out.printf(
                        "  %-20s reqests=%-6d successful=%-6d errors=%-6d responseBytes=%-10d " +
                                "totalDurationMillis=%-8d maxDurationMillis=%d%n",
                        url,
                        stat.requests(),
                        stat.successful(),
                        stat.errors(),
                        stat.responseBytes(),
                        stat.totalDurationMillis(),
                        stat.maxDurationMillis()
                )
        );
        Report r = result.total();
        System.out.println();
        System.out.println("Total report:");
        System.out.printf("  requests      = %d%n", r.totalRequests());
        System.out.printf("  successful    = %d%n", r.successfulRequests());
        System.out.printf("  errors        = %d%n", r.errorRequests());
        System.out.printf("  responseBytes = %d%n", r.responseBytes());
        System.out.printf("  maxDurationillis   = %d ms%n", r.maxDurationMillis());
        return;
    }

    public static void main(String[] args) throws Exception {
        // создание log-файлов
        if (args.length > 0 && args[0].equals("generate")) {
            Path directory = Path.of(
                    args.length > 1 ? args[1] : "logs"
            );
            int fileCount = args.length > 2
                    ? Integer.parseInt(args[2])
                    : 20;
            int linesPerFile = args.length > 3
                    ? Integer.parseInt(args[3])
                    : 1_000;

            GenerateLogs.generate(
                    directory,
                    fileCount,
                    linesPerFile
            );

            System.out.println(
                    "Generated " + fileCount
                            + " files in " + directory
            );
            return;
        }
        // проверка log-файлов
        if (args.length > 0 && args[0].equals("check")) {
            selfCheck();
            return;
        }
        // обработка одним потоком
        if (args.length > 0 && args[0].equals("analyze")) {
            Path directory = Path.of(args.length > 1 ? args[1] : "logs");
            AnalysisResult result = analyzeSingleThread(directory);
            printResult(result);
        }
        // обработка несколькими потоками
        if (args.length > 0 && args[0].equals("analyze-mt")) {
            Path directory = Path.of(args.length > 1 ? args[1] : "logs");
            int threads = args.length > 2
                    ? Integer.parseInt(args[2])
                    : Runtime.getRuntime().availableProcessors();
            int maxFiles = args.length > 3
                    ? Integer.parseInt(args[3])
                    : 4;

            AnalysisResult result = analyzeMultiThreaded(directory, threads, maxFiles, 10);
            printResult(result);
            return;
        }

        System.out.println(
                "Usage: java LogAnalysisAssignment "
                        + "generate <directory> <files> <lines>"
        );
        System.out.println(
                "       java LogAnalysisAssignment check"
        );
        System.out.println(
                "       java LogAnalysisAssignment analyze"
        );
        System.out.println(
                "       java LogAnalysisAssignment analyze-mt <directory> <threads> <maxFiles> <stop>"
        );
    }
}
