import org.duckdb.DuckDBConnection;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public class Issue206Reproducer {

    public static void main(String[] args) throws Exception {
        int numCores = Runtime.getRuntime().availableProcessors();
        int numConnThreads = 2;
        int numDbWorkerThreads = numCores/2;
        int numBatches = 1 << 8;
        int batchSize = 1 << 10;

        System.out.println("CPU cores: " + numCores);
        System.out.println("Connection threads: " + numConnThreads);
        System.out.println("DB worker threads: " + numDbWorkerThreads);
        System.out.println("Batches count: " + numBatches);
        System.out.println("Batch size: " + batchSize);

        Files.deleteIfExists(Path.of("test.db"));

        AtomicLong counter = new AtomicLong(0);

        Properties config = new Properties();
        config.put("threads", numDbWorkerThreads);
        try (DuckDBConnection conn = DriverManager.getConnection("jdbc:duckdb:test.db", config).unwrap(DuckDBConnection.class)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE tab1 (col1 BIGINT, col2 VARCHAR)");
            }

            ExecutorService executorService = Executors.newFixedThreadPool(numConnThreads);
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < numBatches; i++) {
                Future<?> fut = executorService.submit(() -> {
                    try (Connection connDup = conn.duplicate();
                         PreparedStatement ps = connDup.prepareStatement("INSERT INTO tab1 VALUES(?, ?)")) {
                        connDup.setAutoCommit(false);
                        for (int j = 0; j < batchSize; j++) {
                            long num = counter.incrementAndGet();
                            ps.setLong(1, num);
                            ps.setString(2, num + "foo");
                            ps.addBatch();
                            if (0 == num % 10000) {
                                System.out.println(num);
                            }
                        }
                        ps.executeBatch();
                        connDup.commit();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
                futures.add(fut);
            }

            for (Future<?> fut : futures) {
                fut.get();
            }

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM tab1")) {
                rs.next();
                System.out.println("Records inserted: " + rs.getLong(1));
            }

            executorService.shutdown();
        }
    }
}
