package usace.cc.plugin.hmsrunner.actions;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.duckdb.DuckDBAppender;
import org.duckdb.DuckDBConnection;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import hec.heclib.dss.HecTimeSeries;
import hec.io.TimeSeriesContainer;
import usace.cc.plugin.api.Action;
import usace.cc.plugin.api.DataSource;
import usace.cc.plugin.api.DataStore;
import usace.cc.plugin.api.FileStore;
import usace.cc.plugin.api.Logger;
import usace.cc.plugin.hmsrunner.model.SSTTable;
import usace.cc.plugin.hmsrunner.utils.Event;

public class DssToParquetAction {
    // HEC time is stored as minutes since Dec 31, 1899 00:00:00 UTC.
    // This offset converts to Java epoch milliseconds.
    private static final long HEC_EPOCH_OFFSET_MS = -2_209_075_200_000L;

    private final Action action;
    private final Logger log = new Logger("DssToParquetAction");

    public DssToParquetAction(Action a) {
        action = a;
    }

    public void computeAction() {
        long actionStart = System.currentTimeMillis();

        Optional<DataSource> opSource = action.getInputDataSource("source");
        if (!opSource.isPresent()) {
            log.error("could not find input datasource named source");
            return;
        }
        Optional<DataSource> opDest = action.getOutputDataSource("destination");
        if (!opDest.isPresent()) {
            log.error("could not find output datasource named destination");
            return;
        }

        DataSource sourceDatasource = opSource.get();
        DataSource destDatasource = opDest.get();
        String sourcePathTemplate = sourceDatasource.getPaths().get("default");
        String destPathTemplate = destDatasource.getPaths().get("default");

        FileStore sourceStore = getFileStore(sourceDatasource.getStoreName());
        FileStore destStore = getFileStore(destDatasource.getStoreName());
        if (sourceStore == null || destStore == null) return;

        // Multi-event mode when a storms datasource is configured.
        // Single-file mode otherwise (e.g. a simple compute_simulation workflow).
        Optional<DataSource> opStorms;
        try {
            opStorms = action.getInputDataSource("storms");
        } catch (Exception e) {
            opStorms = Optional.empty();
        }

        if (opStorms.isPresent()) {
            computeMultiEvent(opStorms.get(), sourcePathTemplate, sourceStore, destPathTemplate, destStore);
        } else {
            int eventId = resolveEventId();
            log.info("dss_to_parquet starting single-event mode", "event_id", eventId);
            processEvent(sourcePathTemplate, sourceStore, destPathTemplate, eventId, destStore);
        }

        long elapsed = (System.currentTimeMillis() - actionStart) / 1000;
        log.info("dss_to_parquet complete", "elapsed_seconds", elapsed);
    }

    // Multi-event: reads storms.csv, derives all event numbers for the storm, processes each SST.dss.
    private void computeMultiEvent(DataSource stormsDs, String sourcePathTemplate, FileStore sourceStore,
                                   String destPathTemplate, FileStore destStore) {
        byte[] stormsCsv;
        try {
            stormsCsv = action.get(stormsDs.getName(), "default", "");
        } catch (Exception e) {
            log.error("could not read storms datasource", "error", e.getMessage());
            return;
        }

        SSTTable table = new SSTTable(new String(stormsCsv).split("\n"));

        String stormName = resolveStormName();
        if (stormName == null || stormName.isEmpty()) {
            log.error("could not determine storm name for dss_to_parquet");
            return;
        }

        Event[] events = table.getEventsByName(stormName);
        if (events.length == 0) {
            log.error("no events found for storm", "storm_name", stormName);
            return;
        }

        int threads = Math.max(1, Math.min(events.length, Runtime.getRuntime().availableProcessors()));
        log.info("dss_to_parquet starting multi-event mode",
                "storm_name", stormName,
                "event_count", events.length,
                "threads", threads);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<?>> futures = new ArrayList<>();

        for (Event e : events) {
            String sourcePath = sourcePathTemplate.replace("$<eventnumber>", e.EventNumber.toString());
            final int eventId = e.EventNumber;
            futures.add(pool.submit(() ->
                processEvent(sourcePath, sourceStore, destPathTemplate, eventId, destStore)));
        }

        pool.shutdown();
        int completed = 0;
        int failed = 0;
        for (Future<?> f : futures) {
            try {
                f.get();
                completed++;
            } catch (Exception ex) {
                failed++;
                log.error("dss_to_parquet worker failed", "error", ex.getMessage());
            }
        }
        log.info("dss_to_parquet all workers finished",
                "completed", completed,
                "failed", failed);
    }

    // Downloads a DSS file from the store, converts it to parquet, and uploads each F-group.
    private void processEvent(String sourcePath, FileStore sourceStore, String destPathTemplate,
                              int eventId, FileStore destStore) {
        long eventStart = System.currentTimeMillis();
        Path tmpDss = null;
        try {
            log.info("dss_to_parquet downloading DSS", "event_id", eventId, "source_path", sourcePath);
            tmpDss = Files.createTempFile("sst_" + eventId + "_", ".dss");
            InputStream is = sourceStore.get(sourcePath).getContent();
            Files.copy(is, tmpDss, StandardCopyOption.REPLACE_EXISTING);
            long downloadSec = (System.currentTimeMillis() - eventStart) / 1000;
            log.info("dss_to_parquet download complete", "event_id", eventId, "elapsed_seconds", downloadSec);

            long convertStart = System.currentTimeMillis();
            Map<String, byte[]> parquetGroups = convertDssToParquet(tmpDss.toString(), eventId);
            long convertSec = (System.currentTimeMillis() - convertStart) / 1000;
            log.info("dss_to_parquet conversion complete",
                    "event_id", eventId,
                    "groups", parquetGroups.size(),
                    "elapsed_seconds", convertSec);

            String dssFileName = sourcePath.substring(sourcePath.lastIndexOf('/') + 1);
            String dssStem = dssFileName.contains(".")
                    ? dssFileName.substring(0, dssFileName.lastIndexOf('.'))
                    : dssFileName;

            long uploadStart = System.currentTimeMillis();
            for (Map.Entry<String, byte[]> entry : parquetGroups.entrySet()) {
                String destPath = buildDestPath(destPathTemplate, eventId, dssStem, entry.getKey());
                log.info("dss_to_parquet uploading parquet",
                        "event_id", eventId,
                        "group", entry.getKey(),
                        "dest_path", destPath,
                        "size_bytes", entry.getValue().length);
                destStore.put(new ByteArrayInputStream(entry.getValue()), destPath);
            }
            long uploadSec = (System.currentTimeMillis() - uploadStart) / 1000;
            long totalSec = (System.currentTimeMillis() - eventStart) / 1000;
            log.info("dss_to_parquet event complete",
                    "event_id", eventId,
                    "upload_seconds", uploadSec,
                    "total_seconds", totalSec);

        } catch (Exception ex) {
            log.error("dss_to_parquet failed for event", "event_id", eventId, "error", ex.getMessage());
        } finally {
            if (tmpDss != null) tmpDss.toFile().delete();
        }
    }

    // Constructs the destination path for a given fKey.
    // "$<eventnumber>" is substituted, then the directory portion is kept and dssStem.fKey.parquet appended.
    private String buildDestPath(String template, int eventId, String dssStem, String fKey) {
        String path = template.replace("$<eventnumber>", Integer.toString(eventId));
        if (path.endsWith("/")) {
            return path + dssStem + "." + fKey + ".parquet";
        }
        int slash = path.lastIndexOf('/');
        String dir = slash >= 0 ? path.substring(0, slash + 1) : "";
        return dir + dssStem + "." + fKey + ".parquet";
    }

    private Map<String, byte[]> convertDssToParquet(String dssFilePath, int eventId) {
        Map<String, byte[]> result = new LinkedHashMap<>();

        HecTimeSeries reader = new HecTimeSeries();
        int status = reader.setDSSFileName(dssFilePath);
        if (status < 0) {
            log.error("dss_to_parquet failed to open DSS file",
                    "event_id", eventId, "path", dssFilePath);
            reader.getLastError().printMessage();
            return result;
        }

        // Read the full DSS catalog and group paths by F-key.
        // F parts like "RUN:SST" are stripped to "RUN".
        // Deduplicate by (A/B/C/E/F) base key. The DSS catalog returns one entry per
        // D-part time block, so a series spanning two months has two catalog entries.
        // We read each series once with the D-part stripped (empty D = read all blocks).
        // Without dedup, the same series would be read and inserted multiple times.
        String[] catalogPaths = reader.getCatalog(false);
        Map<String, List<String>> groups = new LinkedHashMap<>();
        Map<String, java.util.Set<String>> seen = new LinkedHashMap<>();
        for (String path : catalogPaths) {
            String[] parts = path.split("/");
            if (parts.length < 7) continue;
            String fPart = parts[6];
            String fKey = fPart.contains(":") ? fPart.split(":")[0] : fPart;
            // Base key = A/B/C/E/F (strips the D date-range part).
            // A-part is included so series from different basins with the same B/C/E/F
            // are not incorrectly deduplicated.
            String baseKey = parts[1] + "/" + parts[2] + "/" + parts[3] + "/" + parts[5] + "/" + parts[6];
            seen.computeIfAbsent(fKey, k -> new java.util.LinkedHashSet<>());
            if (seen.get(fKey).add(baseKey)) {
                groups.computeIfAbsent(fKey, k -> new ArrayList<>()).add(path);
            }
        }

        log.info("dss_to_parquet catalog read",
                "event_id", eventId,
                "total_catalog_entries", catalogPaths.length,
                "unique_series", groups.values().stream().mapToInt(List::size).sum(),
                "f_groups", groups.size());

        for (Map.Entry<String, List<String>> entry : groups.entrySet()) {
            long groupStart = System.currentTimeMillis();
            byte[] parquet = buildParquet(entry.getKey(), entry.getValue(), reader, eventId);
            if (parquet != null) {
                result.put(entry.getKey(), parquet);
                log.info("dss_to_parquet group built",
                        "event_id", eventId,
                        "group", entry.getKey(),
                        "series_count", entry.getValue().size(),
                        "size_bytes", parquet.length,
                        "elapsed_seconds", (System.currentTimeMillis() - groupStart) / 1000);
            }
        }

        reader.close();
        return result;
    }

    private byte[] buildParquet(String fKey, List<String> paths, HecTimeSeries reader, int eventId) {
        Path tmpFile = null;
        try {
            tmpFile = Files.createTempFile(fKey + "_", ".parquet");
            String parquetPath = tmpFile.toString().replace("'", "''");

            try {
                Class.forName("org.duckdb.DuckDBDriver");
            } catch (ClassNotFoundException e) {
                log.error("DuckDB JDBC driver not found", "error", e.getMessage());
                return null;
            }
            try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("CREATE TABLE ts (datetime TIMESTAMP, value DOUBLE, event_id INTEGER, B VARCHAR, C VARCHAR)");
                }

                DuckDBConnection duckConn = conn.unwrap(DuckDBConnection.class);
                try (DuckDBAppender appender = duckConn.createAppender(DuckDBConnection.DEFAULT_SCHEMA, "ts")) {
                    for (String recordPath : paths) {
                        String[] parts = recordPath.split("/");
                        if (parts.length < 7) continue;
                        String bPart = parts[2];
                        String cPart = parts[3];

                        // Empty D-part (//) causes DSS to read across all storage blocks
                        // rather than truncating at the labeled D-part date boundary.
                        // retrieveAllTimes = true is the explicit "period of record" flag.
                        // The boolean in read(tsc, true) is trimMissing and it strips leading
                        // and trailing missing values and updates startTime/endTime accordingly.
                        String noDPart = "/" + parts[1] + "/" + parts[2] + "/" + parts[3]
                                + "//" + parts[5] + "/" + parts[6] + "/";
                        TimeSeriesContainer tsc = new TimeSeriesContainer();
                        tsc.fullName = noDPart;
                        tsc.retrieveAllTimes = true;
                        int readStatus = reader.read(tsc, true);
                        if (readStatus < 0) {
                            log.warn("dss_to_parquet failed to read series",
                                    "event_id", eventId, "path", noDPart);
                            reader.getLastError().printMessage();
                            continue;
                        }

                        if (tsc.interval <= 0) {
                            // Irregular (event-based) series: each value has its own timestamp
                            // in tsc.times[], stored as HEC minutes since Dec 31, 1899.
                            if (tsc.times == null || tsc.times.length != tsc.values.length) {
                                log.warn("dss_to_parquet skipping irregular series with missing times array",
                                        "event_id", eventId, "path", noDPart);
                                continue;
                            }
                            for (int i = 0; i < tsc.values.length; i++) {
                                long epochMs = HEC_EPOCH_OFFSET_MS + (long) tsc.times[i] * 60_000L;
                                LocalDateTime dt = LocalDateTime.ofInstant(
                                        Instant.ofEpochMilli(epochMs), ZoneOffset.UTC);
                                appender.beginRow();
                                appender.appendLocalDateTime(dt);
                                appender.append(tsc.values[i]);
                                appender.append(eventId);
                                appender.append(bPart);
                                appender.append(cPart);
                                appender.endRow();
                            }
                        } else {
                            // Regular series: reconstruct timestamps from startTime + i * step.
                            // tsc.interval = ticks per step, tsc.timeGranularitySeconds = seconds per tick.
                            // e.g. 1Hour data at minute precision: interval=60 ticks, timeGranularitySeconds=60 s/tick
                            // stepMs = 60 * 60 * 1000 = 3,600,000 ms = 1 hour
                            long startMs = HEC_EPOCH_OFFSET_MS + (long) tsc.startTime * 60_000L;
                            long stepMs = (long) tsc.interval * (long) tsc.timeGranularitySeconds * 1_000L;
                            for (int i = 0; i < tsc.values.length; i++) {
                                long epochMs = startMs + (long) i * stepMs;
                                LocalDateTime dt = LocalDateTime.ofInstant(
                                        Instant.ofEpochMilli(epochMs), ZoneOffset.UTC);
                                appender.beginRow();
                                appender.appendLocalDateTime(dt);
                                appender.append(tsc.values[i]);
                                appender.append(eventId);
                                appender.append(bPart);
                                appender.append(cPart);
                                appender.endRow();
                            }
                        }
                    }
                    appender.flush();
                }

                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("COPY ts TO '" + parquetPath + "' (FORMAT PARQUET, CODEC 'SNAPPY')");
                }
            }

            return Files.readAllBytes(tmpFile);
        } catch (Exception e) {
            log.error("dss_to_parquet error building parquet group",
                    "event_id", eventId, "group", fKey, "error", e.getMessage());
            return null;
        } finally {
            if (tmpFile != null) tmpFile.toFile().delete();
        }
    }

    private FileStore getFileStore(String storeName) {
        Optional<DataStore> opStore = action.getStore(storeName);
        if (!opStore.isPresent()) {
            log.error("could not find store", "store_name", storeName);
            return null;
        }
        FileStore fileStore = (FileStore) opStore.get().getSession();
        if (fileStore == null) {
            log.error("store session is not a FileStore", "store_name", storeName);
            return null;
        }
        return fileStore;
    }

    private String resolveStormName() {
        Optional<String> attr = action.getAttributes().get("storm-name");
        if (attr != null && attr.isPresent()) return attr.get();
        return System.getenv("CC_EVENT_IDENTIFIER");
    }

    private int resolveEventId() {
        Optional<Double> attr = action.getAttributes().get("event-id");
        if (attr != null && attr.isPresent()) return attr.get().intValue();
        String env = System.getenv("CC_EVENT_IDENTIFIER");
        if (env != null && !env.isEmpty()) {
            try { return Integer.parseInt(env); } catch (NumberFormatException ignored) {}
        }
        return 0;
    }
}
