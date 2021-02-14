package com.sparkTutorial.rdd.airports;

import com.sparkTutorial.SparkUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

import static com.sparkTutorial.rdd.airports.WordSparkUtils.readAirportsFile;

@Slf4j
public abstract class AbstractAirportsProblem<T> {


    /**
     * Read airports file select/filter and save it to output file
     *
     * @param partsFolderPath - folder that spark uses to stores output
     * @throws IOException
     */
    public void processData(String inputFile, String partsFolderPath, String outputFile) throws IOException {
        final SparkSession sc = SparkUtils.setup();

        deleteDir(partsFolderPath);
        deleteFile(outputFile);

        final Dataset<Row> rdd = readAirportsFile(sc, inputFile);
        transformAndSave(partsFolderPath, rdd);

        renamePartsToFilename(partsFolderPath, outputFile);
        deleteDir(partsFolderPath);
    }

    private void transformAndSave(String partsFolderPath, Dataset<Row> rdd) {
        saveToFile(selectAndFilter(rdd), partsFolderPath);
    }

    protected abstract void saveToFile(T selectAndFilter, String partsFolderPath);

    protected abstract T selectAndFilter(Dataset<Row> rdd);

    protected void renamePartsToFilename(String partsFolder, String filepath) throws IOException {
        final File dir = new File(partsFolder);
        log.info(dir.getAbsolutePath());
        final File[] files = new File(partsFolder).listFiles((file, s) -> s.endsWith(".csv"));
        assert files != null && files.length > 0;
        log.info(Arrays.toString(files));
        FileUtils.moveFile(files[0], new File(filepath));
    }

    private void deleteDir(String path) throws IOException {
        final File directory = new File(path);
        if (directory.exists()) {
            FileUtils.deleteDirectory(directory);
        }
    }

    protected void deleteFile(String path) throws IOException {
        Files.deleteIfExists(new File(path).toPath());
    }
}
