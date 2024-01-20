package org.example;

import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class HoodieSparkTest {

    private final String dataDir = "/tmp/lakehouse";
    private final SparkSession session = new HoodieSparkSession(dataDir, true).getSession();
    private final String schemaName = "hudi";
    private final String tableName = "avatar_books";

    @BeforeEach
    void setup() throws IOException {
        session.sql("DROP DATABASE IF EXISTS " + schemaName + " CASCADE ");

        var dataDirPath = Path.of(dataDir);
        FileUtils.cleanDirectory(dataDirPath.toFile());

        session.sql("CREATE DATABASE IF NOT EXISTS " + schemaName);
    }

    @Test
    public void executeTableCreateWriteAndSelect() {
        // Given
        var tablePath = dataDir + "/" + schemaName + "/" + tableName;

        // When
        var avatarBooks = List.of(
                new AvatarBook("Book One", "Water"),
                new AvatarBook("Book Two", "Earth"),
                new AvatarBook("Book Three", "Fire")
        );

        var booksDataset = session.createDataset(avatarBooks, AvatarBook.getEncoder());

        booksDataset
                .write()
                .format("hudi")
                .option("hoodie.table.name", tableName)
                .mode(SaveMode.Overwrite)
                .save(tablePath);

        // Then
        var resultDataFrame = session.read().format("hudi").load(tablePath);
        resultDataFrame.createOrReplaceTempView(tableName);

        var result = session.sql("SELECT * FROM " + tableName);
        result.show();

        assertThat(result.as(AvatarBook.getEncoder()).collectAsList()).hasSameElementsAs(avatarBooks);
    }

    @Test
    public void executeTableCreateWriteAndSelectSql() {
        // When
        session.sql("CREATE TABLE IF NOT EXISTS %s.%s(title STRING, description STRING) USING hudi".formatted(schemaName, tableName));
        session.sql("INSERT INTO %s.%s SELECT 'Book One', 'Water';".formatted(schemaName, tableName));
        session.sql("INSERT INTO %s.%s SELECT 'Book Two', 'Earth';".formatted(schemaName, tableName));

        // Then
        var result = session.sql("SELECT * FROM " + schemaName + "." + tableName);
        result.show();

        assertThat(result).isNotNull();
    }

    @Test
    public void executeTableCreateWriteAndSelectMixed() {
        // Given
        var tablePath = dataDir + "/" + schemaName + ".db" + "/" + tableName;

        // When
        session.sql("CREATE TABLE IF NOT EXISTS %s.%s(title STRING, description STRING) USING hudi OPTIONS (primaryKey = 'title')".formatted(schemaName, tableName));

        var avatarBooks = List.of(
                new AvatarBook("Book One", "Water"),
                new AvatarBook("Book Two", "Earth"),
                new AvatarBook("Book Three", "Fire")
        );

        var booksDataset = session.createDataset(avatarBooks, AvatarBook.getEncoder());

        booksDataset
                .write()
                .format("hudi")
                .option("hoodie.table.name", tableName)
                .option("hoodie.datasource.write.recordkey.field", "title")
                .option("hoodie.datasource.write.partitionpath.field", "title")
                .option("hoodie.datasource.write.precombine.field", "title")
                .mode(SaveMode.Append)
                .save(tablePath);

        // Then
        var result = session.sql("SELECT * FROM " + schemaName + "." + tableName);
        result.show();

        assertThat(result.as(AvatarBook.getEncoder()).collectAsList()).hasSameElementsAs(avatarBooks);
    }

}
