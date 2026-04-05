package org.gotson.komga.infrastructure.datasource

import com.zaxxer.hikari.HikariDataSource
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.gotson.komga.infrastructure.configuration.KomgaProperties
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.springframework.jdbc.core.JdbcTemplate
import java.nio.file.Files
import java.sql.Timestamp
import java.time.LocalDateTime

class SqliteToPostgresMigrationTest {
  private val komgaProperties = mockk<KomgaProperties>(relaxed = true)
  private val targetMainDataSource = mockk<HikariDataSource>(relaxed = true)
  private val targetTasksDataSource = mockk<HikariDataSource>(relaxed = true)

  private val migration =
    SqliteToPostgresMigration(
      targetMainDataSource = targetMainDataSource,
      targetTasksDataSource = targetTasksDataSource,
      komgaProperties = komgaProperties,
    )

  @Nested
  inner class ConvertValue {
    @Test
    fun `given boolean type with integer 1 then returns true`() {
      assertThat(migration.convertValue(1, "boolean")).isEqualTo(true)
    }

    @Test
    fun `given boolean type with integer 0 then returns false`() {
      assertThat(migration.convertValue(0, "boolean")).isEqualTo(false)
    }

    @Test
    fun `given boolean type with long 1 then returns true`() {
      assertThat(migration.convertValue(1L, "boolean")).isEqualTo(true)
    }

    @Test
    fun `given boolean type with long 0 then returns false`() {
      assertThat(migration.convertValue(0L, "boolean")).isEqualTo(false)
    }

    @Test
    fun `given boolean type with string 0 then returns false`() {
      assertThat(migration.convertValue("0", "boolean")).isEqualTo(false)
    }

    @Test
    fun `given boolean type with string 1 then returns true`() {
      assertThat(migration.convertValue("1", "boolean")).isEqualTo(true)
    }

    @Test
    fun `given boolean type with string false then returns false`() {
      assertThat(migration.convertValue("false", "boolean")).isEqualTo(false)
    }

    @Test
    fun `given boolean type with string true then returns true`() {
      assertThat(migration.convertValue("TRUE", "boolean")).isEqualTo(true)
    }

    @Test
    fun `given boolean type with actual Boolean true then returns true`() {
      assertThat(migration.convertValue(true, "boolean")).isEqualTo(true)
    }

    @Test
    fun `given boolean type with actual Boolean false then returns false`() {
      assertThat(migration.convertValue(false, "boolean")).isEqualTo(false)
    }

    @Test
    fun `given datetime type with valid string then returns Timestamp`() {
      val result = migration.convertValue("2023-01-15 10:30:00.123", "datetime")
      assertThat(result).isInstanceOf(Timestamp::class.java)
      val ts = result as Timestamp
      assertThat(ts.toLocalDateTime()).isEqualTo(LocalDateTime.of(2023, 1, 15, 10, 30, 0, 123_000_000))
    }

    @Test
    fun `given datetime type with ISO string including T then returns Timestamp`() {
      val result = migration.convertValue("2023-01-15T10:30:00.000", "datetime")
      assertThat(result).isInstanceOf(Timestamp::class.java)
    }

    @Test
    fun `given datetime type with ISO string ending in Z then returns Timestamp`() {
      val result = migration.convertValue("2023-01-15T10:30:00.000Z", "datetime")
      assertThat(result).isInstanceOf(Timestamp::class.java)
    }

    @Test
    fun `given datetime type with already a Timestamp then returns same Timestamp`() {
      val ts = Timestamp.valueOf("2023-01-15 10:30:00")
      assertThat(migration.convertValue(ts, "datetime")).isSameAs(ts)
    }

    @Test
    fun `given datetime type with unparseable string then returns the raw string`() {
      val result = migration.convertValue("not-a-date", "datetime")
      assertThat(result).isEqualTo("not-a-date")
    }

    @Test
    fun `given blob type with byte array then returns same byte array`() {
      val bytes = byteArrayOf(1, 2, 3)
      assertThat(migration.convertValue(bytes, "blob")).isSameAs(bytes)
    }

    @Test
    fun `given blob type with string then converts to byte array`() {
      val result = migration.convertValue("hello", "blob")
      assertThat(result).isInstanceOf(ByteArray::class.java)
      assertThat(result as ByteArray).isEqualTo("hello".toByteArray())
    }

    @Test
    fun `given varchar type with string then returns unchanged`() {
      assertThat(migration.convertValue("hello", "varchar")).isEqualTo("hello")
    }

    @Test
    fun `given integer type with int then returns unchanged`() {
      assertThat(migration.convertValue(42, "integer")).isEqualTo(42)
    }

    @Test
    fun `given any type with null value then returns null`() {
      assertThat(migration.convertValue(null, "boolean")).isNull()
      assertThat(migration.convertValue(null, "datetime")).isNull()
      assertThat(migration.convertValue(null, "blob")).isNull()
      assertThat(migration.convertValue(null, "varchar")).isNull()
    }
  }

  @Nested
  inner class MigrateIfNeeded {
    @Test
    fun `given non-postgresql database then migration is skipped`() {
      val dbProps = KomgaProperties.Database().apply { type = KomgaProperties.DatabaseType.SQLITE }
      every { komgaProperties.database } returns dbProps

      migration.run(mockk(relaxed = true))

      // No interaction with datasources
      verify(exactly = 0) { targetMainDataSource.connection }
    }

    @Test
    fun `given postgresql with blank file then migration is skipped`() {
      val dbProps =
        KomgaProperties.Database().apply {
          type = KomgaProperties.DatabaseType.POSTGRESQL
          file = ""
        }
      every { komgaProperties.database } returns dbProps
      every { komgaProperties.tasksDb } returns KomgaProperties.Database().apply {
        type = KomgaProperties.DatabaseType.POSTGRESQL
        file = ""
      }

      migration.run(mockk(relaxed = true))

      verify(exactly = 0) { targetMainDataSource.connection }
    }

    @Test
    fun `given postgresql with non-existent sqlite file then migration is skipped`() {
      val nonExistent = System.getProperty("java.io.tmpdir") + "/nonexistent-${System.currentTimeMillis()}.sqlite"
      val dbProps =
        KomgaProperties.Database().apply {
          type = KomgaProperties.DatabaseType.POSTGRESQL
          file = nonExistent
        }
      every { komgaProperties.database } returns dbProps
      every { komgaProperties.tasksDb } returns KomgaProperties.Database().apply {
        type = KomgaProperties.DatabaseType.POSTGRESQL
        file = ""
      }

      migration.run(mockk(relaxed = true))

      verify(exactly = 0) { targetMainDataSource.connection }
    }
  }

  @Nested
  inner class IsMigrationAlreadyDone {
    private lateinit var sqliteDataSource: HikariDataSource
    private lateinit var pgTemplate: JdbcTemplate

    @BeforeEach
    fun setup() {
      sqliteDataSource =
        migration.createSqliteDataSource("file:test_already_done?mode=memory&cache=shared") as HikariDataSource
      pgTemplate = JdbcTemplate(sqliteDataSource)
      pgTemplate.execute("""CREATE TABLE IF NOT EXISTS "SERVER_SETTINGS" ("KEY" varchar PRIMARY KEY, "VALUE" varchar)""")
    }

    @AfterEach
    fun teardown() {
      pgTemplate.execute("""DROP TABLE IF EXISTS "SERVER_SETTINGS"""")
      sqliteDataSource.close()
    }

    @Test
    fun `given empty server settings then migration is not done`() {
      assertThat(migration.isMigrationAlreadyDone(pgTemplate)).isFalse()
    }

    @Test
    fun `given migration done key present then migration is done`() {
      pgTemplate.update(
        """INSERT INTO "SERVER_SETTINGS" ("KEY", "VALUE") VALUES (?, ?)""",
        SqliteToPostgresMigration.MIGRATION_DONE_KEY,
        "true",
      )
      assertThat(migration.isMigrationAlreadyDone(pgTemplate)).isTrue()
    }

    @Test
    fun `given other key present but not migration key then migration is not done`() {
      pgTemplate.update(
        """INSERT INTO "SERVER_SETTINGS" ("KEY", "VALUE") VALUES (?, ?)""",
        "some-other-key",
        "some-value",
      )
      assertThat(migration.isMigrationAlreadyDone(pgTemplate)).isFalse()
    }
  }

  @Nested
  inner class GetColumnTypes {
    private lateinit var sqliteDataSource: HikariDataSource
    private lateinit var sqliteTemplate: JdbcTemplate

    @BeforeEach
    fun setup() {
      sqliteDataSource = migration.createSqliteDataSource("file:test_coltypes?mode=memory&cache=shared") as HikariDataSource
      sqliteTemplate = JdbcTemplate(sqliteDataSource)
      sqliteTemplate.execute(
        """
        CREATE TABLE IF NOT EXISTS TEST_TABLE (
          ID varchar NOT NULL PRIMARY KEY,
          NAME varchar NOT NULL,
          IS_ACTIVE boolean NOT NULL DEFAULT 0,
          CREATED_DATE datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
          DATA blob NULL,
          SIZE int8 NOT NULL DEFAULT 0
        )
        """.trimIndent(),
      )
    }

    @AfterEach
    fun teardown() {
      sqliteTemplate.execute("DROP TABLE IF EXISTS TEST_TABLE")
      sqliteDataSource.close()
    }

    @Test
    fun `given existing table then returns column type map`() {
      val types = migration.getColumnTypes(sqliteTemplate, "TEST_TABLE")
      assertThat(types).containsKeys("ID", "NAME", "IS_ACTIVE", "CREATED_DATE", "DATA", "SIZE")
      assertThat(types["IS_ACTIVE"]).isEqualTo("boolean")
      assertThat(types["CREATED_DATE"]).isEqualTo("datetime")
      assertThat(types["DATA"]).isEqualTo("blob")
    }

    @Test
    fun `given non-existent table then returns empty map`() {
      val types = migration.getColumnTypes(sqliteTemplate, "NONEXISTENT_TABLE")
      assertThat(types).isEmpty()
    }
  }

  @Nested
  inner class MigrateTable {
    private lateinit var sourceSqliteDataSource: HikariDataSource
    private lateinit var targetSqliteDataSource: HikariDataSource
    private lateinit var sourceTemplate: JdbcTemplate
    private lateinit var targetTemplate: JdbcTemplate

    @BeforeEach
    fun setup() {
      sourceSqliteDataSource = migration.createSqliteDataSource("file:src_migrate?mode=memory&cache=shared") as HikariDataSource
      targetSqliteDataSource = migration.createSqliteDataSource("file:tgt_migrate?mode=memory&cache=shared") as HikariDataSource
      sourceTemplate = JdbcTemplate(sourceSqliteDataSource)
      targetTemplate = JdbcTemplate(targetSqliteDataSource)

      sourceTemplate.execute(
        """
        CREATE TABLE IF NOT EXISTS LIBRARY (
          ID varchar NOT NULL PRIMARY KEY,
          NAME varchar NOT NULL,
          ROOT varchar NOT NULL,
          ACTIVE boolean NOT NULL DEFAULT 1,
          CREATED_DATE datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
          THUMBNAIL blob NULL
        )
        """.trimIndent(),
      )
      targetTemplate.execute(
        """
        CREATE TABLE IF NOT EXISTS LIBRARY (
          ID varchar NOT NULL PRIMARY KEY,
          NAME varchar NOT NULL,
          ROOT varchar NOT NULL,
          ACTIVE boolean NOT NULL DEFAULT 1,
          CREATED_DATE datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
          THUMBNAIL blob NULL
        )
        """.trimIndent(),
      )
    }

    @AfterEach
    fun teardown() {
      sourceTemplate.execute("DROP TABLE IF EXISTS LIBRARY")
      targetTemplate.execute("DROP TABLE IF EXISTS LIBRARY")
      sourceSqliteDataSource.close()
      targetSqliteDataSource.close()
    }

    @Test
    fun `given rows in source table then migrates all rows`() {
      sourceTemplate.update(
        """INSERT INTO LIBRARY (ID, NAME, ROOT, ACTIVE, CREATED_DATE, THUMBNAIL) VALUES (?, ?, ?, ?, ?, ?)""",
        "lib1", "My Library", "file:///books", 1, "2023-01-01 00:00:00.000", null,
      )
      sourceTemplate.update(
        """INSERT INTO LIBRARY (ID, NAME, ROOT, ACTIVE, CREATED_DATE, THUMBNAIL) VALUES (?, ?, ?, ?, ?, ?)""",
        "lib2", "Comics", "file:///comics", 0, "2023-06-15 12:00:00.000", byteArrayOf(1, 2, 3),
      )

      val columnTypes = migration.getColumnTypes(sourceTemplate, "LIBRARY")
      val count = migration.migrateTable(sourceTemplate, targetTemplate, "LIBRARY", columnTypes)

      assertThat(count).isEqualTo(2)
      val rows = targetTemplate.queryForList("""SELECT * FROM LIBRARY ORDER BY ID""")
      assertThat(rows).hasSize(2)
      assertThat(rows[0]["ID"]).isEqualTo("lib1")
      assertThat(rows[0]["NAME"]).isEqualTo("My Library")
      assertThat(rows[1]["ID"]).isEqualTo("lib2")
    }

    @Test
    fun `given empty source table then returns zero count`() {
      val columnTypes = migration.getColumnTypes(sourceTemplate, "LIBRARY")
      val count = migration.migrateTable(sourceTemplate, targetTemplate, "LIBRARY", columnTypes)
      assertThat(count).isEqualTo(0)
    }

    @Test
    fun `given duplicate rows then ON CONFLICT DO NOTHING prevents duplicates`() {
      sourceTemplate.update(
        """INSERT INTO LIBRARY (ID, NAME, ROOT, ACTIVE, CREATED_DATE) VALUES (?, ?, ?, ?, ?)""",
        "lib1", "My Library", "file:///books", 1, "2023-01-01 00:00:00.000",
      )
      targetTemplate.update(
        """INSERT INTO LIBRARY (ID, NAME, ROOT, ACTIVE, CREATED_DATE) VALUES (?, ?, ?, ?, ?)""",
        "lib1", "Existing", "file:///existing", 1, "2022-01-01 00:00:00.000",
      )

      val columnTypes = migration.getColumnTypes(sourceTemplate, "LIBRARY")
      migration.migrateTable(sourceTemplate, targetTemplate, "LIBRARY", columnTypes)

      val rows = targetTemplate.queryForList("""SELECT * FROM LIBRARY""")
      assertThat(rows).hasSize(1)
      // Original row preserved, not overwritten
      assertThat(rows[0]["NAME"]).isEqualTo("Existing")
    }
  }

  @Nested
  inner class FullMigrationFlow {
    private lateinit var sqliteFile: java.nio.file.Path

    @BeforeEach
    fun setup() {
      sqliteFile = Files.createTempFile("komga_test_migration_", ".sqlite")
    }

    @AfterEach
    fun teardown() {
      Files.deleteIfExists(sqliteFile)
    }

    @Test
    fun `given existing sqlite file with data and target with schema then migrates data and marks done`() {
      // Populate the source SQLite file with some data
      val sourcDs = migration.createSqliteDataSource(sqliteFile.toString()) as HikariDataSource
      val srcTemplate = JdbcTemplate(sourcDs)
      try {
        srcTemplate.execute(
          """
          CREATE TABLE IF NOT EXISTS "SERVER_SETTINGS" ("KEY" varchar PRIMARY KEY, "VALUE" varchar)
          """.trimIndent(),
        )
        srcTemplate.update(
          """INSERT INTO "SERVER_SETTINGS" ("KEY", "VALUE") VALUES (?, ?)""",
          "some-setting", "some-value",
        )
      } finally {
        sourcDs.close()
      }

      // Target "PostgreSQL" is a second in-memory SQLite (simulating the schema already created by Flyway)
      val targetDs = migration.createSqliteDataSource("file:full_mig_tgt?mode=memory&cache=shared") as HikariDataSource
      val tgtTemplate = JdbcTemplate(targetDs)
      try {
        tgtTemplate.execute(
          """
          CREATE TABLE IF NOT EXISTS "SERVER_SETTINGS" ("KEY" varchar PRIMARY KEY, "VALUE" varchar)
          """.trimIndent(),
        )

        migration.migrateIfNeeded(
          sqliteFile = sqliteFile.toString(),
          targetDataSource = targetDs,
          tables = listOf("SERVER_SETTINGS"),
          dbDescription = "test",
        )

        // Data migrated
        val rows = tgtTemplate.queryForList("""SELECT * FROM "SERVER_SETTINGS" ORDER BY "KEY"""")
        // Should have both the migrated setting and the migration-done marker
        assertThat(rows.map { it["KEY"] }).contains("some-setting", SqliteToPostgresMigration.MIGRATION_DONE_KEY)

        // Verify migration done flag
        assertThat(migration.isMigrationAlreadyDone(tgtTemplate)).isTrue()
      } finally {
        targetDs.close()
      }
    }

    @Test
    fun `given migration already done then second call does not re-migrate`() {
      // Populate source SQLite file
      val sourcDs = migration.createSqliteDataSource(sqliteFile.toString()) as HikariDataSource
      val srcTemplate = JdbcTemplate(sourcDs)
      try {
        srcTemplate.execute(
          """CREATE TABLE IF NOT EXISTS "SERVER_SETTINGS" ("KEY" varchar PRIMARY KEY, "VALUE" varchar)""",
        )
        srcTemplate.update(
          """INSERT INTO "SERVER_SETTINGS" ("KEY", "VALUE") VALUES (?, ?)""", "key1", "val1",
        )
      } finally {
        sourcDs.close()
      }

      val targetDs = migration.createSqliteDataSource("file:full_mig_idempotent?mode=memory&cache=shared") as HikariDataSource
      val tgtTemplate = JdbcTemplate(targetDs)
      try {
        tgtTemplate.execute(
          """CREATE TABLE IF NOT EXISTS "SERVER_SETTINGS" ("KEY" varchar PRIMARY KEY, "VALUE" varchar)""",
        )
        // Pre-set the migration done flag
        tgtTemplate.update(
          """INSERT INTO "SERVER_SETTINGS" ("KEY", "VALUE") VALUES (?, ?)""",
          SqliteToPostgresMigration.MIGRATION_DONE_KEY, "true",
        )

        migration.migrateIfNeeded(
          sqliteFile = sqliteFile.toString(),
          targetDataSource = targetDs,
          tables = listOf("SERVER_SETTINGS"),
          dbDescription = "test",
        )

        // "key1" should NOT have been migrated because migration was already done
        val rows = tgtTemplate.queryForList("""SELECT * FROM "SERVER_SETTINGS"""")
        assertThat(rows.map { it["KEY"] }).doesNotContain("key1")
      } finally {
        targetDs.close()
      }
    }

    @Test
    fun `given migration fails mid-way then exception is propagated`() {
      // Source file exists but contains no schema (simulates unexpected DB state)
      val sourcDs = migration.createSqliteDataSource(sqliteFile.toString()) as HikariDataSource
      sourcDs.close()

      val brokenTargetDs = mockk<HikariDataSource>(relaxed = true)
      every { brokenTargetDs.connection } throws RuntimeException("Connection refused")

      assertThatThrownBy {
        migration.migrateIfNeeded(
          sqliteFile = sqliteFile.toString(),
          targetDataSource = brokenTargetDs,
          tables = listOf("SERVER_SETTINGS"),
          dbDescription = "test",
        )
      }.isInstanceOf(RuntimeException::class.java)
    }
  }

  @Nested
  inner class MigrateAllTables {
    private lateinit var sourceSqliteDataSource: HikariDataSource
    private lateinit var targetSqliteDataSource: HikariDataSource
    private lateinit var sourceTemplate: JdbcTemplate
    private lateinit var targetTemplate: JdbcTemplate

    @BeforeEach
    fun setup() {
      sourceSqliteDataSource = migration.createSqliteDataSource("file:all_src?mode=memory&cache=shared") as HikariDataSource
      targetSqliteDataSource = migration.createSqliteDataSource("file:all_tgt?mode=memory&cache=shared") as HikariDataSource
      sourceTemplate = JdbcTemplate(sourceSqliteDataSource)
      targetTemplate = JdbcTemplate(targetSqliteDataSource)
    }

    @AfterEach
    fun teardown() {
      sourceSqliteDataSource.close()
      targetSqliteDataSource.close()
    }

    @Test
    fun `given multiple tables then migrates all of them`() {
      for (tbl in listOf("T1", "T2")) {
        sourceTemplate.execute("""CREATE TABLE IF NOT EXISTS "$tbl" ("ID" varchar PRIMARY KEY, "VAL" varchar)""")
        targetTemplate.execute("""CREATE TABLE IF NOT EXISTS "$tbl" ("ID" varchar PRIMARY KEY, "VAL" varchar)""")
        sourceTemplate.update("""INSERT INTO "$tbl" ("ID", "VAL") VALUES (?, ?)""", "id-$tbl", "value-$tbl")
      }

      migration.migrateAllTables(sourceTemplate, targetTemplate, listOf("T1", "T2"))

      for (tbl in listOf("T1", "T2")) {
        val rows = targetTemplate.queryForList("""SELECT * FROM "$tbl"""")
        assertThat(rows).hasSize(1)
        assertThat(rows[0]["ID"]).isEqualTo("id-$tbl")
      }
    }

    @Test
    fun `given table missing in source then skips it without error`() {
      targetTemplate.execute("""CREATE TABLE IF NOT EXISTS "PRESENT" ("ID" varchar PRIMARY KEY)""")
      // "MISSING" table does not exist in source
      migration.migrateAllTables(sourceTemplate, targetTemplate, listOf("MISSING"))
      // No exception thrown
    }
  }
}
