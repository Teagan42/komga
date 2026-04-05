package org.gotson.komga.infrastructure.datasource

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.github.oshai.kotlinlogging.KotlinLogging
import org.gotson.komga.infrastructure.configuration.KomgaProperties
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Component
import java.sql.Timestamp
import javax.sql.DataSource
import kotlin.io.path.Path
import kotlin.io.path.exists

private val logger = KotlinLogging.logger {}

/**
 * Migrates data from a SQLite database to PostgreSQL when:
 * 1. The database type is POSTGRESQL
 * 2. `komga.database.file` points to an existing SQLite database file (main DB migration source)
 * 3. `komga.tasks-db.file` points to an existing SQLite tasks database file (tasks DB migration source)
 *
 * The migration runs once and records completion in SERVER_SETTINGS.
 * On subsequent startups, the migration is skipped if the flag is already set.
 */
@Component
class SqliteToPostgresMigration(
  @Qualifier("sqliteDataSourceRW") private val targetMainDataSource: DataSource,
  @Qualifier("tasksDataSourceRW") private val targetTasksDataSource: DataSource,
  private val komgaProperties: KomgaProperties,
) : ApplicationRunner {
  companion object {
    internal const val MIGRATION_DONE_KEY = "sqlite-migration-completed"

    // Table insertion order must respect foreign key dependencies
    internal val MAIN_TABLES =
      listOf(
        "LIBRARY",
        "USER",
        "USER_LIBRARY_SHARING",
        "SERIES",
        "SERIES_METADATA",
        "SERIES_METADATA_GENRE",
        "SERIES_METADATA_TAG",
        "SERIES_METADATA_SHARING",
        "SERIES_METADATA_LINK",
        "SERIES_METADATA_ALTERNATE_TITLE",
        "BOOK",
        "MEDIA",
        "MEDIA_PAGE",
        "MEDIA_FILE",
        "BOOK_METADATA",
        "BOOK_METADATA_AUTHOR",
        "BOOK_METADATA_TAG",
        "BOOK_METADATA_LINK",
        "BOOK_METADATA_AGGREGATION",
        "BOOK_METADATA_AGGREGATION_AUTHOR",
        "BOOK_METADATA_AGGREGATION_TAG",
        "THUMBNAIL_BOOK",
        "THUMBNAIL_SERIES",
        "COLLECTION",
        "COLLECTION_SERIES",
        "THUMBNAIL_COLLECTION",
        "READLIST",
        "READLIST_BOOK",
        "THUMBNAIL_READLIST",
        "READ_PROGRESS",
        "READ_PROGRESS_SERIES",
        "SIDECAR",
        "AUTHENTICATION_ACTIVITY",
        "PAGE_HASH",
        "PAGE_HASH_THUMBNAIL",
        "HISTORICAL_EVENT",
        "HISTORICAL_EVENT_PROPERTIES",
        "USER_SHARING",
        "ANNOUNCEMENTS_READ",
        "LIBRARY_EXCLUSIONS",
        "SERVER_SETTINGS",
        "USER_API_KEY",
        "SYNC_POINT",
        "SYNC_POINT_BOOK",
        "SYNC_POINT_BOOK_REMOVED_SYNCED",
        "SYNC_POINT_READLIST",
        "SYNC_POINT_READLIST_BOOK",
        "SYNC_POINT_READLIST_REMOVED_SYNCED",
        "USER_ROLE",
        "CLIENT_SETTINGS_GLOBAL",
        "CLIENT_SETTINGS_USER",
      )

    internal val TASKS_TABLES = listOf("TASK")
  }

  override fun run(args: ApplicationArguments) {
    if (!komgaProperties.database.isPostgresql()) return

    migrateIfNeeded(
      sqliteFile = komgaProperties.database.file,
      targetDataSource = targetMainDataSource,
      tables = MAIN_TABLES,
      dbDescription = "main",
    )
    migrateIfNeeded(
      sqliteFile = komgaProperties.tasksDb.file,
      targetDataSource = targetTasksDataSource,
      tables = TASKS_TABLES,
      dbDescription = "tasks",
    )
  }

  internal fun migrateIfNeeded(
    sqliteFile: String,
    targetDataSource: DataSource,
    tables: List<String>,
    dbDescription: String,
  ) {
    if (sqliteFile.isBlank()) return
    val sqlitePath = Path(sqliteFile)
    if (!sqlitePath.exists()) return

    val pgTemplate = JdbcTemplate(targetDataSource)

    if (isMigrationAlreadyDone(pgTemplate)) {
      logger.info { "SQLite to PostgreSQL migration already completed for $dbDescription database, skipping." }
      return
    }

    logger.info { "Starting SQLite to PostgreSQL migration for $dbDescription database from: $sqliteFile" }

    val sqliteDataSource = createSqliteDataSource(sqliteFile)
    try {
      val sqliteTemplate = JdbcTemplate(sqliteDataSource)
      migrateAllTables(sqliteTemplate, pgTemplate, tables)
      if (tables.contains("SERVER_SETTINGS")) {
        markMigrationDone(pgTemplate)
      }
      logger.info { "SQLite to PostgreSQL migration for $dbDescription database completed successfully." }
    } catch (e: Exception) {
      logger.error(e) { "SQLite to PostgreSQL migration for $dbDescription database failed." }
      throw e
    } finally {
      (sqliteDataSource as? HikariDataSource)?.close()
    }
  }

  internal fun isMigrationAlreadyDone(pgTemplate: JdbcTemplate): Boolean {
    val count =
      pgTemplate.queryForObject(
        """SELECT COUNT(*) FROM "SERVER_SETTINGS" WHERE "KEY" = ?""",
        Int::class.java,
        MIGRATION_DONE_KEY,
      ) ?: 0
    return count > 0
  }

  private fun markMigrationDone(pgTemplate: JdbcTemplate) {
    pgTemplate.update(
      """INSERT INTO "SERVER_SETTINGS" ("KEY", "VALUE") VALUES (?, ?) ON CONFLICT ("KEY") DO UPDATE SET "VALUE" = EXCLUDED."VALUE"""",
      MIGRATION_DONE_KEY,
      "true",
    )
  }

  internal fun migrateAllTables(
    sourceTemplate: JdbcTemplate,
    targetTemplate: JdbcTemplate,
    tables: List<String>,
  ) {
    for (table in tables) {
      val columnTypes = getColumnTypes(sourceTemplate, table)
      if (columnTypes.isEmpty()) {
        logger.warn { "Table $table not found in source SQLite database, skipping." }
        continue
      }
      val count = migrateTable(sourceTemplate, targetTemplate, table, columnTypes)
      logger.debug { "Migrated $count rows from table $table" }
    }
  }

  /**
   * Reads column name → declared type from SQLite's PRAGMA table_info.
   * The declared type is used to decide how to convert values for PostgreSQL.
   */
  internal fun getColumnTypes(
    template: JdbcTemplate,
    table: String,
  ): Map<String, String> =
    template
      .query("""PRAGMA table_info("$table")""") { rs, _ ->
        rs.getString("name") to rs.getString("type").lowercase()
      }
      .toMap()

  internal fun migrateTable(
    sourceTemplate: JdbcTemplate,
    targetTemplate: JdbcTemplate,
    table: String,
    columnTypes: Map<String, String>,
  ): Int {
    val columns = columnTypes.keys.toList()
    val quotedCols = columns.joinToString(", ") { """"$it"""" }
    val placeholders = columns.joinToString(", ") { "?" }
    val insertSql = """INSERT INTO "$table" ($quotedCols) VALUES ($placeholders) ON CONFLICT DO NOTHING"""

    var count = 0
    sourceTemplate.query("""SELECT * FROM "$table"""") { rs ->
      val values =
        columns.map { col ->
          convertValue(rs.getObject(col), columnTypes[col] ?: "")
        }
        .toTypedArray()
      targetTemplate.update(insertSql, *values)
      count++
    }
    return count
  }

  /**
   * Converts a SQLite column value to the appropriate PostgreSQL type.
   *
   * - boolean columns in SQLite are stored as integers (0/1); convert to Boolean
   * - datetime columns in SQLite are stored as strings; convert to Timestamp
   * - blob columns in SQLite are byte arrays; passed through unchanged (PostgreSQL bytea)
   * - all other types are passed through as-is
   */
  internal fun convertValue(
    rawValue: Any?,
    declaredType: String,
  ): Any? {
    if (rawValue == null) return null
    return when {
      declaredType.contains("bool") ->
        when (rawValue) {
          is Boolean -> rawValue
          is Number -> rawValue.toInt() != 0
          is String -> rawValue != "0" && rawValue.lowercase() != "false"
          else -> rawValue
        }

      declaredType.contains("datetime") || declaredType.contains("timestamp") ->
        when (rawValue) {
          is Timestamp -> rawValue
          is String ->
            try {
              Timestamp.valueOf(rawValue.replace("T", " ").trimEnd('Z'))
            } catch (_: Exception) {
              rawValue
            }
          else -> rawValue
        }

      declaredType.contains("blob") || declaredType.contains("bytea") ->
        when (rawValue) {
          is ByteArray -> rawValue
          is String -> rawValue.toByteArray()
          else -> rawValue
        }

      else -> rawValue
    }
  }

  internal fun createSqliteDataSource(file: String): DataSource =
    HikariDataSource(
      HikariConfig().apply {
        jdbcUrl = "jdbc:sqlite:$file"
        driverClassName = "org.sqlite.JDBC"
        maximumPoolSize = 1
        poolName = "SqliteMigrationSource"
      },
    )
}
