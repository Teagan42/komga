package org.gotson.komga.infrastructure.configuration

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.gotson.komga.domain.model.ConfigurationException
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path

class ConfigurationCheckerTest {
  @Nested
  inner class PostgresqlSkipsFilesystemCheck {
    @Test
    fun `given postgresql database type then filesystem check is skipped without error`() {
      val props = buildProperties { db ->
        db.type = KomgaProperties.DatabaseType.POSTGRESQL
        db.url = "jdbc:postgresql://localhost:5432/komga"
        db.file = ""
        db.checkLocalFilesystem = true
      }
      val checker = ConfigurationChecker(props)
      assertThatCode { checker.checkDatabasesPath() }.doesNotThrowAnyException()
    }

    @Test
    fun `given postgresql database type and check-local-filesystem true then no exception thrown`() {
      val props = buildProperties { db ->
        db.type = KomgaProperties.DatabaseType.POSTGRESQL
        db.url = "jdbc:postgresql://localhost:5432/komga"
        db.file = ""
        db.checkLocalFilesystem = true
      }
      val checker = ConfigurationChecker(props)
      assertThatCode { checker.checkDatabasesPath() }.doesNotThrowAnyException()
    }

    @Test
    fun `given postgresql for both main and tasks databases then both filesystem checks are skipped`() {
      val props = KomgaProperties()
      props.database.apply {
        type = KomgaProperties.DatabaseType.POSTGRESQL
        url = "jdbc:postgresql://localhost:5432/komga"
        file = ""
        checkLocalFilesystem = true
      }
      props.tasksDb.apply {
        type = KomgaProperties.DatabaseType.POSTGRESQL
        url = "jdbc:postgresql://localhost:5432/komga"
        file = ""
        checkLocalFilesystem = true
      }
      val checker = ConfigurationChecker(props)
      assertThatCode { checker.checkDatabasesPath() }.doesNotThrowAnyException()
    }
  }

  @Nested
  inner class SqliteFilesystemCheckEnabled {
    @Test
    fun `given sqlite with check enabled and valid local path then no exception thrown`(
      @TempDir tempDir: Path,
    ) {
      val dbFile = tempDir.resolve("test.sqlite").toString()
      val props = buildProperties { db ->
        db.type = KomgaProperties.DatabaseType.SQLITE
        db.file = dbFile
        db.checkLocalFilesystem = true
      }
      val checker = ConfigurationChecker(props)
      assertThatCode { checker.checkDatabasesPath() }.doesNotThrowAnyException()
    }

    @Test
    fun `given sqlite with check disabled then no exception thrown`() {
      val props = buildProperties { db ->
        db.type = KomgaProperties.DatabaseType.SQLITE
        db.file = "/some/path/database.sqlite"
        db.checkLocalFilesystem = false
      }
      val checker = ConfigurationChecker(props)
      assertThatCode { checker.checkDatabasesPath() }.doesNotThrowAnyException()
    }

    @Test
    fun `given sqlite with in-memory path then check returns gracefully`() {
      val props = buildProperties { db ->
        db.type = KomgaProperties.DatabaseType.SQLITE
        db.file = "file:database?mode=memory"
        db.checkLocalFilesystem = true
      }
      val checker = ConfigurationChecker(props)
      assertThatCode { checker.checkDatabasesPath() }.doesNotThrowAnyException()
    }
  }

  @Nested
  inner class DatabaseTypeIsPostgresql {
    @Test
    fun `given POSTGRESQL type then isPostgresql returns true`() {
      val db = KomgaProperties.Database()
      db.type = KomgaProperties.DatabaseType.POSTGRESQL
      assertThat(db.isPostgresql()).isTrue()
    }

    @Test
    fun `given SQLITE type then isPostgresql returns false`() {
      val db = KomgaProperties.Database()
      db.type = KomgaProperties.DatabaseType.SQLITE
      assertThat(db.isPostgresql()).isFalse()
    }

    @Test
    fun `given default type then isPostgresql returns false and SQLITE is the default`() {
      val db = KomgaProperties.Database()
      assertThat(db.isPostgresql()).isFalse()
      assertThat(db.type).isEqualTo(KomgaProperties.DatabaseType.SQLITE)
    }
  }

  private fun buildProperties(configure: (KomgaProperties.Database) -> Unit): KomgaProperties {
    val props = KomgaProperties()
    configure(props.database)
    configure(props.tasksDb)
    return props
  }
}
