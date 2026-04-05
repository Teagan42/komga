package org.gotson.komga.infrastructure.datasource

import org.flywaydb.core.Flyway
import org.gotson.komga.infrastructure.configuration.KomgaProperties
import org.springframework.beans.factory.InitializingBean
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component
import javax.sql.DataSource

@Component
class FlywaySecondaryMigrationInitializer(
  @Qualifier("tasksDataSourceRW")
  private val tasksDataSource: DataSource,
  private val komgaProperties: KomgaProperties,
) : InitializingBean {
  // by default Spring Boot will perform migration only on the @Primary datasource
  override fun afterPropertiesSet() {
    val vendor = if (komgaProperties.tasksDb.isPostgresql()) "postgresql" else "sqlite"
    Flyway
      .configure()
      .locations("classpath:tasks/migration/$vendor")
      .dataSource(tasksDataSource)
      .load()
      .apply {
        migrate()
      }
  }
}
