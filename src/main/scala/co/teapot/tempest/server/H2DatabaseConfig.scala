package co.teapot.tempest.server

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

class H2DatabaseConfig extends DatabaseConfig {
  lazy val createConnectionSource: HikariDataSource = {
    val config = new HikariConfig()
    config.setJdbcUrl("jdbc:h2:mem:test")
    new HikariDataSource(config)
  }
}
