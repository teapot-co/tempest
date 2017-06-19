package co.teapot.tempest.server

import com.zaxxer.hikari.HikariDataSource

trait DatabaseConfig {
  def createConnectionSource(): HikariDataSource
}
