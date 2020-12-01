package bio.terra.cda.app.configuration;

import bio.terra.cda.app.StartupInitializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import java.util.Properties;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@Configuration
@EnableConfigurationProperties
@EnableTransactionManagement
@ConfigurationProperties(prefix = "cda")
public class ApplicationConfiguration {

  // Configurable properties

  @Bean("objectMapper")
  public ObjectMapper objectMapper() {
    return new ObjectMapper()
        .registerModule(new ParameterNamesModule())
        .registerModule(new Jdk8Module())
        .registerModule(new JavaTimeModule());
  }

  // This is a "magic bean": It supplies a method that Spring calls after the application is setup,
  // but before the port is opened for business. That lets us do database migration and stairway
  // initialization on a system that is otherwise fully configured. The rule of thumb is that all
  // bean initialization should avoid database access. If there is additional database work to be
  // done, it should happen inside this method.
  @Bean
  public SmartInitializingSingleton postSetupInitialization(ApplicationContext applicationContext) {
    return () -> {
      StartupInitializer.initialize(applicationContext);
    };
  }
}
