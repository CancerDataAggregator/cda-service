package bio.terra.cda.app.configuration;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class ApiResourceConfig implements WebMvcConfigurer {
  @Override
  public void addResourceHandlers(ResourceHandlerRegistry registry) {
    registry
        .addResourceHandler("/api/swagger-webjar/**")
        .addResourceLocations("classpath:/META-INF/resources/webjars/swagger-ui/4.14.0/");
    registry.addResourceHandler("/api/**").addResourceLocations("classpath:/api/");
  }
}
