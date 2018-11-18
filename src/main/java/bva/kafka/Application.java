package bva.kafka;

import bva.kafka.lib.ConsumerService;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties
@ComponentScan
public class Application implements CommandLineRunner {
	private static final Logger logger = LogManager.getLogger(Application.class);

	@Autowired
	private ConsumerService consumer;

	@Override
	public void run(String... args) {
		Thread thread = null;
		try {
			thread = new Thread(() -> consumer.start());
			thread.start();

			Runtime.getRuntime().addShutdownHook(new Thread(() -> {
				consumer.stop();
				logger.info("Shutting down...");
			}));
		} catch (Exception ex) {
			logger.error("Error while consumer threads execution", ex);
		} finally {
			if (thread != null && thread.isAlive()) {
				thread.stop();
			}
		}
	}

	public static void main(String[] args) {
		SpringApplication app = new SpringApplication(Application.class);
		app.setShowBanner(false);
		app.run(args);
	}
}
