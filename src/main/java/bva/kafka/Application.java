package bva.kafka;

import bva.kafka.lib.ConsumerService;
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

	@Autowired
	private ConsumerService consumer;

	@Override
	public void run(String... args) {
		try {
			Thread thread = new Thread(() -> consumer.start());
			thread.start();

			Runtime.getRuntime().addShutdownHook(new Thread(() -> {
				consumer.stop();
				System.out.println("Shutting down...");
			}));
		} catch (Exception ex) {
			System.out.println("error while consumer threads execution" + ex.getMessage());
		}
	}

	public static void main(String[] args) {
		SpringApplication app = new SpringApplication(Application.class);
		app.setShowBanner(false);
		app.run(args);
	}
}
