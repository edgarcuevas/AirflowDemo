package music.SpotifySearch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SpotifySearchApplication {

	public static void main(String[] args) {
		SpotifySayHello.main(args);
		SpringApplication.run(SpotifySearchApplication.class, args);
	}

}
