package nip.edu.jobprep;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class JobPrepApplication {

    public static void main(String[] args) {
        SpringApplication.run(JobPrepApplication.class, args);
    }

}
