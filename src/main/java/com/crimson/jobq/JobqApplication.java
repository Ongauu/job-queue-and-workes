package com.crimson.jobq;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class JobqApplication {

	public static void main(String[] args) {
		SpringApplication.run(JobqApplication.class, args);
	}

}
