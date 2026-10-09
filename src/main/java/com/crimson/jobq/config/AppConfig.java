package com.crimson.jobq.config;

import com.crimson.jobq.ratelimit.*;
import com.crimson.jobq.worker.RetryBackoff;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

@Configuration
public class AppConfig implements WebMvcConfigurer {
    private final JobQueueProperties props;

    public AppConfig(JobQueueProperties props){
        this.props = props;
    }

    @Bean
    public  RateLimiter apiRateLimiter(){
        var a = props.api();
        return switch (a.algorithm()){
            case "token-bucket" -> new TokenBucketRateLimiter(a.capacity(),a.refillPerSecond(), System::nanoTime);
            case "sliding-window" -> new SlidingWindowLogRateLimiter(a.capacity(), Duration.ofSeconds(a.windowSeconds()), System::nanoTime);
            default -> throw new IllegalArgumentException("Unknown jobq.api.algorithm: " + a.algorithm());
        };
    }

    @Bean
    public OutboundThrottle outboundThrottle(){
        var w = props.worker();
        return new OutboundThrottle(w.outboundBurst(), w.outboundRatePerSecond(), System::nanoTime);
    }

    @Bean
    public RetryBackoff retryBackoff(){
        var r = props.retry();
        return new RetryBackoff(r.baseDelayMs(), r.maxDelayMs(), () -> ThreadLocalRandom.current().nextDouble());
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry){
        registry.addInterceptor(new RateLimiterInterceptor(apiRateLimiter())).addPathPatterns("/api/jobs");
    }
}
