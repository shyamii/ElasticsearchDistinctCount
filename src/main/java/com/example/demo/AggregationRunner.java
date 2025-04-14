package com.example.demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Component
public class AggregationRunner implements CommandLineRunner {

    @Autowired
    private AggregationService aggregationService;

    @Value("${start.time}")
    private String startTime;

    @Value("${end.time}")
    private String endTime;

    @Value("${csv.file.path}")
    private String csvFilePath;

    private static final List<String> FIELDS = Arrays.asList(
            "name",
            "age",
            "email"
    );

    @Override
    public void run(String... args) throws Exception {
        long count = aggregationService.getDocumentCount(startTime, endTime);
        List<AggregationResult> results = aggregationService.runAggregations(
                FIELDS, 
                startTime, 
                endTime, 
                (int) Math.min(count, Integer.MAX_VALUE)
        );
        System.out.println(results.toString());
        aggregationService.writeToCsv(results, csvFilePath);
        System.out.println("CSV file generated successfully at: " + csvFilePath);
    }
}
