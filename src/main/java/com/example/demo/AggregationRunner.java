package com.example.demo;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;

@Component
public class AggregationRunner implements CommandLineRunner {

	@Autowired
	private AggregationService aggregationService;

	@Autowired
	private ElasticsearchClient esClient;

	@Value("${elasticsearch.index}")
	private String index;

	@Value("${start.time}")
	private String startTime;

	@Value("${end.time}")
	private String endTime;

	@Value("${csv.file.path}")
	private String csvFilePath;

	private static final List<String> FIELDS = Arrays.asList("name", "age", "email");


	@Override
	public void run(String... args) throws Exception {
		// Build the appropriate query based on configuration
		Query query = aggregationService.buildQuery();

		// Get document count
		long count = esClient.count(c -> c.index(index).query(query)).count();

		// Run aggregations
		List<AggregationResult> results = aggregationService.runAggregations(FIELDS, query, (int) Math.min(count, Integer.MAX_VALUE));

		// Save results based on configuration
		aggregationService.saveResults(results);

		System.out.println("Operation completed successfully");
	}
}
