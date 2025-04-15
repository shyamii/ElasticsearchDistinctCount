package com.example.demo;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.opencsv.CSVWriter;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregate;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsAggregate;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsBucket;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.RangeQuery;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.json.JsonData;

@Service
public class AggregationService {

	@Autowired
	private ElasticsearchClient esClient;

	@Autowired
	private AggregationResultRepository repository;

	@Value("${elasticsearch.index}")
	private String index;

	@Value("${search.mode}")
	private String searchMode;

	@Value("${output.destination}")
	private String outputDestination;

	@Value("${start.time:#{null}}")
	private String startTime;

	@Value("${end.time:#{null}}")
	private String endTime;

	@Value("${csv.file.path}")
	private String csvFilePath;

	public Query buildQuery() {
		if ("date-range".equalsIgnoreCase(searchMode)) {
			if (startTime == null || endTime == null) {
				throw new IllegalStateException("Date range search requires both start.time and end.time in properties");
			}

			return RangeQuery.of(r -> r.field("join_date").gte(JsonData.of(startTime)).lte(JsonData.of(endTime)))._toQuery();
		}
		return Query.of(q -> q.matchAll(m -> m));
	}

	public List<AggregationResult> runAggregations(List<String> fields, Query query, int size) throws IOException {
		List<AggregationResult> results = new ArrayList<>();

		for (String field : fields) {
			SearchRequest searchRequest = SearchRequest.of(s -> s.index(index).size(0).query(query).aggregations("unique_values", a -> a.terms(t -> t.field(field).size(size))));
			System.out.println(searchRequest.toString());
			SearchResponse<Void> response = esClient.search(searchRequest, Void.class);

			Aggregate agg = response.aggregations().get("unique_values");

			if (agg != null && agg.isSterms()) {
				StringTermsAggregate termsAgg = agg.sterms();
				List<StringTermsBucket> buckets = termsAgg.buckets().array();

				for (StringTermsBucket bucket : buckets) {
					AggregationResult result = new AggregationResult();
					result.setField(field);
					result.setTerm(bucket.key().stringValue());
					result.setCount(bucket.docCount());
					results.add(result);
				}
			}
		}

		return results;
	}

	public void saveResults(List<AggregationResult> results) {
		if ("csv".equalsIgnoreCase(outputDestination)) {
			writeToCsv(results, csvFilePath);
		} else if ("database".equalsIgnoreCase(outputDestination)) {
			repository.saveAll(results);
			System.out.println("Saved " + results.size() + " records to database");
		} else {
			throw new IllegalStateException("Invalid output destination: " + outputDestination);
		}
	}

	public void writeToCsv(List<AggregationResult> results, String csvFilePath) {
		try (CSVWriter writer = new CSVWriter(new FileWriter(csvFilePath))) {
			String[] header = { "Field", "Term", "Count" };
			writer.writeNext(header);

			for (AggregationResult result : results) {
				String[] row = { result.getField(), result.getTerm(), String.valueOf(result.getCount()) };
				writer.writeNext(row);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
