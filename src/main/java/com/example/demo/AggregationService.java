package com.example.demo;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.aggregations.*;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.RangeQuery;
import co.elastic.clients.elasticsearch.core.CountRequest;
import co.elastic.clients.elasticsearch.core.CountResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.json.JsonData;
import com.opencsv.CSVWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
public class AggregationService {

	@Autowired
	private ElasticsearchClient esClient;

	@Value("${elasticsearch.index}")
	private String index;

	public long getDocumentCount(String startTime, String endTime) throws IOException {
		Query rangeQuery = RangeQuery
				.of(r -> r.field("join_date").gte(JsonData.of(startTime)).lte(JsonData.of(endTime)))._toQuery();

		System.out.println(rangeQuery.toString());

		CountRequest countRequest = CountRequest.of(c -> c.index(index).query(rangeQuery));

		CountResponse response = esClient.count(countRequest);

		System.out.println("Total count :" + response.count());
		return response.count();
	}

	public List<AggregationResult> runAggregations(List<String> fields, String startTime, String endTime, int size)
			throws IOException {
		List<AggregationResult> results = new ArrayList<>();

		for (String field : fields) {
			SearchRequest searchRequest = SearchRequest.of(s -> s.index(index).size(0)
					.query(q -> q.range(
							RangeQuery.of(r -> r.field("join_date").gte(JsonData.of(startTime)).lte(JsonData.of(endTime)))))
					.aggregations("unique_values", a -> a.terms(t -> t.field(field).size(size))));

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

	public void writeToCsv(List<AggregationResult> results, String csvFilePath) throws IOException {
		try (CSVWriter writer = new CSVWriter(new FileWriter(csvFilePath))) {
			String[] header = { "Field", "Term", "Count" };
			writer.writeNext(header);

			for (AggregationResult result : results) {
				String[] row = { result.getField(), result.getTerm(), String.valueOf(result.getCount()) };
				writer.writeNext(row);
			}
		}
	}
}
