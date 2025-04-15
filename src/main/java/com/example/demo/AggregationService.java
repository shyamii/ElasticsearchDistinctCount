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

	@Value("${elasticsearch.max-bucket-size:65000}")
	private int maxBucketSize;

	@Value("${elasticsearch.enable-pagination:true}")
	private boolean enablePagination;

	public List<AggregationResult> runAggregations(List<String> fields, Query query, long totalDocs) throws IOException {
		List<AggregationResult> results = new ArrayList<>();

		for (String field : fields) {
			boolean needsPagination = totalDocs > maxBucketSize;
			if (needsPagination) {
				results.addAll(getPaginatedAggregation(field, query, maxBucketSize));
			} else {
				results.addAll(getSimpleAggregation(field, query, (int) totalDocs));
			}
		}
		return results;
	}

	private List<AggregationResult> getSimpleAggregation(String field, Query query, int size) throws IOException {
		SearchRequest request = SearchRequest.of(s -> s.index(index).size(0).query(query).aggregations("unique_values", a -> a.terms(t -> t.field(field).size(size))));
		return processAggregationResponse(field, esClient.search(request, Void.class));
	}

	private List<AggregationResult> processAggregationResponse(String field, SearchResponse<Void> response) {
		List<AggregationResult> fieldResults = new ArrayList<>();
		Aggregate agg = response.aggregations().get("unique_values");

		if (agg != null && agg.isSterms()) {
			StringTermsAggregate termsAgg = agg.sterms();
			List<StringTermsBucket> buckets = termsAgg.buckets().array();

			for (StringTermsBucket bucket : buckets) {
				AggregationResult result = new AggregationResult();
				result.setField(field);
				result.setTerm(bucket.key().stringValue());
				result.setCount(bucket.docCount());
				fieldResults.add(result);
			}
		}

		return fieldResults;
	}

	private List<AggregationResult> getPaginatedAggregation(String field, Query query, int pageSize) throws IOException {
		List<AggregationResult> results = new ArrayList<>();
		Map<String, FieldValue> afterKey = null;

		do {
			SearchRequest request = buildCompositeSearchRequest(field, query, pageSize, afterKey);
			SearchResponse<Void> response = esClient.search(request, Void.class);
			CompositeAggregate compositeAggregate = response.aggregations().get("terms_agg").composite();

			results.addAll(processCompositeAggregation(field, compositeAggregate));
			afterKey = compositeAggregate.afterKey();

		} while (afterKey != null && !afterKey.isEmpty());

		return results;
	}

	private SearchRequest buildCompositeSearchRequest(String field, Query query, int size, Map<String, FieldValue> afterKey) {
		// Build a composite source with the field as the key
		Map<String, CompositeAggregationSource> source = new HashMap<>();
		source.put(field, CompositeAggregationSource.of(b -> b.terms(t -> t.field(field))));

		List<Map<String, CompositeAggregationSource>> sources = new ArrayList<>();
		sources.add(source);

		return SearchRequest.of(s -> s.index(index).size(0).query(query).aggregations("terms_agg", a -> a.composite(c -> {
			c.sources(sources);
			c.size(size);
			if (afterKey != null && !afterKey.isEmpty()) {
				c.after(afterKey);
			}
			return c;
		})));
	}

	private List<AggregationResult> processCompositeAggregation(String field, CompositeAggregate compositeAggregate) {
		return compositeAggregate.buckets().array().stream().map(bucket -> {
			AggregationResult result = new AggregationResult();
			result.setField(field);
			result.setTerm(bucket.key().get("terms_source").toString());
			result.setCount(bucket.docCount());
			return result;
		}).collect(Collectors.toList());
	}

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

	public void saveToDatabase(List<AggregationResult> results) {
        String sql = "INSERT INTO aggregation_results (field, term, count) VALUES (?, ?, ?)";
        
        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                AggregationResult result = results.get(i);
                ps.setString(1, result.getField());
                ps.setString(2, result.getTerm());
                ps.setLong(3, result.getCount());
            }

            @Override
            public int getBatchSize() {
                return results.size();
            }
        });
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
