package com.example.demo;

public class AggregationResult {
	private String field;
	private String term;
	private long count;

	public String getField() {
		return field;
	}

	public void setField(String field) {
		this.field = field;
	}

	public String getTerm() {
		return term;
	}

	public void setTerm(String term) {
		this.term = term;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	@Override
	public String toString() {
		return "AggregationResult [field=" + field + ", term=" + term + ", count=" + count + "]";
	}

}
