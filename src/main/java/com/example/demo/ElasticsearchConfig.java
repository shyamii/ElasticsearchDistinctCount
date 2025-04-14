//package com.example.demo;
//
//import co.elastic.clients.elasticsearch.ElasticsearchClient;
//import co.elastic.clients.json.jackson.JacksonJsonpMapper;
//import co.elastic.clients.transport.ElasticsearchTransport;
//import co.elastic.clients.transport.rest_client.RestClientTransport;
//import org.apache.http.HttpHost;
//import org.elasticsearch.client.RestClient;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//
//@Configuration
//public class ElasticsearchConfig {
//
//    @Value("${elasticsearch.host}")
//    private String host;
//
//    @Value("${elasticsearch.port}")
//    private int port;
//
//    @Bean
//    RestClient restClient() {
//        return RestClient.builder(new HttpHost(host, port)).build();
//    }
//
//    @Bean
//    ElasticsearchTransport elasticsearchTransport(RestClient restClient) {
//        return new RestClientTransport(restClient, new JacksonJsonpMapper());
//    }
//
//    @Bean
//    ElasticsearchClient elasticsearchClient(ElasticsearchTransport transport) {
//        return new ElasticsearchClient(transport);
//    }
//}
