package com.ConcurrentCRUDpoc.repository;


import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;

import com.ConcurrentCRUDpoc.model.Product;

import reactor.core.publisher.Flux;

public interface ProductRepository extends ReactiveMongoRepository<Product, String> {
    Flux<Product> findByNameContaining(String name); // Example custom query
}