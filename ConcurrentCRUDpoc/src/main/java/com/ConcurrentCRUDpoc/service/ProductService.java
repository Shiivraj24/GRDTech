package com.ConcurrentCRUDpoc.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.ConcurrentCRUDpoc.model.Product;
import com.ConcurrentCRUDpoc.repository.ProductRepository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Service
public class ProductService {
    private final ProductRepository repo;
    
    private static final Logger logger = LoggerFactory.getLogger(ProductService.class);


    public ProductService(ProductRepository repo) {
        this.repo = repo;
    }

    public Mono<Product> create(Product product) {
    	  logger.info("Creating product: {}", product);
          return Mono.fromCallable(() -> product)
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(repo::save);
    }

    public Flux<Product> getAll() {
        return repo.findAll();
    }

    public Mono<Product> getById(String id) {
        return repo.findById(id);
    }

    public Mono<Product> update(String id, Product updated) {
        return repo.findById(id)
            .flatMap(existing -> {
                existing.setName(updated.getName());
                existing.setQuantity(updated.getQuantity());
                existing.setPrice(updated.getPrice());
                return repo.save(existing);
            });
    }

    public Mono<Void> delete(String id) {
        return repo.deleteById(id);
    }
}
