
package com.CRUDKafka.CRUDKafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

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
        return Mono.just(product)
            .filter(p -> p.getId() != null)
            .switchIfEmpty(Mono.error(new IllegalArgumentException("Product ID is required")))
            .subscribeOn(Schedulers.boundedElastic())
            .flatMap(repo::save);
    }

    public Flux<Product> getAll() {
        logger.info("Retrieving all products");
        return repo.findAll().subscribeOn(Schedulers.boundedElastic());
    }

    public Mono<Product> getById(String id) {
        logger.info("Retrieving product by ID: {}", id);
        return repo.findById(id)
            .switchIfEmpty(Mono.error(new RuntimeException("Product not found: " + id)))
            .subscribeOn(Schedulers.boundedElastic());
    }

    public Mono<Product> update(String id, Product updated) {
        logger.info("Updating product ID: {}", id);
        return repo.findById(id)
            .switchIfEmpty(Mono.error(new RuntimeException("Product not found: " + id)))
            .flatMap(existing -> {
                existing.setName(updated.getName());
                existing.setQuantity(updated.getQuantity());
                existing.setPrice(updated.getPrice());
                return repo.save(existing);
            })
            .subscribeOn(Schedulers.boundedElastic());
    }

    public Mono<Void> delete(String id) {
        logger.info("Deleting product ID: {}", id);
        return repo.findById(id)
            .switchIfEmpty(Mono.error(new RuntimeException("Product not found: " + id)))
            .flatMap(existing -> repo.deleteById(id))
            .subscribeOn(Schedulers.boundedElastic());
    }
}
