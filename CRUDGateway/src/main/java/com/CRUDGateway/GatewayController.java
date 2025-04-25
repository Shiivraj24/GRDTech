
package com.CRUDGateway;

import com.CRUDGateway.Product;
import com.CRUDGateway.KafkaGatewayService;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/gateway")
public class GatewayController {
    private final KafkaGatewayService gatewayService;

    public GatewayController(KafkaGatewayService gatewayService) {
        this.gatewayService = gatewayService;
    }

    @PostMapping("/create")
    public Mono<String> create(@RequestBody Product product) {
        return gatewayService.sendCreate(product);
    }

    @GetMapping("/get/{id}")
    public Mono<String> get(@PathVariable String id) {
        return gatewayService.sendGet(id);
    }

    @GetMapping("/get-all")
    public Mono<String> getAll() {
        return gatewayService.sendGetAll();
    }

    @PutMapping("/update")
    public Mono<String> update(@RequestBody Product product) {
        return gatewayService.sendUpdate(product);
    }

    @DeleteMapping("/delete/{id}")
    public Mono<String> delete(@PathVariable String id) {
        return gatewayService.sendDelete(id);
    }

    @PostMapping("/bulk/{count}")
    public Mono<String> bulk(@PathVariable int count) {
        return gatewayService.sendBulk(count);
    }
}
