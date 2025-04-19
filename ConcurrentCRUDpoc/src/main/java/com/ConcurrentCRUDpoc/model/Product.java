package com.ConcurrentCRUDpoc.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.relational.core.mapping.Table;


@Document(collection = "products")
public class Product {
    @Id
    private String id;
    private String name;
    private double quantity;
    private double price;
    
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public double getQuantity() {
		return quantity;
	}
	public void setQuantity(double quntity) {
		this.quantity = quntity;
	}
	public double getPrice() {
		return price;
	}
	public void setPrice(double price) {
		this.price = price;
	}

    // Getters, Setters, Constructors
}
