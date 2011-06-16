Mongo Insert Products Demo
=============================

INTRODUCTION
   This demo shows how to add objects to a Mongo collection, 
   either by passing its attributes as Java objects, or by passing valid JSon. It lets the user enter
   products information and add it to a products collection

HOW TO DEMO:
  This demo has no prerequisites aside having a mongodb running locally.
  1. Run the MongoFunctionalTestDriver, or deploy this demo an a Mule Container. 
  	a. Insert products passing attributes: Run the testInsertProduct test or alternatively hit 
  	http://localhost:9090/mongo-demo-insert-product, passing as query params the product attributes.
  	Example: 
  	b. Insert products passing its JSon: Run the testInsertProductJSon test or alternatively hit 
  	http://localhost:9090/mongo-demo-insert-product-json, passing a Json that describes the product.
  	Example: 
 	
HOW IT WORKS:
  The demo inserts product documents into the "products" collection of the "test" database, either by 
  passing its JSon or by specifying its fields. This demo implicitly creates the products collection
  if it does not already exist.  
  
