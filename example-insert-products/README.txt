Mongo Insert Products Demo
=============================

INTRODUCTION
   This demo shows how to add objects to a Mongo collection, 
   by passing its attributes as Java objects. It lets the user enter
   products information and add it to a products collection

HOW TO DEMO:
  This demo has no prerequisites aside having a mongodb running locally.
  1. Run the MongoFunctionalTestDriver, or deploy this demo an a Mule Container. 
  	a. Insert products passing attributes: Run the testInsertProduct test or alternatively hit 
  	http://localhost:9090/mongo-demo-insert-product, passing as query params the product attributes.
  	Example: http://localhost:9090/mongo-demo-insert-product?sku=AF1596&price=110&description=Chair&available=true
 	
HOW IT WORKS:
  The demo inserts product documents into the "products" collection of the "test" database, by specifying its fields. This demo implicitly creates the products collection
  if it does not already exist.  
  
