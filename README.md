# Hubuum - A flexible asset management system

*Hubuum (𒄷𒁍𒌝) in Sumerian translates as “axle” or “wheel assembly”*[^1].

Hubuum is a REST service that provides a shared interface for your resources.

## Concept

Most content management systems (CMDBs) are strongly opinionated. They provide fairly strict models with user interfaces designed for those models and all their data. This design may not be ideal for every use case.

CMDBs also like to be authoritative for any data they possess. The problem with this in this day and age, very often other highly dedicated systems are the authoritative sources of lots and lots data, and these sources typically come with very domain specific scraping tools.

With Hubuum you can...

- define your own data structures and their relationships.
- populate your data structures as JSON, and enforce validation when required.
- draw in data from any source into any object, structuring it as your organization requires.
- look up and search within these JSON structures in an efficient way, via a REST interface.
- offload the work of searching and indexing to Hubuum, and focus on your data.
- control permissions to one object set in one application instead of having to do it in multiple places.
- know that REST is your interface, no matter what data you are accessing.

Once upon a time your data was everywhere, each in its own silo. Now you can have it all in one place, and access it all through a single REST interface.

## Design

Hubuum is designed around the idea of classes and objects, where the classes are user-defined and optionally constrained by a JSON schema[^2]. Objects are instances of these classes and these classes only. If the class defines a schema, and the class requires validation against the schema, you are guaranteed that objects within that class conform to said schema.

[^1]: Hubuum is probably a loanword from Akkadian.
[^2]: [JSON schema](https://json-schema.org) is a powerful tool for validating the structure of JSON data. It allows you to define the expected format of your data, including required fields, data types, and constraints on values.
