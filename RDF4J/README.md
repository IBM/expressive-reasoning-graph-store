This project contains submodules for exposing different [RDF4J](https://rdf4j.org) APIs. RDF4J repository interface is implemented on top of an underlying Apache Tinkerpop3 compliant Property Graph database along with all the required interfaces to support ingestion and querying of RDF data. It provides four different types of interfaces to the user.
1. **RDF4J console**: A command-line application that can be used for accessing and modifying the data.
2. **RDF4J workbench**: It provides different graphical interfaces for interacting with the underlying store using SPARQL and other endpoints.
3. **RDF4J server**: Set of REST APIs which allow the user to interact with the RDF store using HTTP protocol.
4. **HTTP repository**: It provides a proxy for remote RDF4J repository, such that users could use Java APIs to interact with the repository just like a local repository.
