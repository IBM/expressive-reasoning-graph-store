Ingestion Module is responsible for storing RDF data into property graph format. It implements the core RDF4J repository APIs that enable handling and ingesting the input data through various RDF4J supported input mechanisms. Currently, JanusGraph is used as property graph backend for data storage. Specifically, ingestion module offers following features:

#### Schema Translation and loading:
Schema translation is responsible for transforming input triple data into the property graph model. It builds schema of property graph which decides nodes structure, edges and their properties and translates triple appropriately to node property or edge during loading.

#### Metadata for Supporting Query Translation: 
For supporting execution of SPARQL queries on property graph, metadata is stored as sub-graph which provides schema translation related information to Query Translation module

#### Parallel Ingestion and Index Utilization: 
For making loading process faster we have developed parallel approach by dividing loading task across threads.

We make use of the different indexing mechanisms provided by Janus Graph for faster query execution. It enables to reduce query execution
time and to execute complex text keyword based queries.

#### Forward Chaining enabled Ingestion: 
For executing reasoning queries more efficiently current ingestion pipeline supports enabling forward chaining during data ingestion.  Currently, we support RDFS reasoning and few OWL constructs which includes *owl:inverseOf, owl:symmetric, owl:TransitiveProperty*. Currently, we do not have full support for forward chaining in parallel loading, e.g., we do not support parallel ingestion for forward chaining for *owl:TransitiveProperty*.  In future, we will add this feature with other OWL axioms.
