This project contains different modules for exposing query related services. Query Engine consists of three main modules.
1. **Query Expansion**: This module expands the input SPARQL query as a union of multiple conjunctive queries to find some of the implicit solutions to the query. It uses another open-source library [Quetzal](https://github.com/Quetzal-RDF/quetzal) for query expansion that can handle any SPARQL 1.0 query and OWL-QL knowledge bases.
2. **Query Preprocessing**: SPARQL queries are declarative in nature. This implies that the user can enter the triple patterns in any order without worrying about the order in which these patterns will be used during query execution. As part of query preprocessing, we reorder the query patterns in SPARQL declarative queries such that it can be directly translated using the SPARQL-Gremlin translator. 
3. **SPARQL to Gremlin Translation**:  This module acts as a middle-ware between the  query  endpoint  and  the  underlying  property  graph  database  such  that  the  target system is viewed as an RDF store by the end-user. We use the meta graph constructed during the data ingestion phase to differentiate between edge traversals andproperty traversals at the time of query translation.

**Limitations**: The following constructs are not supported.
1. String functions: STRLEN, SUBSTR, UCASE, LCASE, STRBEFORE, STRAFTER, ENCODE_FOR_URI,CONCAT, REPLACE.
2. Dataset Definition: FROM, FROM NAMED and GRAPH.
3. Functions on Dates and Times: NOW, YEAR, MONTH, DAY, HOURS, MINUTES, SECONDS, TIMEZONE, TZ.
4. Hash Functions: MD5, SHA1, SHA256, SHA384, SHA512. 
Most of these limitations are there because there are no equivalent gremlin operations for these clauses.
