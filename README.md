<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->

# Implementing Diamond Hardened Joins in Apache Calcite

Instructions

1. Build and run the QueryRunner.java in org.example.diamondhardenedjoins package.
    1. Place the further cleaned JOB dataset in the path "C:\JOB_dataset"
    2. Create a folder for query results as "C:\query_results"

2. After loading the in-memory database and building the schema, you will get a prompt as
   ``` sql> ```

    - Command format:
        - ```\s <query> [--std-out 0|1] [--omit-exec 0|1] [--out file]```
        - ```\f <filename> [--std-out 0|1] [--omit-exec 0|1] [--out file]```
            - --std-out: whether to display output in console
            - --omit-exec: whether to omit the in-built execution engine in Apache Calcite for physical plan evaluation
            - --out: output file name of the sql query processing results(if not provided, a default name will be generated)

Apache Calcite: https://github.com/apache/calcite

Join Order Benchmark Dataset: https://github.com/VimukthiPahasaraGodage/join_order_benchmark
