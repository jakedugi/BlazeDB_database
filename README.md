# BlazeDB — Lightweight SQL Query Engine in Java

BlazeDB is a lightweight, from-scratch SQL query engine implemented in Java for my Advanced Database Systems course project.  
It demonstrates efficient relational join processing, selection disambiguation, and logical optimization strategies.

This module outlines a fundamental query processor that emphasizes efficiency in join evaluation. By restructuring how selection and join conditions are interpreted and applied, it minimizes intermediate result sizes and accelerates execution.

## Logical Separation of Join Semantics

A key architectural decision in this processor is the early extraction of join conditions embedded within SQL `WHERE` clauses. This is applied during join execution rather than post hoc reducing unnecessary computation.

### Methodology

- **Condition Disambiguation**:  
  SQL queries often conflate selection and join predicates within a single `WHERE` clause. This processor parses those predicates to distinguish:
    - *Selection conditions* — predicates referring to a single relation.
    - *Join conditions* — predicates referencing attributes from two distinct relations.

- **Join-Aware Operator Insertion**:  
  Once join conditions are isolated, they are embedded directly into the join operator itself. This diverges from the naive cross-product followed by filtering and instead evaluates predicates in situ during tuple comparisons. By doing this the system avoids generating full Cartesian products when they are unnecessary.

- **Left-Deep Join Tree Execution**:  
  Joins are executed in a left-deep tree formation, consistent with the order specified in the `FROM` clause. Each join operator incrementally builds the result set, evaluating associated predicates at each level. This layout prioritizes early pruning of non-qualifying tuples.

### Performance Implications

- **Lower Intermediate Cardinality**:  
  Because non-matching tuples are filtered immediately within the join, fewer intermediate tuples are created, passed, or stored.

- **Reduced Computational Load**:  
  Eliminating infeasible tuple combinations early reduces the total number of operations and the memory footprint, particularly in multi-table joins.

## Optimization Techniques

The system incorporates a set of well-established relational query optimizations such as, projection pruning, early join predicate evaluation, predicate combination, predicate pushdown, Tuple-Nested-Loop Join Optimization. Each of these improves query optimization efficiency through reducing either the cardinality or the width of intermediate results.

### 1. Projection Pruning

**Overview**:  
Columns not required for downstream computation are excluded as early as possible. This avoids carrying excess data through the operator pipeline.

**Justification**:
- *Relational Validity*: If a column is not referenced in projections or conditions, its omission has no bearing on the query result.
- *Compliance with SELECT Clause*: The optimizer guarantees that only columns explicitly requested by the query are included in the output.

**Impact**:
- *Narrower Tuples*: Intermediate datasets occupy less memory.
- *I/O Reduction*: Narrow data structures accelerate subsequent operations, including sorting and hashing.

### 2. Early Join Predicate Evaluation

**Overview**:  
Join conditions are extracted early and evaluated during tuple pairing, as opposed to after a full cross-product. This optimization mirrors the semantics of standard relational algebra but restructures execution for improved performance.

**Justification**:
- *Semantically Sound*: The result of evaluating a join predicate after tuple generation is logically identical to its evaluation during tuple formation. Thus, correctness is preserved.
- *Operational Efficiency*: Applying conditions during pairing avoids the creation of invalid tuple combinations entirely, reducing downstream computation.

**Impact**:
- *Minimized Tuple Creation*: Non-matching combinations are rejected early.
- *Resource Savings*: Less data is processed through subsequent stages of the plan.

### 3. Predicate Combination

**Overview**:  
Multiple selection predicates on the same relation are merged into a unified condition, which is then evaluated in a single operation.

**Justification**:
- *Logical Equivalence*: A composite predicate formed via conjunction (logical AND) yields the same truth values as evaluating the components independently.
- *Operational Streamlining*: Consolidating conditions reduces redundant filtering steps.

**Impact**:
- *Faster Evaluation*: Conditions are processed in a single pass.
- *Reduced Operator Count*: Fewer plan nodes translate into a more compact execution plan.


### 4. Predicate Pushdown

**Overview**:  
Selection predicates are applied immediately following the table scan rather than further downstream. This principle is known as predicate pushdown.

**Justification**:
- *Logical Independence*: As long as a condition references only a single relation and does not rely on the output of a join or projection, it can be applied immediately.
- *Transformation Safety*: Moving a selection closer to the scan does not alter query semantics.

**Impact**:
- *Lower Row Volume*: Fewer rows flow into joins, projections, or aggregations.
- *Efficiency Gains*: Earlier filtering reduces total processing time and memory usage.


### 5. Tuple-Nested-Loop Join Optimization

**Overview**:  
The join operator employs a tuple-nested-loop strategy, iterating over each tuple from the outer relation and comparing it against tuples in the inner relation. While simple, this strategy is substantially improved when paired with early predicate evaluation.

**Justification**:
- *Exhaustive Pairing with Filtering*: All valid tuple pairs are considered, and those failing the join predicate are discarded without further materialization.
- *Algorithmic Soundness*: The correctness of this method is grounded in the systematic consideration of every potential pairing under the join condition.

**Impact**:
- *Controlled Intermediate Growth*: Invalid tuples are never stored.
- *Lower Memory Usage*: By avoiding materialization of irrelevant tuples, the join maintains a smaller working set.
- *Faster Execution*: The system avoids redundant computations inherent in post-join filtering.

## Limitations

- **Lack of Cost-Based Optimization**:  
The current system does not implement a cost-based optimizer. Join order and execution strategy are determined syntactically, typically reflecting the order in the `FROM` clause rather than being informed by relation sizes, selectivity estimates, or index availability. As a result, the chosen execution plan may be suboptimal, particularly in scenarios involving skewed data distributions or highly selective predicates. Incorporating cardinality estimation and cost modeling would enable more intelligent plan generation.
