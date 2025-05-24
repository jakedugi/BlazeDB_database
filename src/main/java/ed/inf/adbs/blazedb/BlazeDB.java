package ed.inf.adbs.blazedb;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.concurrent.atomic.AtomicInteger;


import ed.inf.adbs.blazedb.operator.*;
import ed.inf.adbs.blazedb.result.AggregateResult;
import ed.inf.adbs.blazedb.result.InitResult;
import ed.inf.adbs.blazedb.result.ProjectResult;
import ed.inf.adbs.blazedb.util.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import ed.inf.adbs.blazedb.operator.SumOperator;
import ed.inf.adbs.blazedb.operator.Operator;
import java.util.ArrayList;
import java.util.List;



/**

 * BlazeDB - An agile in-memory database engine for executing SQL queries.
 * This refactored implementation parses SQL SELECT queries, constructs an operator tree
 * (using a left-deep join strategy and pushdown techniques), and applies projection, sorting,
 * aggregation and duplicate removal as needed. All configuration (database directory, query file,
 * and result file) is provided via the command-line interface.
 */
public class BlazeDB {

	public static void main(String[] args) {

		if (args.length != 3) {
			System.err.println("Usage: BlazeDB database_dir input_file output_file");
			return;
		}

		final String databaseDir;
		databaseDir = args[0];
		String inputFile = args[1];
		String outputFile = args[2];

		// Start the query execution plan using the provided file paths.
		runQueryPlan(inputFile, outputFile, databaseDir);
	}


	/**
	 * Runs the complete query execution plan.
	 * <p>
	 * This method reads the SQL query from the given query file, builds the operator tree,
	 * applies projection, aggregation, duplicate removal and sorting as needed, and finally
	 * writes out and validates the result.
	 *
	 * @param inputFile   Path to the SQL query file.
	 * @param outputFile  Path to the file where results will be saved.
	 * @param databaseDir Path to the database directory.
	 */

	public static void runQueryPlan(String inputFile, String outputFile, String databaseDir) {
		try (FileReader fileReader = new FileReader(inputFile)) {
			// Build the operator tree and complete column mapping.
			List<String> tableList = new ArrayList<>();
			InitResult initOutcome = initializeQueryPlan(fileReader, tableList, databaseDir);
			Operator currentOperator = initOutcome.getRootOp();
			Map<String, Integer> columnMapping = initOutcome.getSchemaMap();

			// Parse the SQL query.
			Statement statement = CCJSqlParserUtil.parse(new FileReader(inputFile));
			Select selectStatement = (Select) statement;
			PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();

			// Collect required columns.
			String primaryTable = tableList.get(0);
			Set<String> neededColumns = gatherNeededColumns(plainSelect, primaryTable, databaseDir, columnMapping);
			/**System.out.println("Required columns: " + neededColumns);
			 *
			 */

			// Process aggregations.
			AggregateResult aggOutcome = extractAggregationFunctions(plainSelect);
			boolean containsAggregation = aggOutcome.containsAggregation();
			List<Expression> aggregateExprList = aggOutcome.getSumFuncs();
			Map<Expression, String> literalAggregateMap = aggOutcome.getLiteralSumAliases();

			// Non-aggregation branch: project early if no aggregation.
			if (!containsAggregation) {
				ProjectResult projOutcome = applyNonAggProjection(plainSelect, currentOperator, columnMapping, neededColumns);
				currentOperator = projOutcome.fetchRootOp();
				columnMapping = projOutcome.fetchSchemaMap();
			}
			// Aggregation branch: build pruned mapping and wrap with SumOperator.
			if (containsAggregation) {
				Map<String, Integer> refinedMapping = processAggregationBranch(plainSelect, neededColumns, columnMapping, aggregateExprList, literalAggregateMap);
				// Retrieve GROUP BY expressions.
				List<Expression> groupExprList = new ArrayList<>();
				if (plainSelect.getGroupBy() != null && plainSelect.getGroupBy().getGroupByExpressions() != null) {
					groupExprList.addAll(plainSelect.getGroupBy().getGroupByExpressions());
				}
				currentOperator = new SumOperator(currentOperator, groupExprList, aggregateExprList, refinedMapping);
				if (currentOperator instanceof SumOperator) {
					columnMapping = ((SumOperator) currentOperator).getColumnMapping();
				}
			}

			// For non-aggregation queries, handle duplicate elimination.
			if (!containsAggregation) {
				currentOperator = applyDuplicateRemoval(plainSelect, currentOperator);
			}

			// Process subsequent projection.
			ProjectResult finalProjection = applyFinalProjection(plainSelect, currentOperator, columnMapping, containsAggregation, tableList);
			currentOperator = finalProjection.fetchRootOp();
			columnMapping = finalProjection.fetchSchemaMap();
			/** System.out.println("Column Mapping after projections: " + columnMapping);
			 *
			 */

			// Final schema mapping for ORDER BY.
			if (containsAggregation) {
				columnMapping = reconstructColumnMapping(plainSelect, columnMapping);
				System.out.println("Final columnMapping for ORDER BY: " + columnMapping);
			}

			// Order By processing.
			currentOperator = applySortOperator(plainSelect, currentOperator, columnMapping);

			// Execution.
			executePlanAndValidateOutput(currentOperator, outputFile);
		} catch (Exception e) {
			System.err.println("Error during query plan execution: " + e.getMessage());
		}
	}

	/**
	 * Refines the column mapping for an aggregation query by retaining only the needed columns and
	 * adding unique aliases for literal aggregate expressions.
	 *
	 * @param plainSelect          The parsed SQL query.
	 * @param neededColumns        Set of columns required by the query.
	 * @param columnMapping        The complete mapping of fully qualified column names to indices.
	 * @param aggregateExpressions List of aggregate (SUM) expressions.
	 * @param literalAggregateMap  Mapping of literal aggregate expressions to unique alias strings.
	 * @return A refined mapping containing only the required columns and aggregate aliases.
	 */
	private static Map<String, Integer> processAggregationBranch(PlainSelect plainSelect, Set<String> neededColumns,
																 Map<String, Integer> columnMapping,
																 List<Expression> aggregateExpressions,
																 Map<Expression, String> literalAggregateMap) {
		Map<String, Integer> refinedMapping = new LinkedHashMap<>();
		// Use forEach for needed columns
		neededColumns.forEach(col -> {
			Integer origIdx = columnMapping.get(col);
			if (origIdx != null) {
				refinedMapping.put(col, origIdx);
			} else {
				System.err.println("Warning: Column " + col + " missing in column mapping.");
			}
		});
		// Process literal aggregates using a simple for-loop over the entrySet
		int baseIndex = refinedMapping.size();
		for (Map.Entry<Expression, String> entry : literalAggregateMap.entrySet()) {
			String aggExprStr = entry.getKey().toString().trim();
			if (!refinedMapping.containsKey(aggExprStr)) {
				refinedMapping.put(aggExprStr, baseIndex++);
			}
		}
		return refinedMapping;
	}

	/**
	 * •	Constructs a new schema mapping that includes only the columns or expressions specified in the SELECT clause, with indices starting at 0.
	 * •	For each SELECT item, the method uses its alias if one is provided, or falls back to the column/expression name.
	 * •	It then performs a case-insensitive search in the original schema mapping for a matching entry before adding it to the new mapping.
	 * •
	 * •	@param plainSelect   The parsed plain select query.
	 * •	@param schemaMapping The original full schema mapping with column names and their indexes.
	 * •	@return A new schema mapping containing exclusively the SELECT items, re-indexed from 0.
	 */
	private static Map<String, Integer> reconstructColumnMapping(PlainSelect plainSelect, Map<String, Integer> columnMapping) {
		// Collect SELECT items into a list of keys.
		List<String> keys = plainSelect.getSelectItems().stream()
				.map(item -> item.toString().trim())
				.collect(Collectors.toList());
		Map<String, Integer> newMapping = new LinkedHashMap<>();
		int index = 0;
		for (String key : keys) {
			// Try direct lookup; if missing, use a case-insensitive search.
			Optional<Integer> idx = Optional.ofNullable(columnMapping.get(key));
			if (!idx.isPresent()) {
				idx = columnMapping.entrySet().stream()
						.filter(e -> e.getKey().equalsIgnoreCase(key))
						.map(Map.Entry::getValue)
						.findFirst();
			}
			if (!idx.isPresent()) {
				System.err.println("Error: Column " + key + " not found in column mapping.");
			}
			newMapping.put(key, index++);
		}
		return newMapping;
	}

	/**
	 * Handles projection in queries without aggregation by extracting the necessary columns
	 * from the operator tree and adjusting the schema mapping to match.
	 *
	 * @param plainSelect   The parsed SELECT statement.
	 * @param opRoot        The current top-level operator in the operator tree.
	 * @param columnMapping The current schema mapping of column names to their indices.
	 * @param neededColumns The set of columns explicitly required by the query.
	 * @return A {@link ProjectResult} with the updated root operator and schema mapping.
	 */

	private static ProjectResult applyNonAggProjection(PlainSelect plainSelect,
													   Operator opRoot,
													   Map<String, Integer> columnMapping,
													   Set<String> neededColumns) {
		List<String> orderedColumns = new ArrayList<>();

		// Handle SELECT items
		if (plainSelect.getSelectItems().size() == 1 && plainSelect.getSelectItems().get(0).toString().trim().equals("*")) {
			orderedColumns.addAll(columnMapping.keySet());
		} else {
			plainSelect.getSelectItems().forEach(item -> {
				String col = item.toString().trim();
				if (neededColumns.contains(col)) {
					orderedColumns.add(col);
					System.out.println("Including needed column: " + col);
				} else {
					System.err.println("Warning: " + col + " missing in needed columns.");
				}
			});
		}

		// Handle ORDER BY elements using forEach
		if (plainSelect.getOrderByElements() != null) {
			plainSelect.getOrderByElements().forEach(orderBy -> {
				String orderCol = orderBy.toString().trim();
				if (!orderedColumns.contains(orderCol)) {
					orderedColumns.add(orderCol);
					System.out.println("Appending ORDER BY column: " + orderCol);
				}
			});
		}

		// Build pruned mapping using streams
		Map<String, Integer> prunedMapping = orderedColumns.stream()
				.filter(columnMapping::containsKey)
				.collect(Collectors.toMap(col -> col, col -> columnMapping.get(col), (a, b) -> a, LinkedHashMap::new));

		opRoot = new ProjectOperator(opRoot, orderedColumns.toArray(new String[0]), prunedMapping);
		return new ProjectResult(opRoot, prunedMapping);
	}


	/**
	 * Handles the wrapping of the operator tree with a DuplicateEliminationOperator when required.
	 * This method checks whether the SQL query includes DISTINCT or GROUP BY clauses and applies
	 * duplicate elimination accordingly to ensure that the result set adheres to the query's specifications.
	 *
	 * @param plainSelect The parsed SQL SELECT statement.
	 * @param opRoot      The current root operator in the operator tree prior to duplicate elimination.
	 * @return The updated root operator after applying duplicate elimination if required. If duplicate
	 * elimination is not needed, returns the original root operator unchanged.
	 */
	private static Operator applyDuplicateRemoval(PlainSelect plainSelect, Operator opRoot) {
		boolean distinctPresent = (plainSelect.getDistinct() != null);
		boolean groupByPresent = plainSelect.getGroupBy() != null &&
				plainSelect.getGroupBy().getGroupByExpressions() != null &&
				!plainSelect.getGroupBy().getGroupByExpressions().isEmpty();

		if (distinctPresent || groupByPresent) {
			opRoot = new DuplicateEliminationOperator(opRoot);
		}

		return opRoot;
	}

	/**
	 * Extracts all columns needed for executing the query. This includes columns referenced in
	 * SELECT, WHERE, GROUP BY, and ORDER BY. If the query uses SELECT *, it pulls the full schema
	 * from setupOperators to get all columns.
	 *
	 * @param plainSelect  Parsed SELECT statement.
	 * @param defaultTable Table name to fall back on when a column isn't qualified with a table alias.
	 * @param inputFile    Path to the SQL file being processed.
	 * @return Set of fully qualified column names required to executePlan the query.
	 * @throws Exception on parse errors or schema lookup failures.
	 */
	public static Set<String> gatherNeededColumns(PlainSelect plainSelect, String defaultTable,
												  String inputFile, Map<String, Integer> columnMapping) throws Exception {
		Set<String> neededCols = new HashSet<>();
		List<? extends SelectItem> selectItems = plainSelect.getSelectItems();
		if (selectItems.size() == 1 && selectItems.get(0).toString().trim().equals("*")) {
			neededCols.addAll(columnMapping.keySet());
		} else {
			for (SelectItem item : selectItems) {
				String itemStr = item.toString().trim();
				try {
					Expression expr = CCJSqlParserUtil.parseExpression(itemStr);
					if (expr != null) {
						expr.accept(new ExpressionVisitorAdapter() {
							@Override
							public void visit(Column column) {
								String tblAlias = (column.getTable() != null) ? column.getTable().getName() : defaultTable;
								neededCols.add(tblAlias + "." + column.getColumnName());
							}

							@Override
							public void visit(Function function) {
								if (function.getParameters() != null && function.getParameters().getExpressions() != null) {
									for (Object obj : function.getParameters().getExpressions()) {
										((Expression) obj).accept(this);
									}
								}
							}
						});
					}
				} catch (Exception e) {
					System.err.println("Parsing error for SELECT item: " + itemStr);
				}
			}
		}
		if (plainSelect.getWhere() != null) {
			plainSelect.getWhere().accept(new ExpressionVisitorAdapter() {
				@Override
				public void visit(Column column) {
					String tblAlias = (column.getTable() != null) ? column.getTable().getName() : defaultTable;
					neededCols.add(tblAlias + "." + column.getColumnName());
				}

				@Override
				public void visit(Function function) {
					if (function.getParameters() != null && function.getParameters().getExpressions() != null) {
						for (Object obj : function.getParameters().getExpressions()) {
							((Expression) obj).accept(this);
						}
					}
				}
			});
		}
		if (plainSelect.getGroupBy() != null && plainSelect.getGroupBy().getGroupByExpressions() != null) {
			for (Object obj : plainSelect.getGroupBy().getGroupByExpressions()) {
				((Expression) obj).accept(new ExpressionVisitorAdapter() {
					@Override
					public void visit(Column column) {
						String tblAlias = (column.getTable() != null) ? column.getTable().getName() : defaultTable;
						neededCols.add(tblAlias + "." + column.getColumnName());
					}

					@Override
					public void visit(Function function) {
						if (function.getParameters() != null && function.getParameters().getExpressions() != null) {
							for (Object obj : function.getParameters().getExpressions()) {
								((Expression) obj).accept(this);
							}
						}
					}
				});
			}
		}
		List<OrderByElement> orderByElems = plainSelect.getOrderByElements();
		if (orderByElems != null) {
			for (OrderByElement obe : orderByElems) {
				obe.getExpression().accept(new ExpressionVisitorAdapter() {
					@Override
					public void visit(Column column) {
						String tblAlias = (column.getTable() != null) ? column.getTable().getName() : defaultTable;
						neededCols.add(tblAlias + "." + column.getColumnName());
					}

					@Override
					public void visit(Function function) {
						if (function.getParameters() != null && function.getParameters().getExpressions() != null) {
							for (Object obj : function.getParameters().getExpressions()) {
								((Expression) obj).accept(this);
							}
						}
					}
				});
			}
		}
		return neededCols;
	}


	/**
	 * Applies SortOperator to the operator tree if the query includes ORDER BY.
	 *
	 * @param plainSelect   Parsed SELECT query.
	 * @param opRoot        Current root of the operator tree.
	 * @param columnMapping Mapping from column names to their index positions.
	 * @return Root operator, wrapped with SortOperator if ORDER BY is present; otherwise unchanged.
	 */
	private static Operator applySortOperator(PlainSelect plainSelect, Operator opRoot, Map<String, Integer> columnMapping) {
		List<OrderByElement> orderByElems = plainSelect.getOrderByElements();
		if (orderByElems == null || orderByElems.isEmpty()) {
			return opRoot;
		}
		for (OrderByElement orderElem : orderByElems) {
			if (!(orderElem.getExpression() instanceof Column)) {
				String exprText = orderElem.getExpression().toString().trim();
				if (!columnMapping.containsKey(exprText)) {
					if (exprText.toUpperCase().startsWith("SUM(") && exprText.endsWith(")")) {
						String innerText = exprText.substring(4, exprText.length() - 1).trim();
						Optional<String> foundKeyOpt = columnMapping.keySet().stream()
								.filter(key -> key.toUpperCase().startsWith("SUM(") && key.endsWith(")"))
								.filter(key -> key.substring(4, key.length() - 1).trim().equalsIgnoreCase(innerText))
								.findFirst();
						if (foundKeyOpt.isPresent()) {
							exprText = foundKeyOpt.get();
						} else {
							throw new IllegalArgumentException("Invalid ORDER BY expression: " + orderElem.getExpression());
						}
					} else {
						throw new IllegalArgumentException("Invalid ORDER BY expression: " + orderElem.getExpression());
					}
				}
				orderElem.setExpression(new Column(exprText));
			}
		}
		return new SortOperator(opRoot, orderByElems, columnMapping);
	}


	/**
	 * Runs the final operator tree and checks the output against the expected results.
	 *
	 * @param opRoot     The final root of the operator tree.
	 * @param outputFile Path to the file where query results will be written.
	 */

	private static void executePlanAndValidateOutput(Operator opRoot, String outputFile) {
		// Execute the final operator tree and write output to file.
		execute(opRoot, outputFile);
		// Read and print the contents of the output file.
		try (BufferedReader reader = new BufferedReader(new FileReader(outputFile))) {
			String line;
			while ((line = reader.readLine()) != null) {
				System.out.println(line);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		// Extract query identifier and compare output.
		Pattern patQuery = Pattern.compile("query(\\d+)\\.csv");
		Pattern patTest = Pattern.compile("test(\\d+)\\.csv");
		Pattern patT = Pattern.compile("t(\\d+)\\.csv");
		Matcher mQuery = patQuery.matcher(outputFile);
		Matcher mTest = patTest.matcher(outputFile);
		Matcher mT = patT.matcher(outputFile);
		String queryNumber = "";
		String testNumber = "";
		String tNum = "";
		if (mQuery.find()) {
			queryNumber = mQuery.group(1);
		} else if (mTest.find()) {
			testNumber = mTest.group(1);
		} else if (mT.find()) {
			tNum = mT.group(1);
		} else {
			System.err.println("Could not extract query number from output file path: " + outputFile);
		}

		String expectedPath = "";
		if (!queryNumber.isEmpty()) {
			expectedPath = "samples/expected_output/query" + queryNumber + ".csv";
		} else if (!testNumber.isEmpty()) {
			expectedPath = "samples/expected_output/test" + testNumber + ".csv";
		} else if (!tNum.isEmpty()) {
			expectedPath = "samples/expected_output/t" + tNum + ".csv";
		}

		boolean comparison = FileComparator.compareFileContents(outputFile, expectedPath);
		if (comparison) {
			System.out.println("Results are as expected.");
		} else {
			System.out.println("Results do not match the expected output.");
		}
	}

	/**
	 * Parses the SQL query and builds the initial operator tree, including any join logic.
	 *
	 * @param fileReader Reader for the SQL input file.
	 * @param tableList  Output list to collect table names used in the query.
	 * @return OperatorInitializationResult with the root of the operator tree and the corresponding schema mapping.
	 * @throws Exception If query parsing or operator setup fails.
	 */
	private static InitResult initializeQueryPlan(FileReader fileReader, List<String> tableList, String databaseDir) throws Exception {
		Statement stmt = CCJSqlParserUtil.parse(fileReader);

		if (!(stmt instanceof Select)) {
			throw new IllegalArgumentException("Only SELECT statements are permitted.");
		}

		Select selectStmt = (Select) stmt;
		PlainSelect plainSelect = (PlainSelect) selectStmt.getSelectBody();
		List<Join> joinList = plainSelect.getJoins();

		Table baseTable = (Table) plainSelect.getFromItem();
		tableList.add(baseTable.getName());
		InitResult initOutcome = setupOperators(joinList, plainSelect, tableList, baseTable);

		if (initOutcome.getSchemaMap() == null) {
			throw new IllegalStateException("Failed to initialize column mapping.");
		}

		return initOutcome;
	}

	/**
	 * Derives a mapping from column names to their indices from the given operator.
	 *
	 * @param op The operator instance expected to provide schema details.
	 * @return A map where keys are column names and values are their respective indices.
	 * @throws RuntimeException if the operator does not implement SchemaProvider.
	 */
	private static Map<String, Integer> deriveOutputMapping(Operator op) {
		List<String> columns;
		if (op instanceof SchemaProvider) {
			columns = ((SchemaProvider) op).getOutputColumns();
		} else {
			throw new RuntimeException("Operator does not implement SchemaProvider interface.");
		}

		Map<String, Integer> mapping = new HashMap<>();
		for (int index = 0; index < columns.size(); index++) {
			mapping.put(columns.get(index), index);
		}
		return mapping;
	}

	/**
	 * Merges two schema mappings into a single mapping.
	 * The indices for the right mapping are placed after those of the left mapping.
	 *
	 * @param leftMapping  Mapping from the left operator.
	 * @param rightMapping Mapping from the right operator.
	 * @return A new mapping with combined entries, with right mapping indices offset.
	 */
	private static Map<String, Integer> combineSchemaMappings(Map<String, Integer> leftMapping,
															  Map<String, Integer> rightMapping) {
		Map<String, Integer> mergedMapping = new HashMap<>();
		int currentIndex = 0;
		for (String col : leftMapping.keySet()) {
			mergedMapping.put(col, currentIndex++);
		}
		for (String col : rightMapping.keySet()) {
			mergedMapping.put(col, currentIndex++);
		}
		return mergedMapping;
	}

	/**
	 * Applies the final projection on the operator tree based on the SELECT clause.
	 * <p>
	 * This method handles both aggregated and non-aggregated queries, adjusting the schema mapping
	 * according to the selected columns or expressions.
	 *
	 * @param plainSelect   Parsed SELECT statement.
	 * @param operatorRoot  Current operator tree root.
	 * @param schemaMapping Current column mapping.
	 * @param aggregation   Flag indicating if aggregation is involved.
	 * @param tableList     List of tables used in the query.
	 * @return A ProjectionResult containing the updated operator and new schema mapping.
	 */
	private static ProjectResult applyFinalProjection(PlainSelect plainSelect,
													  Operator operatorRoot,
													  Map<String, Integer> schemaMapping,
													  boolean aggregation,
													  List<String> tableList) {
		List<SelectItem<?>> selectItems = plainSelect.getSelectItems();
		boolean useDirectProjection = !(selectItems.size() == 1 && selectItems.get(0).toString().trim().equals("*"));

		if (useDirectProjection) {
			if (aggregation) {
				if (plainSelect.getGroupBy() != null &&
						plainSelect.getGroupBy().getGroupByExpressions() != null &&
						!plainSelect.getGroupBy().getGroupByExpressions().isEmpty()) {
					List<String> aggColumns = new ArrayList<>();
					for (SelectItem item : selectItems) {
						String colExpr = item.toString().trim().toUpperCase();
						aggColumns.add(colExpr.startsWith("SUM(") ? "SUM" : "GROUP");
					}
					operatorRoot = new ProjectOperator(operatorRoot, aggColumns.toArray(new String[0]), schemaMapping);
					Map<String, Integer> newMapping = new LinkedHashMap<>();
					for (int i = 0; i < aggColumns.size(); i++) {
						newMapping.put(aggColumns.get(i), i);
					}
					schemaMapping = newMapping;
				} else {
					List<String> aggColumns = new ArrayList<>(schemaMapping.keySet());
					operatorRoot = new ProjectOperator(operatorRoot, aggColumns.toArray(new String[0]), schemaMapping);
					Map<String, Integer> newMapping = new LinkedHashMap<>();
					for (int i = 0; i < aggColumns.size(); i++) {
						newMapping.put(aggColumns.get(i), i);
					}
					schemaMapping = newMapping;
				}
			} else {
				List<String> projColumns = new ArrayList<>();
				for (SelectItem item : selectItems) {
					String colStr = item.toString().trim();
					projColumns.add(colStr.contains(".") ? colStr : tableList.get(0) + "." + colStr);
				}
				operatorRoot = new ProjectOperator(operatorRoot, projColumns.toArray(new String[0]), schemaMapping);
				Map<String, Integer> newMapping = new HashMap<>();
				for (int i = 0; i < projColumns.size(); i++) {
					newMapping.put(projColumns.get(i), i);
				}
				schemaMapping = newMapping;
			}
		}
		return new ProjectResult(operatorRoot, schemaMapping);
	}

	/**
	 * Extracts aggregate (SUM) expressions from the SELECT clause.
	 * <p>
	 * This method scans the SELECT items for SUM expressions, replacing literal expressions with unique aliases.
	 *
	 * @param plainSelect The parsed SELECT statement.
	 * @return An AggregationResult containing the list of SUM expressions, a flag indicating if aggregation exists, and a mapping for literal expressions.
	 */
	private static AggregateResult extractAggregationFunctions(PlainSelect plainSelect) {
		List<Expression> sumExprList = new ArrayList<>();
		// Use AtomicInteger for mutable counter in lambda
		AtomicInteger aliasCounter = new AtomicInteger(0);
		// Determine if any aggregation exists using a stream
		boolean aggregationFound = plainSelect.getSelectItems().stream()
				.map(SelectItem::toString)
				.anyMatch(itemStr -> itemStr.trim().toUpperCase().startsWith("SUM("));
		Map<Expression, String> literalAliasMap = new HashMap<>();

		plainSelect.getSelectItems().forEach(item -> {
			String itemStr = item.toString();
			if (itemStr.trim().toUpperCase().startsWith("SUM(")) {
				int openParen = itemStr.indexOf("(");
				int closeParen = itemStr.lastIndexOf(")");
				if (openParen != -1 && closeParen != -1 && closeParen > openParen) {
					String innerText = itemStr.substring(openParen + 1, closeParen);
					try {
						Expression innerExpr = CCJSqlParserUtil.parseExpression(innerText);
						if (innerExpr instanceof LongValue || innerExpr instanceof DoubleValue) {
							String uniqueAlias = "LITERAL_SUM_" + aliasCounter.getAndIncrement();
							literalAliasMap.put(innerExpr, uniqueAlias);
							innerExpr = new Column(uniqueAlias);
						}
						sumExprList.add(innerExpr);
					} catch (Exception ex) {
						System.err.println("Error parsing SUM inner expression: " + innerText);
					}
				}
			}
		});
		return new AggregateResult(sumExprList, aggregationFound, literalAliasMap);
	}

	/**
	 * Initializes the operator tree and schema mapping based on the provided joins and FROM table.
	 *
	 * @param joins       List of JOIN conditions, if any.
	 * @param plainSelect The parsed SELECT statement.
	 * @param tableList   Output list to collect table names involved.
	 * @param baseTable   The base table from the FROM clause.
	 * @return An OperatorInitializationResult containing the root operator and schema mapping.
	 * @throws Exception if initialization fails.
	 */
	private static InitResult setupOperators(List<Join> joins, PlainSelect plainSelect, List<String> tableList, Table baseTable) throws Exception {
		Operator opRoot;
		Map<String, Integer> schemaMap;

		if (joins != null && !joins.isEmpty()) {
			for (Join join : joins) {
				Table joinTbl = (Table) join.getRightItem();
				tableList.add(joinTbl.getName());
			}
			opRoot = constructJoinTree(tableList, plainSelect.getWhere());
			if (tableList.size() == 1) {
				schemaMap = createSchemaMapping(tableList.get(0));
			} else {
				Map<String, Integer> tempMap = createSchemaMapping(tableList.get(0));
				for (int i = 1; i < tableList.size(); i++) {
					Map<String, Integer> nextMap = createSchemaMapping(tableList.get(i));
					tempMap = mergeColumnMappings(tempMap, nextMap);
				}
				schemaMap = tempMap;
			}
		} else {
			String tblName = baseTable.getName();
			/**System.out.println("Performing scan on table: " + tblName);
			 *
			 */
			opRoot = new ScanOperator(tblName, false);
			schemaMap = createSchemaMapping(tblName);
			if (plainSelect.getWhere() != null) {
				opRoot = new SelectOperator(opRoot, plainSelect.getWhere(), schemaMap);
			}
		}
		return new InitResult(opRoot, schemaMap);
	}

	/**
	 * Builds a left-deep join tree using the list of table names and the WHERE clause.
	 * Each table is joined in order, and any local selections or join conditions are applied as we go.
	 *
	 * @param tableList   List of table names to join, in the order they should be joined.
	 * @param whereClause The full WHERE clause, including both filters and join conditions.
	 * @return The root operator representing the full join tree with selections applied.
	 * @throws IllegalArgumentException if no tables are provided.
	 */
	public static Operator constructJoinTree(List<String> tableList, Expression whereClause) {
		if (tableList.isEmpty()) {
			throw new IllegalArgumentException("At least one table must be specified in the FROM clause.");
		}

		Map<String, Integer> currentMapping = createSchemaMapping(tableList.get(0));
		Operator currentOp = new ScanOperator(tableList.get(0), false);
		Expression leftFilter = pullLocalSelection(whereClause, tableList.get(0));
		if (leftFilter != null) {
			currentOp = new SelectOperator(currentOp, leftFilter, currentMapping);
		}

		for (int i = 1; i < tableList.size(); i++) {
			String tableName = tableList.get(i);
			Map<String, Integer> rightMapping = createSchemaMapping(tableName);
			Operator rightOp = new ScanOperator(tableName, false);

			Expression rightFilter = pullLocalSelection(whereClause, tableName);
			if (rightFilter != null) {
				rightOp = new SelectOperator(rightOp, rightFilter, rightMapping);
			}

			Expression joinCondition = pullJoinCondition(whereClause, currentMapping, rightMapping);
			Map<String, Integer> mergedMapping = mergeColumnMappings(currentMapping, rightMapping);
			currentOp = new JoinOperator(currentOp, rightOp, joinCondition, mergedMapping);
			currentMapping = mergedMapping;
		}
		return currentOp;
	}

	/**
	 * +     * Checks if all column references in the expression belong exclusively to the specified table.
	 * +     *
	 * +     * @param expr The SQL expression.
	 * +     * @param tableName The table name to check against.
	 * +     * @return true if all columns in expr are from tableName, false otherwise.
	 * +
	 */
	private static boolean isConditionLocal(Expression expr, final String tableName) {
		final boolean[] isLocal = {true};
		expr.accept(new ExpressionVisitorAdapter() {
			@Override
			public void visit(Column column) {
				if (column.getTable() != null && column.getTable().getName() != null) {
					String referencedTable = column.getTable().getName();
					if (!referencedTable.equalsIgnoreCase(tableName)) {
						isLocal[0] = false;
					}
				}
			}
		});
		return isLocal[0];
	}


	/**
	 * Extracts selection conditions from the WHERE clause that apply solely to the specified table.
	 *
	 * @param whereClause The full WHERE clause expression.
	 * @param tableName   The target table.
	 * @return An expression combining local selection conditions, or null if none.
	 */
	private static Expression pullLocalSelection(Expression whereClause, String tableName) {
		if (whereClause == null) return null;

		if (isConditionLocal(whereClause, tableName)) {
			return whereClause;
		}

		if (whereClause instanceof AndExpression) {
			AndExpression andExpr = (AndExpression) whereClause;
			Expression leftLocal = pullLocalSelection(andExpr.getLeftExpression(), tableName);
			Expression rightLocal = pullLocalSelection(andExpr.getRightExpression(), tableName);
			if (leftLocal != null && rightLocal != null) {
				return new AndExpression(leftLocal, rightLocal);
			} else if (leftLocal != null) {
				return leftLocal;
			} else {
				return rightLocal;
			}
		}

		return null;
	}

	/**
	 * Extracts a join condition from the WHERE clause that involves columns from both provided mappings.
	 *
	 * @param whereClause    The complete WHERE clause expression.
	 * @param currentMapping Mapping for the left operator.
	 * @param rightMapping   Mapping for the right operator.
	 * @return The join condition expression, or null if none is found.
	 */
	private static Expression pullJoinCondition(Expression whereClause,
												Map<String, Integer> currentMapping,
												Map<String, Integer> rightMapping) {
		if (whereClause == null) return null;

		if (whereClause instanceof AndExpression) {
			AndExpression andExpr = (AndExpression) whereClause;
			Expression leftCond = pullJoinCondition(andExpr.getLeftExpression(), currentMapping, rightMapping);
			Expression rightCond = pullJoinCondition(andExpr.getRightExpression(), currentMapping, rightMapping);

			if (leftCond != null && rightCond != null) {
				return new AndExpression(leftCond, rightCond);
			} else if (leftCond != null) {
				return leftCond;
			} else {
				return rightCond;
			}
		}

		if (whereClause instanceof BinaryExpression) {
			Set<String> refTables = getReferencedTables(whereClause);
			Set<String> leftTables = getTablesFromMapping(currentMapping);
			Set<String> rightTables = getTablesFromMapping(rightMapping);

			boolean leftUsed = false, rightUsed = false;
			for (String table : refTables) {
				if (leftTables.contains(table)) leftUsed = true;
				if (rightTables.contains(table)) rightUsed = true;
			}

			if (leftUsed && rightUsed) {
				return whereClause;
			}
		}

		return null;
	}

	/**
	 * Retrieves all table names referenced in the given expression.
	 *
	 * @param expr The SQL expression.
	 * @return A set of table names.
	 */
	private static Set<String> getReferencedTables(Expression expr) {
		Set<String> tableNames = new HashSet<>();
		expr.accept(new ExpressionVisitorAdapter() {
			@Override
			public void visit(Column column) {
				if (column.getTable() != null && column.getTable().getName() != null) {
					tableNames.add(column.getTable().getName());
				}
			}
		});
		return tableNames;
	}

	/**
	 * Extracts unique table names from a schema mapping, assuming keys are in the format "Table.Column".
	 *
	 * @param mapping The schema mapping.
	 * @return A set of table names.
	 */
	private static Set<String> getTablesFromMapping(Map<String, Integer> mapping) {
		Set<String> tables = new HashSet<>();
		for (String key : mapping.keySet()) {
			String[] parts = key.split("\\.");
			if (parts.length > 0) {
				tables.add(parts[0]);
			}
		}
		return tables;
	}


	/**
	 * Merges two column mappings, offsetting the indices from the right mapping.
	 *
	 * @param leftMapping  Mapping from the left operator.
	 * @param rightMapping Mapping from the right operator.
	 * @return A merged mapping with updated indices.
	 */
	private static Map<String, Integer> mergeColumnMappings(Map<String, Integer> leftMapping,
															Map<String, Integer> rightMapping) {
		int offset = leftMapping.size();
		Map<String, Integer> adjustedRight = rightMapping.entrySet().stream()
				.collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue() + offset));
		Map<String, Integer> merged = new HashMap<>(leftMapping);
		merged.putAll(adjustedRight);
		return merged;
	}

	/**
	 * Executes the provided query plan by repeatedly calling `getNextTuple()`
	 * on the root object of the operator tree. Writes the result to `outputFile`.
	 *
	 * @param root       The root operator of the operator tree (assumed to be non-null).
	 * @param outputFile The name of the file where the result will be written.
	 */
	public static void execute(Operator root, String outputFile) {
		try {
			// Create a BufferedWriter
			BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));

			// Iterate over the tuples produced by root
			Tuple tuple = root.getNextTuple();
			while (tuple != null) {
				writer.write(tuple.toString());
				writer.newLine();
				tuple = root.getNextTuple();
			}

			// Close the writer
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Constructs a schema mapping for the given table by reading its definition from the schema file.
	 *
	 * @param tableName The name of the table.
	 * @return A mapping of fully qualified column names to their index positions.
	 */
	private static Map<String, Integer> createSchemaMapping(String tableName) {
		Map<String, Integer> mapping = new HashMap<>();
		String schemaFilePath = "samples/db/schema.txt";

		try (BufferedReader reader = new BufferedReader(new FileReader(schemaFilePath))) {
			String line;
			while ((line = reader.readLine()) != null) {
				if (line.startsWith(tableName + " ")) {
					String[] tokens = line.trim().split("\\s+");
					for (int i = 1; i < tokens.length; i++) {
						mapping.put(tableName + "." + tokens[i], i - 1);
					}
					break;
				}
			}
		} catch (IOException ex) {
			System.err.println("Error reading schema file: " + ex.getMessage());
		}
		return mapping;
	}
}
