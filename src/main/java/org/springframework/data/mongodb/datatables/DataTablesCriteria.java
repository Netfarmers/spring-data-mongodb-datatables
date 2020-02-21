package org.springframework.data.mongodb.datatables;

import org.springframework.beans.BeanUtils;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.aggregation.*;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.*;

import static java.util.stream.Collectors.toList;
import static org.springframework.data.domain.Sort.by;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.util.ObjectUtils.isEmpty;
import static org.springframework.util.StringUtils.hasText;

final class DataTablesCriteria<T> {
    private Map<String, String> resolvedColumn = new HashMap<>();
    private Aggregation aggregation;
    private Aggregation filteredCountAggregation;

    private Fields allClassFields;
    private String originalIdField;

    private Map<String, DataTablesInput.SearchConfiguration.ColumnSearchConfiguration> columnSearchConfiguration;
    private List<String> excludedColumns;

    DataTablesCriteria(DataTablesInput input, Criteria additionalCriteria, Criteria preFilteringCriteria, Class<T> classType) {
        if (input.getSearchConfiguration() != null) {
            columnSearchConfiguration = input.getSearchConfiguration().getColumnSearchConfiguration();
            excludedColumns = input.getSearchConfiguration().getExcludedColumns();
        } else {
            columnSearchConfiguration = new HashMap<>();
            excludedColumns = new ArrayList<>();
        }

        allClassFields = getFields(classType, excludedColumns);

        for (String excludedColumn : excludedColumns) {
            input.getColumn(excludedColumn).ifPresent(column -> input.getColumns().remove(column));
            input.getColumnMap().remove(excludedColumn);
            columnSearchConfiguration.remove(excludedColumn);
        }

        setDeclaredIdField(classType);

        if (!StringUtils.isEmpty(originalIdField)) {
            input.getColumn(originalIdField).ifPresent(c -> {

                DataTablesInput.SearchConfiguration.ColumnSearchConfiguration searchConfig = columnSearchConfiguration.get(c.getData());
                columnSearchConfiguration.remove(c.getData());

                c.setData("_id");
                input.setColumns(input.getColumns());

                columnSearchConfiguration.put(c.getData(), searchConfig);
            });
        }

        List<AggregationOperation> aggregationOperations = new ArrayList<>();

        if (additionalCriteria != null) aggregationOperations.add(Aggregation.match(additionalCriteria));
        if (preFilteringCriteria != null) aggregationOperations.add(Aggregation.match(preFilteringCriteria));

        // If there is not projection because of references but there are excluded columns,
        // an extra projection has to be added to exclude these columns
        if (!columnSearchConfiguration.isEmpty()) {
            List<AggregationOperation> referenceResolverOps = addReferenceResolver(input);
            aggregationOperations.addAll(referenceResolverOps);

            if (referenceResolverOps.isEmpty() && !excludedColumns.isEmpty()) {
                aggregationOperations.add(createFieldProjection(input));
            }
        } else if (!excludedColumns.isEmpty()) {
            aggregationOperations.add(createFieldProjection(input));
        }

        AggregationOperation globalMatching = addGlobalCriteria(input);

        if (globalMatching != null) {
            aggregationOperations.add(globalMatching);
        }

        input.getColumns().forEach(column -> {
            MatchOperation columnCriteriaMatcher = addColumnCriteria(column);
            if (columnCriteriaMatcher != null) {
                aggregationOperations.add(columnCriteriaMatcher);
            }
        });

        List<AggregationOperation> filteredCountOperations = new ArrayList<>(aggregationOperations);
        filteredCountOperations.add(Aggregation.count().as("filtered_count"));

        filteredCountAggregation = Aggregation.newAggregation(filteredCountOperations);

        aggregationOperations.addAll(addSort(input));
        aggregation = Aggregation.newAggregation(aggregationOperations);

        if (!StringUtils.isEmpty(originalIdField)) {
            input.getColumn("_id").ifPresent(c -> {
                c.setData(originalIdField);
                input.setColumns(input.getColumns());
            });
        }
    }

    private List<AggregationOperation> addReferenceResolver(DataTablesInput input) {

        List<AggregationOperation> aggregations = new ArrayList<>();

        List<String> columnStrings = input.getColumns().stream()
                .map(column -> column.getData().contains(".") ? column.getData().substring(0, column.getData().indexOf(".")) : column.getData())
                .distinct()
                .collect(toList());

        for (DataTablesInput.Column c: input.getColumns()) {

            DataTablesInput.SearchConfiguration.ColumnSearchConfiguration searchConfig = columnSearchConfiguration.get(c.getData());

            if (searchConfig != null && searchConfig.isReference() && (c.isSearchable() || c.isOrderable())) {

                String resolvedReferenceColumn = getResolvedRefColumn(c, columnStrings);

                resolvedColumn.put(c.getData(), resolvedReferenceColumn);

                String[] columnStringsArr = columnStrings.toArray(new String[0]);

                // Convert reference field array of key-value objects
                ProjectionOperation projectDbRefArr = Aggregation
                        .project(allClassFields)
                        .andInclude(columnStringsArr)
                        .and(ObjectOperators.ObjectToArray.valueOfToArray(c.getData()))
                        .as(resolvedReferenceColumn + "_fk_arr");

                // Extract object with Id from array
                ProjectionOperation projectDbRefObject = Aggregation
                        .project(allClassFields)
                        .andInclude(columnStringsArr)
                        .and( resolvedReferenceColumn + "_fk_arr").arrayElementAt(1)
                        .as(resolvedReferenceColumn + "_fk_obj");

                // Get value field from key-value object
                ProjectionOperation projectPidField = Aggregation
                        .project(allClassFields)
                        .andInclude(columnStringsArr)
                        .and(resolvedReferenceColumn + "_fk_obj.v").as(resolvedReferenceColumn + "_id");

                // Lookup object with id in reference collection and save it in document
                LookupOperation lookupOperation = Aggregation
                        .lookup(searchConfig.getReferenceCollection(), resolvedReferenceColumn + "_id", "_id", resolvedReferenceColumn);

                // Make sure resolved object stays in future projections
                columnStrings.add(resolvedReferenceColumn);

                aggregations.add(projectDbRefArr);
                aggregations.add(projectDbRefObject);
                aggregations.add(projectPidField);
                aggregations.add(lookupOperation);
            }
        }

        return aggregations;
    }

    private AggregationOperation addGlobalCriteria(DataTablesInput input) {
        if (!hasText(input.getSearch().getValue())) return null;

        Criteria[] criteriaArray = input.getColumns().stream()
                .filter(DataTablesInput.Column::isSearchable)
                .map(column -> createCriteria(column, input.getSearch()))
                .flatMap(Collection::stream)
                .toArray(Criteria[]::new);

        if (criteriaArray.length == 1) {
            return Aggregation.match(criteriaArray[0]);
        } else if (criteriaArray.length >= 2) {
            return Aggregation.match(new Criteria().orOperator(criteriaArray));
        } else {
            return null;
        }
    }

    private MatchOperation addColumnCriteria(DataTablesInput.Column column) {
        if (column.isSearchable() && hasText(column.getSearch().getValue())) {
            List<Criteria> criteria = createCriteria(column, column.getSearch());
            if (criteria.size() == 1) {
                return Aggregation.match(criteria.get(0));
            } else if (criteria.size() >= 2) {
                return Aggregation.match(new Criteria().orOperator(criteria.toArray(new Criteria[0])));
            }
        }

        return null;
    }

    private List<Criteria> createCriteria(DataTablesInput.Column column, DataTablesInput.Search search) {

        String searchValue = search.getValue();
        DataTablesInput.SearchConfiguration.ColumnSearchConfiguration searchConfig = columnSearchConfiguration.get(column.getData());
        if (searchConfig == null) {
            searchConfig = DataTablesInput.SearchConfiguration.ColumnSearchConfiguration.DEFAULT;
        }

        if (searchConfig.isReference()) {
            // In case of reference, no searchType is available -> autoconvert true/false, else do string comparison
            if ("true".equalsIgnoreCase(searchValue) || "false".equalsIgnoreCase(searchValue)) {
                boolean boolSearchValue = Boolean.parseBoolean(searchValue);

                return searchConfig.getReferenceColumns().stream()
                        .map(data -> where(resolvedColumn.get(column.getData()) + "." + data).is(boolSearchValue))
                        .collect(toList());
            } else {
                return searchConfig.getReferenceColumns().stream()
                        .map(data -> search.isRegex() ?
                                where(resolvedColumn.get(column.getData()) + "." + data).regex(searchValue) : where(resolvedColumn.get(column.getData()) + "." + data).regex(searchValue.trim(), "i"))
                        .collect(toList());
            }
        } else {
            List<Criteria> criteria = new ArrayList<>();

            switch (searchConfig.getSearchType()) {
                case Boolean:
                    if ("true".equalsIgnoreCase(searchValue) || "false".equalsIgnoreCase(searchValue)) {
                        criteria.add(where(column.getData()).is(Boolean.parseBoolean(searchValue)));
                    }
                    break;
                case Integer:
                    try {
                        int intSearchValue = Integer.parseInt(searchValue.trim());
                        criteria.add(where(column.getData()).is(intSearchValue));
                    } catch (NumberFormatException e) {
                        return criteria;
                    }
                    break;
                default:
                    if (search.isRegex()) {
                        criteria.add(where(column.getData()).regex(searchValue));
                    } else {
                        criteria.add(where(column.getData()).regex(searchValue.trim(), "i"));
                    }
                    break;
            }

            return criteria;
        }
    }

    private List<AggregationOperation> addSort(DataTablesInput input) {
        List<AggregationOperation> operations = new ArrayList<>();

        if (!isEmpty(input.getOrder())) {
            List<Sort.Order> orders = input.getOrder().stream()
                    .filter(order -> isOrderable(input, order))
                    .map(order -> toOrder(input, order)).collect(toList());

            if (orders.size() != 0) {
                operations.add(Aggregation.sort(by(orders)));
            }

        }

        operations.add(Aggregation.skip(input.getStart()));

        if (input.getLength() >= 0) {
            operations.add(Aggregation.limit(input.getLength()));
        }

        return operations;
    }

    private boolean isOrderable(DataTablesInput input, DataTablesInput.Order order) {
        boolean isWithinBounds = order.getColumn() < input.getColumns().size();

        DataTablesInput.Column column = input.getColumns().get(order.getColumn());

        if (columnSearchConfiguration.containsKey(column.getData())) {
            DataTablesInput.SearchConfiguration.ColumnSearchConfiguration searchConfig = columnSearchConfiguration.get(column.getData());
            return isWithinBounds && column.isOrderable()
                    && (!searchConfig.isReference() || !StringUtils.isEmpty(searchConfig.getReferenceOrderColumn()));
        } else {
            return isWithinBounds && column.isOrderable();
        }
    }

    private Sort.Order toOrder(DataTablesInput input, DataTablesInput.Order order) {
        DataTablesInput.Column column = input.getColumns().get(order.getColumn());
        Sort.Direction sortDir = order.getDir() == DataTablesInput.Order.Direction.asc ? Sort.Direction.ASC : Sort.Direction.DESC;

        if (columnSearchConfiguration.containsKey(column.getData())) {
            DataTablesInput.SearchConfiguration.ColumnSearchConfiguration searchConfig = columnSearchConfiguration.get(column.getData());
            if (searchConfig.isReference()) {
                return new Sort.Order(sortDir, resolvedColumn.get(column.getData()) + "." + searchConfig.getReferenceOrderColumn());
            }
        }

        return new Sort.Order(sortDir, column.getData());
    }

    private String getResolvedRefColumn(DataTablesInput.Column c, List<String> columnStrings) {

        String resolvedColumn = c.getData();
        boolean columnAlreadyExists;

        do {
            resolvedColumn += "_";
            String columnName = resolvedColumn;
            columnAlreadyExists = columnStrings.stream().anyMatch(s -> s.startsWith(columnName));
        } while (columnAlreadyExists);

        return resolvedColumn;
    }

    public Aggregation toAggregation() {
        return aggregation;
    }

    public Aggregation toFilteredCountAggregation() {
        return filteredCountAggregation;
    }

    private AggregationOperation createFieldProjection(DataTablesInput input) {
        List<String> columnStrings = input.getColumns().stream()
                .map(column -> column.getData().contains(".") ? column.getData().substring(0, column.getData().indexOf(".")) : column.getData())
                .distinct()
                .collect(toList());

        return Aggregation.project(allClassFields).andInclude(columnStrings.toArray(new String[0]));
    }

    /**
     * Use official getFields method to list all fields of the class.
     * Source: https://github.com/spring-projects/spring-data-mongodb/blob/1a5de2e1db939f7b35579f11815894fd637fc227/spring-data-mongodb/src/main/java/org/springframework/data/mongodb/core/aggregation/AggregationOperationContext.java#L88
     * @return Class fields
     */
    private Fields getFields(Class<T> type, List<String> excludedColumns) {

        return Fields.fields(Arrays.stream(BeanUtils.getPropertyDescriptors(type))
                .filter(it -> {
                    Method method = it.getReadMethod();
                    if (method == null) {
                        return false;
                    }
                    if (ReflectionUtils.isObjectMethod(method)) {
                        return false;
                    }

                    if (excludedColumns.contains(it.getName())) {
                        return false;
                    }

                    return !method.isDefault();
                })
                .map(PropertyDescriptor::getName)
                .toArray(String[]::new));
    }

    private void setDeclaredIdField(Class<T> classType) {
        Optional<java.lang.reflect.Field> idField = Arrays.stream(classType.getDeclaredFields())
                .filter(f -> f.isAnnotationPresent(Id.class)).findFirst();
        idField.ifPresent(f -> originalIdField = idField.get().getName());
    }
}