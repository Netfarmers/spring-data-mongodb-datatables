package org.springframework.data.mongodb.datatables;

import org.bson.Document;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.data.mongodb.repository.support.SimpleMongoRepository;

import java.io.Serializable;
import java.util.List;
import java.util.function.Function;

import static java.util.stream.Collectors.toList;
import static org.springframework.data.mongodb.core.query.Query.query;

final class DataTablesRepositoryImpl<T, ID extends Serializable> extends SimpleMongoRepository<T, ID>
        implements DataTablesRepository<T, ID> {

    private final MongoEntityInformation<T, ID> metadata;
    private final MongoOperations mongoOperations;

    /**
     * Creates a new {@link SimpleMongoRepository} for the given {@link MongoEntityInformation} and {@link MongoTemplate}.
     *
     * @param metadata        must not be {@literal null}.
     * @param mongoOperations must not be {@literal null}.
     */
    public DataTablesRepositoryImpl(MongoEntityInformation<T, ID> metadata, MongoOperations mongoOperations) {
        super(metadata, mongoOperations);
        this.metadata = metadata;
        this.mongoOperations = mongoOperations;
    }

    @Override
    public DataTablesOutput<T> findAll(DataTablesInput input) {
        return findAll(input, null, null, null);
    }

    @Override
    public DataTablesOutput<T> findAll(DataTablesInput input, Criteria additionalCriteria) {
        return findAll(input, additionalCriteria, null, null);
    }

    @Override
    public DataTablesOutput<T> findAll(DataTablesInput input, Criteria additionalCriteria, Criteria preFilteringCriteria) {
        return findAll(input, additionalCriteria, preFilteringCriteria, null);
    }

    @Override
    public <R> DataTablesOutput<R> findAll(DataTablesInput input, Function<T, R> converter) {
        return findAll(input, null, null, converter);
    }

    @Override
    public <R> DataTablesOutput<R> findAll(DataTablesInput input, Criteria additionalCriteria, Criteria preFilteringCriteria, Function<T, R> converter) {
        DataTablesOutput<R> output = new DataTablesOutput<>();
        output.setDraw(input.getDraw());
        if (input.getLength() == 0) {
            return output;
        }

        try {
            if (containsReferenceColumn(input, preFilteringCriteria) || containsReferenceColumn(input, additionalCriteria)) {
                throw new IllegalArgumentException("Additional criteria and prefilter criteria cannot use a reference column.");
            }

            long recordsTotal = count(preFilteringCriteria);
            output.setRecordsTotal(recordsTotal);
            if (recordsTotal == 0) {
                return output;
            }

            DataTablesCriteria refCriteria = new DataTablesCriteria(input, additionalCriteria, preFilteringCriteria, metadata.getJavaType());

            AggregationResults<Document> result = mongoOperations.aggregate(refCriteria.toFilteredCountAggregation(), metadata.getCollectionName(), Document.class);

            int recordsFiltered = 0;

            if (result.getUniqueMappedResult() != null) {
                recordsFiltered = (Integer) result.getUniqueMappedResult().get("filtered_count");
            }
            output.setRecordsFiltered(recordsFiltered);
            if (recordsFiltered == 0) {
                return output;
            }

            AggregationResults<T> data = mongoOperations.aggregate(refCriteria.toAggregation(), metadata.getCollectionName(), metadata.getJavaType());
            output.setData(converter == null ? (List<R>) data.getMappedResults() : data.getMappedResults().stream().map(converter).collect(toList()));

        } catch (Exception e) {
            output.setError(e.toString());
        }

        return output;
    }

    private long count(Criteria preFilteringCriteria) {
        if (preFilteringCriteria == null) {
            return count();
        } else {
            return mongoOperations.count(query(preFilteringCriteria), metadata.getCollectionName());
        }
    }

    private boolean containsReferenceColumn(DataTablesInput input, Criteria criteria) {
        if (criteria == null || input.getSearchConfiguration() == null) {
            return false;
        }

        return input.getSearchConfiguration().getColumnSearchConfiguration().entrySet()
                .stream()
                .anyMatch(c -> c.getValue().isReference() && criteria.getCriteriaObject().containsKey(c.getKey()));
    }
}
